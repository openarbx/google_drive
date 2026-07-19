#!/usr/bin/env python3
"""
OmniLib Google Drive Recursive Downloader

Pipeline (map-first, no per-file re-verification of completed work):
  1. Build a full file map of the Drive tree (folders + files + sizes).
  2. Load completed/in_progress state; drop fully-done entries from the map.
  3. Download only what remains (resumable .part files, size-verified).

Designed for large libraries (tens of thousands of files):
  - No Drive metadata GET for every already-completed file.
  - State JSON is rewritten infrequently (batched), not after every chunk.
  - File map is streamed to disk so RAM stays low during discovery.
  - List responses already carry size — no extra round-trips.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import warnings
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Iterator, TypedDict

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PendingDeprecationWarning)

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------

FOLDER_MIME: Final[str] = "application/vnd.google-apps.folder"
GOOGLE_NATIVE_PREFIX: Final[str] = "application/vnd.google-apps."


class CompletedEntry(TypedDict, total=False):
    completed_at: float
    size: int
    file_id: str


class InProgressEntry(TypedDict, total=False):
    file_id: str
    file_name: str
    total_size: int
    started_at: float
    resumed_from: int


class DownloadState(TypedDict):
    completed: dict[str, CompletedEntry]
    in_progress: dict[str, InProgressEntry]


class MappedFile(TypedDict):
    """One non-folder file discovered during the map phase."""
    file_id: str
    name: str
    rel_path: str
    size: int
    mime_type: str


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    client_secret_file: str
    token_file: str
    omnilib_folder_id: str
    destination_path: str
    speed_limit_mbps: float
    chunk_size: int
    state_file: str
    map_file: str
    scopes: tuple[str, ...]
    config_path: str
    state_save_every: int
    map_page_size: int
    skip_google_native: bool
    rebuild_map: bool

    @classmethod
    def from_file(cls, config_path: str = "config.json") -> Config:
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw: dict[str, Any] = json.load(f)

        destination_path: str = raw["destination_path"]
        speed_limit_mbps: float = float(raw.get("speed_limit_mbps", 100.0))
        chunk_size: int = int(raw.get("chunk_size_mb", 8)) * 1024 * 1024
        state_file: str = raw.get(
            "state_file",
            os.path.join(destination_path, ".download_state.json"),
        )
        map_file: str = raw.get(
            "map_file",
            os.path.join(destination_path, ".file_map.jsonl"),
        )
        scopes: tuple[str, ...] = ("https://www.googleapis.com/auth/drive.readonly",)

        cfg = cls(
            client_secret_file=raw["client_secret_file"],
            token_file=raw["token_file"],
            omnilib_folder_id=raw["omnilib_folder_id"],
            destination_path=destination_path,
            speed_limit_mbps=speed_limit_mbps,
            chunk_size=chunk_size,
            state_file=state_file,
            map_file=map_file,
            scopes=scopes,
            config_path=config_path,
            state_save_every=max(1, int(raw.get("state_save_every", 25))),
            map_page_size=min(1000, max(100, int(raw.get("map_page_size", 1000)))),
            skip_google_native=bool(raw.get("skip_google_native", True)),
            rebuild_map=bool(raw.get("rebuild_map", True)),
        )
        if cfg.speed_limit_mbps <= 0:
            raise ValueError("speed_limit_mbps must be positive")
        if cfg.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        return cfg


# -----------------------------------------------------------------------------
# Downloader
# -----------------------------------------------------------------------------

class DriveDownloader:
    _PROGRESS_LOG_INTERVAL: Final[float] = 10.0
    # Sleep only when speed_limit is intentionally low; high limits = no throttle.
    _THROTTLE_BELOW_MBPS: Final[float] = 50.0

    def __init__(self, config_path: str = "config.json", log_base_dir: str = "logs") -> None:
        self.config: Config = Config.from_file(config_path)
        self.logger: logging.Logger = self._setup_logging(log_base_dir)

        self.state: DownloadState = {"completed": {}, "in_progress": {}}
        self._completed_paths: set[str] = set()
        self._state_dirty: int = 0

        self.service: Any = None
        self.creds: Credentials | None = None

        self._dest_path: Path = Path(self.config.destination_path)
        self._state_path: Path = Path(self.config.state_file)
        self._map_path: Path = Path(self.config.map_file)

        self.logger.info(
            "DriveDownloader ready | dest=%s | map=%s | state=%s",
            self._dest_path,
            self._map_path,
            self._state_path,
        )

    # -- logging --------------------------------------------------------------

    def _setup_logging(self, log_base_dir: str) -> logging.Logger:
        logger = logging.getLogger("DriveDownloader")
        logger.setLevel(logging.DEBUG)
        if logger.hasHandlers():
            logger.handlers.clear()

        today_str = datetime.now().strftime("%Y-%m-%d")
        log_dir = Path(log_base_dir) / today_str
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"drive_downloader_{datetime.now().strftime('%H%M%S')}.log"

        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(fh)

        # Also log INFO+ to stderr so run.sh / nohup captures live progress.
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(logging.INFO)
        sh.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(sh)

        logger.info("Logging initialized. File: %s", log_file)
        return logger

    # -- state ----------------------------------------------------------------

    def _load_state(self) -> DownloadState:
        if not self._state_path.is_file():
            self.logger.debug("No state file; starting fresh.")
            return {"completed": {}, "in_progress": {}}
        try:
            with self._state_path.open("r", encoding="utf-8") as f:
                raw: dict[str, Any] = json.load(f)
            completed: dict[str, CompletedEntry] = raw.get("completed", {}) or {}
            in_progress: dict[str, InProgressEntry] = raw.get("in_progress", {}) or {}
            self.logger.info(
                "State loaded: completed=%d in_progress=%d",
                len(completed),
                len(in_progress),
            )
            return {"completed": completed, "in_progress": in_progress}
        except Exception as exc:
            self.logger.warning("State unreadable (%s); starting fresh.", exc)
            return {"completed": {}, "in_progress": {}}

    def _save_state(self, force: bool = False) -> None:
        if not force and self._state_dirty < self.config.state_save_every:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            # Compact JSON (no indent/sort) — critical with 30k+ entries.
            tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self.state, f, separators=(",", ":"))
            tmp.replace(self._state_path)
            self._state_dirty = 0
            self.logger.debug(
                "State saved (completed=%d in_progress=%d)",
                len(self.state["completed"]),
                len(self.state["in_progress"]),
            )
        except Exception as exc:
            self.logger.error("Failed to persist state: %s", exc)

    def _mark_dirty(self) -> None:
        self._state_dirty += 1
        self._save_state(force=False)

    def _mark_completed(self, rel_path: str, size: int = 0, file_id: str = "") -> None:
        entry: CompletedEntry = {"completed_at": time.time()}
        if size > 0:
            entry["size"] = size
        if file_id:
            entry["file_id"] = file_id
        self.state["completed"][rel_path] = entry
        self.state["in_progress"].pop(rel_path, None)
        self._completed_paths.add(rel_path)
        self._mark_dirty()

    # -- auth -----------------------------------------------------------------

    def get_credentials(self) -> Credentials:
        creds: Credentials | None = None
        token_path = Path(self.config.token_file)

        if token_path.is_file():
            try:
                creds = Credentials.from_authorized_user_file(
                    str(token_path), list(self.config.scopes)
                )
                self.logger.debug("Loaded cached credentials.")
            except Exception as exc:
                self.logger.warning("Token file invalid: %s", exc)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    self.logger.info("Credentials refreshed.")
                except Exception as exc:
                    self.logger.error("Refresh failed: %s", exc)
                    creds = None

            if not creds or not creds.valid:
                self.logger.info("Starting OAuth consent flow (port 8080).")
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config.client_secret_file, list(self.config.scopes)
                )
                flow_creds = flow.run_local_server(port=8080, open_browser=False)
                if not isinstance(flow_creds, Credentials):
                    raise RuntimeError(
                        f"OAuth returned unsupported type: {type(flow_creds).__name__}"
                    )
                creds = flow_creds
                self.logger.info("OAuth completed.")

            if creds is None:
                raise RuntimeError("OAuth did not yield credentials.")

            try:
                token_path.write_text(creds.to_json(), encoding="utf-8")
                self.logger.info("Credentials saved to %s", token_path)
            except Exception as exc:
                self.logger.error("Failed to write token: %s", exc)

        if creds is None:
            raise RuntimeError("Failed to obtain valid credentials.")
        return creds

    def build_service(self) -> None:
        self.creds = self.get_credentials()
        self.service = build("drive", "v3", credentials=self.creds, cache_discovery=False)
        self.logger.info("Google Drive API v3 service ready.")

    # -- listing / map --------------------------------------------------------

    def _list_children(self, folder_id: str) -> Iterator[dict[str, Any]]:
        """Yield all children of a folder (paginated, shared-drive aware)."""
        page_token: str | None = None
        while True:
            try:
                results: dict[str, Any] = (
                    self.service.files()
                    .list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        fields="nextPageToken, files(id, name, mimeType, size)",
                        pageSize=self.config.map_page_size,
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        corpora="allDrives",
                    )
                    .execute()
                )
            except HttpError as exc:
                # Fallback without corpora if the account rejects allDrives.
                self.logger.warning(
                    "list(allDrives) failed for %s (%s); retrying without corpora",
                    folder_id,
                    exc,
                )
                results = (
                    self.service.files()
                    .list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        fields="nextPageToken, files(id, name, mimeType, size)",
                        pageSize=self.config.map_page_size,
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute()
                )

            for item in results.get("files", []):
                yield item
            page_token = results.get("nextPageToken")
            if not page_token:
                break

    def build_file_map(self) -> int:
        """
        Walk the entire Drive tree and write a JSONL map to disk.
        Returns the number of downloadable files written.
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 1: Building file map → %s", self._map_path)
        self.logger.info("=" * 60)

        self._map_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._map_path.with_suffix(self._map_path.suffix + ".tmp")

        file_count = 0
        folder_count = 0
        skipped_native = 0
        t0 = time.time()

        # BFS with deque keeps recursion depth O(1) and peak RAM low.
        queue: deque[tuple[str, str]] = deque(
            [(self.config.omnilib_folder_id, "")]
        )

        with tmp.open("w", encoding="utf-8") as out:
            while queue:
                folder_id, rel_prefix = queue.popleft()
                folder_count += 1
                if folder_count == 1 or folder_count % 50 == 0:
                    self.logger.info(
                        "Map progress: folders=%d files=%d queue=%d prefix=%s",
                        folder_count,
                        file_count,
                        len(queue),
                        rel_prefix or "/",
                    )

                try:
                    for item in self._list_children(folder_id):
                        name = item.get("name") or "unnamed"
                        mime = item.get("mimeType") or ""
                        item_id = item.get("id") or ""
                        if not item_id:
                            continue

                        rel = f"{rel_prefix}/{name}" if rel_prefix else name
                        # Normalize to POSIX relative path
                        rel = rel.replace("\\", "/").lstrip("/")

                        if mime == FOLDER_MIME:
                            try:
                                (self._dest_path / rel).mkdir(parents=True, exist_ok=True)
                            except OSError as exc:
                                self.logger.error("mkdir failed %s: %s", rel, exc)
                            queue.append((item_id, rel))
                            continue

                        if (
                            self.config.skip_google_native
                            and mime.startswith(GOOGLE_NATIVE_PREFIX)
                        ):
                            skipped_native += 1
                            self.logger.debug("Skip Google-native: %s (%s)", rel, mime)
                            continue

                        size = int(item.get("size") or 0)
                        rec: MappedFile = {
                            "file_id": item_id,
                            "name": name,
                            "rel_path": rel,
                            "size": size,
                            "mime_type": mime,
                        }
                        out.write(json.dumps(rec, separators=(",", ":")) + "\n")
                        file_count += 1
                except Exception as exc:
                    self.logger.error("Failed listing folder %s (%s): %s", rel_prefix, folder_id, exc)

        tmp.replace(self._map_path)
        elapsed = time.time() - t0
        self.logger.info(
            "Map complete: files=%d folders=%d native_skipped=%d in %.1fs → %s",
            file_count,
            folder_count,
            skipped_native,
            elapsed,
            self._map_path,
        )
        return file_count

    def _iter_map(self) -> Iterator[MappedFile]:
        if not self._map_path.is_file():
            return
        with self._map_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)  # type: ignore[misc]
                except json.JSONDecodeError:
                    continue

    def collect_pending(self) -> list[MappedFile]:
        """
        PHASE 2: Stream the map, drop anything already completed (or verified
        on disk), return only files that still need downloading.
        """
        self.logger.info("=" * 60)
        self.logger.info("PHASE 2: Filtering map against completed state")
        self.logger.info("=" * 60)

        pending: list[MappedFile] = []
        total = 0
        already_done = 0
        promoted = 0  # disk-verified → marked completed without download

        for rec in self._iter_map():
            total += 1
            rel = rec["rel_path"]
            final = self._dest_path / rel
            size = int(rec.get("size") or 0)

            # Fast path: already in completed ledger
            if rel in self._completed_paths:
                # Optional cheap local size check if we know drive size
                if final.is_file() and size > 0:
                    try:
                        if final.stat().st_size != size:
                            self.logger.warning(
                                "Completed but size mismatch — requeue: %s (local=%d drive=%d)",
                                rel,
                                final.stat().st_size,
                                size,
                            )
                            self.state["completed"].pop(rel, None)
                            self._completed_paths.discard(rel)
                            pending.append(rec)
                            self._mark_dirty()
                            continue
                    except OSError:
                        pass
                already_done += 1
                continue

            # Disk already has a full file matching mapped size → promote to completed
            if final.is_file():
                try:
                    local = final.stat().st_size
                    if size == 0 or local == size:
                        self._mark_completed(rel, size=local, file_id=rec["file_id"])
                        promoted += 1
                        continue
                    # Wrong size on disk → will re-download
                    self.logger.warning(
                        "On-disk size mismatch (will re-download): %s local=%d drive=%d",
                        rel,
                        local,
                        size,
                    )
                except OSError as exc:
                    self.logger.error("stat failed %s: %s", rel, exc)

            pending.append(rec)

        self._save_state(force=True)
        self.logger.info(
            "Filter done: mapped=%d already_done=%d promoted=%d PENDING=%d",
            total,
            already_done,
            promoted,
            len(pending),
        )
        return pending

    # -- download -------------------------------------------------------------

    def _part_path(self, final: Path) -> Path:
        return Path(str(final) + ".part")

    def _throttle(self) -> None:
        # Only sleep when an intentional low cap is set.
        if self.config.speed_limit_mbps >= self._THROTTLE_BELOW_MBPS:
            return
        # Rough sleep proportional to chunk throughput target.
        delay = self.config.chunk_size / (self.config.speed_limit_mbps * 1024 * 1024)
        # Cap sleep so tiny limits don't freeze; minimum 0.
        time.sleep(min(delay, 2.0))

    def download_file(self, rec: MappedFile, *, allow_range_retry: bool = True) -> bool:
        """
        Download one mapped file. Returns True on success / already present.
        Uses size from the map (no extra metadata GET).
        """
        file_id = rec["file_id"]
        name = rec["name"]
        rel = rec["rel_path"]
        total_size = int(rec.get("size") or 0)

        final = self._dest_path / rel
        part = self._part_path(final)

        self.logger.info("BEGIN | %s | id=%s | size=%d", rel, file_id, total_size)

        # 1. Existing final file
        if final.is_file():
            try:
                local = final.stat().st_size
                if total_size == 0 or local == total_size:
                    self._mark_completed(rel, size=local, file_id=file_id)
                    self.logger.info("SKIP (present): %s", rel)
                    return True
                self.logger.warning(
                    "SIZE MISMATCH | %s | drive=%d local=%d → delete + redownload",
                    rel,
                    total_size,
                    local,
                )
                final.unlink()
                self.state["completed"].pop(rel, None)
                self._completed_paths.discard(rel)
            except OSError as exc:
                self.logger.error("Cannot handle existing %s: %s", final, exc)
                return False

        # 2. Complete .part → rename
        if part.is_file():
            try:
                psz = part.stat().st_size
                if total_size > 0 and psz == total_size:
                    part.replace(final)
                    self._mark_completed(rel, size=psz, file_id=file_id)
                    self.logger.info("RESUME COMPLETE from .part: %s", rel)
                    return True
                # Corrupt / oversize partial — restart clean
                if total_size > 0 and psz > total_size:
                    self.logger.warning(
                        "Part larger than Drive size (%d > %d); deleting %s",
                        psz,
                        total_size,
                        part,
                    )
                    part.unlink()
            except OSError as exc:
                self.logger.error("Part handling failed for %s: %s", rel, exc)
                return False

        current_size = part.stat().st_size if part.is_file() else 0

        self.state["in_progress"][rel] = {
            "file_id": file_id,
            "file_name": name,
            "total_size": total_size,
            "started_at": time.time(),
            "resumed_from": current_size,
        }
        self._save_state(force=True)  # intent before bytes on disk

        try:
            final.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self.logger.error("mkdir parent failed %s: %s", final.parent, exc)
            return False

        # 3. Stream download (resume via Range when partial exists)
        mode = "ab" if current_size > 0 else "wb"
        try:
            with part.open(mode) as fh:
                request = self.service.files().get_media(
                    fileId=file_id,
                    supportsAllDrives=True,
                )
                if current_size > 0:
                    request.headers["Range"] = f"bytes={current_size}-"
                    self.logger.info("RESUMING %s from byte %d", rel, current_size)

                downloader = MediaIoBaseDownload(
                    fh, request, chunksize=self.config.chunk_size
                )
                done = False
                last_logged_pct = 0.0

                while not done:
                    try:
                        status, done = downloader.next_chunk()
                    except HttpError as chunk_exc:
                        status_code = (
                            chunk_exc.resp.status
                            if chunk_exc.resp is not None
                            else None
                        )
                        if (
                            allow_range_retry
                            and current_size > 0
                            and status_code in (400, 416)
                        ):
                            self.logger.warning(
                                "Range rejected for %s; restarting from 0", rel
                            )
                            try:
                                fh.close()
                            except Exception:
                                pass
                            try:
                                if part.is_file():
                                    part.unlink()
                            except OSError:
                                pass
                            return self.download_file(rec, allow_range_retry=False)
                        self.logger.error("Chunk error %s: %s", rel, chunk_exc)
                        return False
                    except Exception as chunk_exc:
                        self.logger.error("Chunk error %s: %s", rel, chunk_exc)
                        return False

                    if status and total_size > 0:
                        try:
                            cur = part.stat().st_size
                        except OSError:
                            cur = current_size
                        pct = min(100.0, (cur / total_size) * 100.0)
                        if pct - last_logged_pct >= self._PROGRESS_LOG_INTERVAL:
                            self.logger.info(
                                "PROGRESS | %s | %.1f%% (%d/%d)",
                                name,
                                pct,
                                cur,
                                total_size,
                            )
                            last_logged_pct = pct

                    self._throttle()

            # 4. Finalize
            if not part.is_file():
                self.logger.error("Part missing after download: %s", rel)
                return False

            final_size = part.stat().st_size
            if total_size == 0 or final_size == total_size:
                part.replace(final)
                self._mark_completed(rel, size=final_size, file_id=file_id)
                self.logger.info(
                    "COMPLETE | %s | %d bytes", rel, final_size
                )
                return True

            self.logger.warning(
                "POST-WRITE MISMATCH | %s | expected=%d got=%d | keeping .part",
                rel,
                total_size,
                final_size,
            )
            return False

        except Exception as exc:
            self.logger.error("Download failed %s: %s", rel, exc)
            return False

    def download_pending(self, pending: list[MappedFile]) -> None:
        self.logger.info("=" * 60)
        self.logger.info("PHASE 3: Downloading %d pending files", len(pending))
        self.logger.info("=" * 60)

        ok = 0
        fail = 0
        t0 = time.time()

        for i, rec in enumerate(pending, 1):
            if i == 1 or i % 25 == 0 or i == len(pending):
                elapsed = max(0.001, time.time() - t0)
                rate = ok / elapsed
                self.logger.info(
                    "Queue %d/%d | ok=%d fail=%d | %.2f files/s",
                    i,
                    len(pending),
                    ok,
                    fail,
                    rate,
                )
            try:
                if self.download_file(rec):
                    ok += 1
                else:
                    fail += 1
            except Exception as exc:
                fail += 1
                self.logger.error("Unhandled error on %s: %s", rec.get("rel_path"), exc)

        self._save_state(force=True)
        self.logger.info(
            "Download phase finished: ok=%d fail=%d elapsed=%.1fs",
            ok,
            fail,
            time.time() - t0,
        )

    def cleanup_stale_in_progress(self) -> int:
        cleaned = 0
        for rel in list(self.state.get("in_progress", {}).keys()):
            if rel in self._completed_paths:
                self.state["in_progress"].pop(rel, None)
                cleaned += 1
                continue
            part = self._part_path(self._dest_path / rel)
            entry = self.state["in_progress"].get(rel, {})
            total = int(entry.get("total_size") or 0)
            if part.is_file() and total > 0:
                try:
                    psz = part.stat().st_size
                    if psz > total:
                        self.logger.warning(
                            "Stale oversized part %s (%d > %d); removing",
                            rel,
                            psz,
                            total,
                        )
                        part.unlink()
                        self.state["in_progress"].pop(rel, None)
                        cleaned += 1
                        continue
                except OSError:
                    pass
            if not part.is_file() and rel not in self._completed_paths:
                # Leave entry so collect_pending still knows intent; only drop
                # if the file is already completed or map no longer lists it.
                pass

        if cleaned:
            self._save_state(force=True)
            self.logger.info("Cleaned %d stale in_progress entries", cleaned)
        return cleaned

    # -- main -----------------------------------------------------------------

    def run(self) -> None:
        self.logger.info("=" * 72)
        self.logger.info("SESSION START | Map-first Drive Downloader")
        self.logger.info("=" * 72)
        self.logger.info("Destination : %s", self.config.destination_path)
        self.logger.info("Speed limit : %.2f MB/s", self.config.speed_limit_mbps)
        self.logger.info("Chunk size  : %d bytes", self.config.chunk_size)
        self.logger.info("State file  : %s", self.config.state_file)
        self.logger.info("Map file    : %s", self.config.map_file)

        try:
            self._dest_path.mkdir(parents=True, exist_ok=True)
            self.state = self._load_state()
            self._completed_paths = set(self.state["completed"].keys())
            self.cleanup_stale_in_progress()

            self.build_service()

            need_map = (
                self.config.rebuild_map
                or not self._map_path.is_file()
                or self._map_path.stat().st_size == 0
            )
            if need_map:
                self.build_file_map()
            else:
                self.logger.info(
                    "Reusing existing map: %s (set rebuild_map=true to refresh)",
                    self._map_path,
                )

            pending = self.collect_pending()
            if not pending:
                self.logger.info("Nothing to download — everything is complete.")
            else:
                self.download_pending(pending)

            self.cleanup_stale_in_progress()
            self._save_state(force=True)
            self.logger.info(
                "All done. completed=%d still_in_progress=%d",
                len(self.state["completed"]),
                len(self.state["in_progress"]),
            )
        except Exception as exc:
            self.logger.critical("FATAL: %s", exc, exc_info=True)
            self._save_state(force=True)
            raise
        finally:
            self.logger.info("=" * 72)
            self.logger.info("SESSION END")
            self.logger.info("=" * 72)


if __name__ == "__main__":
    config = "config.json"
    if len(sys.argv) > 1:
        config = sys.argv[1]
    DriveDownloader(config_path=config, log_base_dir="logs").run()
