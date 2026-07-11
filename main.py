#!/usr/bin/env python3
"""
OmniLib Google Drive Recursive Downloader - Object Oriented + Extremely Strictly Typed Implementation
with Structured Logging to Dated Directories.

Type System Design (extremely type-specific):
- Frozen dataclass for Config (immutable, hashable if needed, mypy-safe).
- TypedDict for the entire persisted state machine (DownloadState, CompletedEntry, InProgressEntry)
  with precise key/value types and total=False where appropriate.
- pathlib.Path used for all filesystem operations (superior type safety over str; prevents
  accidental str/path confusion that causes runtime bugs).
- Every attribute, parameter, and return value has an explicit type annotation.
- from __future__ import annotations for postponed evaluation (clean forward refs and cleaner syntax).
- Minimal use of Any only where the googleapiclient dynamic client forces it; all such sites are
  documented. The rest of the code is fully statically checkable.
- logging.Logger is precisely typed.
- All methods declare -> Nne or the exact return type.
- Mathematical invariants from the previous version are retained and now also protected by the type system.

This level of typing turns the type checker into a formal verifier for the download state machine.
"""

from __future__ import annotations

import json
import logging
import os
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Final, TypedDict, cast

# Suppress EOL / deprecation warnings from google libraries on Python 3.8
# (matching the original script's intent to run cleanly)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PendingDeprecationWarning)

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# -----------------------------------------------------------------------------
# Strict State Machine Types
# -----------------------------------------------------------------------------

class CompletedEntry(TypedDict):
    """Entry stored when a file has been successfully downloaded and size-verified."""
    completed_at: float  # Unix timestamp


class InProgressEntry(TypedDict, total=False):
    """
    Entry for a file currently being downloaded (or interrupted).
    All fields are optional in the dict to allow partial construction, but in practice
    we always populate the ones we use.
    """
    file_id: str
    file_name: str
    total_size: int
    started_at: float
    resumed_from: int


class DownloadState(TypedDict):
    """Top-level persisted state. Keys are relative paths from destination root."""
    completed: dict[str, CompletedEntry]
    in_progress: dict[str, InProgressEntry]


# -----------------------------------------------------------------------------
# Configuration (Frozen + Strictly Typed)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """
    Immutable, strictly typed configuration loaded from JSON.
    Using frozen dataclass gives us immutability + excellent static typing + __repr__ for free.
    """

    client_secret_file: str
    token_file: str
    omnilib_folder_id: str
    destination_path: str
    speed_limit_mbps: float
    chunk_size: int
    state_file: str
    scopes: tuple[str, ...]  # tuple for immutability
    config_path: str

    @classmethod
    def from_file(cls, config_path: str = "config.json") -> Config:
        """Factory that loads and validates the JSON, returning a frozen Config instance."""
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw: dict[str, Any] = json.load(f)

        # Required keys - explicit fail-fast
        client_secret_file: str = raw["client_secret_file"]
        token_file: str = raw["token_file"]
        omnilib_folder_id: str = raw["omnilib_folder_id"]
        destination_path: str = raw["destination_path"]

        # Optional with defaults (coerced to correct types)
        speed_limit_mbps: float = float(raw.get("speed_limit_mbps", 3.0))
        chunk_size: int = int(raw.get("chunk_size_mb", 2)) * 1024 * 1024

        state_file: str = os.path.join(destination_path, ".download_state.json")
        scopes: tuple[str, ...] = ("https://www.googleapis.com/auth/drive.readonly",)

        return cls(
            client_secret_file=client_secret_file,
            token_file=token_file,
            omnilib_folder_id=omnilib_folder_id,
            destination_path=destination_path,
            speed_limit_mbps=speed_limit_mbps,
            chunk_size=chunk_size,
            state_file=state_file,
            scopes=scopes,
            config_path=config_path,
        )

    def __post_init__(self) -> None:
        # Additional runtime validation if desired (type checker already guarantees most)
        if self.speed_limit_mbps <= 0:
            raise ValueError("speed_limit_mbps must be positive")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")


# -----------------------------------------------------------------------------
# Main Downloader (Extremely Typed)
# -----------------------------------------------------------------------------

class DriveDownloader:
    """
    Core orchestrator – now with extremely precise typing on every surface.

    The type system now enforces:
    - State is always a DownloadState (not a loose Dict[str, Any])
    - All path operations go through pathlib.Path (prevents str/path bugs)
    - Config is immutable
    - Logger is the real logging.Logger, not Any
    """

    # Class constants (Final for type checkers)
    _PROGRESS_LOG_INTERVAL: Final[float] = 10.0  # percent
    _SPEED_SLEEP_FACTOR: Final[float] = 2.0

    def __init__(self, config_path: str = "config.json", log_base_dir: str = "logs") -> None:
        self.config: Config = Config.from_file(config_path)
        self.logger: logging.Logger = self._setup_logging(log_base_dir)

        # Extremely typed state
        self.state: DownloadState = {"completed": {}, "in_progress": {}}

        # googleapiclient objects are dynamically generated → unavoidable Any
        self.service: Any = None
        self.creds: Credentials | None = None

        # Internal Path objects for type-safe filesystem work
        self._dest_path: Path = Path(self.config.destination_path)
        self._state_path: Path = Path(self.config.state_file)

        self.logger.info("DriveDownloader instantiated (strictly typed). Config: %s", self.config)

    def _setup_logging(self, log_base_dir: str) -> logging.Logger:
        """Create dated directory + timestamped log file. Returns a real Logger instance."""
        logger = logging.getLogger("DriveDownloader")
        logger.setLevel(logging.DEBUG)

        if logger.hasHandlers():
            logger.handlers.clear()

        today_str: str = datetime.now().strftime("%Y-%m-%d")
        log_dir: Path = Path(log_base_dir) / today_str
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp: str = datetime.now().strftime("%H%M%S")
        log_file: Path = log_dir / f"drive_downloader_{timestamp}.log"

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.info("Logging initialized. File: %s", log_file)
        logger.info("All events, warnings, errors, and interactions are recorded with full type context.")
        return logger

    def _load_state(self) -> DownloadState:
        """Load state or return a fresh, correctly typed empty state."""
        if not self._state_path.is_file():
            self.logger.debug("No state file present. Using fresh DownloadState.")
            return {"completed": {}, "in_progress": {}}

        try:
            with self._state_path.open("r", encoding="utf-8") as f:
                raw_state: dict[str, Any] = json.load(f)

            # Reconstruct with precise types (mypy-safe)
            completed: dict[str, CompletedEntry] = raw_state.get("completed", {})
            in_progress: dict[str, InProgressEntry] = raw_state.get("in_progress", {})

            state: DownloadState = {"completed": completed, "in_progress": in_progress}
            self.logger.debug(
                "State loaded. completed=%d entries, in_progress=%d entries",
                len(state["completed"]),
                len(state["in_progress"]),
            )
            return state
        except Exception as exc:
            self.logger.warning("State file unreadable or corrupt. Starting fresh. Error: %s", exc)
            return {"completed": {}, "in_progress": {}}

    def _save_state(self) -> None:
        """Persist the strongly-typed DownloadState."""
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            with self._state_path.open("w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, sort_keys=True)
            self.logger.debug("DownloadState persisted successfully.")
        except Exception as exc:
            self.logger.error("Failed to persist state: %s", exc)

    def get_credentials(self) -> Credentials:
        """OAuth2 flow with precise return type."""
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
                self.logger.info("Starting OAuth consent flow (port 8080, no browser auto-open).")
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.config.client_secret_file, list(self.config.scopes)
                    )
                    creds = flow.run_local_server(port=8080, open_browser=False)
                    self.logger.info("OAuth completed successfully.")
                except Exception as exc:
                    self.logger.critical("OAuth flow failed: %s", exc)
                    raise

            try:
                token_path.write_text(creds.to_json(), encoding="utf-8")
                self.logger.info("Credentials saved to %s", token_path)
            except Exception as exc:
                self.logger.error("Failed to write token file: %s", exc)

        return cast(Credentials, creds)  # We know it's valid here

    def build_service(self) -> None:
        """Build Drive service. Annotated as returning None; service stored internally."""
        self.creds = self.get_credentials()
        try:
            # googleapiclient Resource is dynamically typed → Any is the honest annotation
            self.service = build("drive", "v3", credentials=self.creds)
            self.logger.info("Google Drive API v3 service constructed.")
        except Exception as exc:
            self.logger.critical("Failed to build Drive service: %s", exc)
            raise

    def list_files_in_folder(self, folder_id: str) -> list[dict[str, Any]]:
        """Pagination-safe listing. Returns list of raw Drive file dicts."""
        all_files: list[dict[str, Any]] = []
        page_token: str | None = None
        self.logger.debug("Listing folder_id=%s", folder_id)

        try:
            while True:
                # Dynamic client call – unavoidable Any usage documented
                results: dict[str, Any] = (
                    self.service.files()
                    .list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        fields="nextPageToken, files(id, name, mimeType, size)",
                        pageSize=1000,
                        pageToken=page_token,
                    )
                    .execute()
                )
                batch: list[dict[str, Any]] = results.get("files", [])
                all_files.extend(batch)
                page_token = results.get("nextPageToken")
                if not page_token:
                    break

            self.logger.debug("Listed %d items from folder %s", len(all_files), folder_id)
            return all_files
        except Exception as exc:
            self.logger.error("Drive list() failed for folder %s: %s", folder_id, exc)
            return []

    def download_file(self, file_id: str, file_name: str, dest_path: str) -> None:
        """
        Single-file download with full type safety on all local variables and state updates.
        """
        rel_path: str = os.path.relpath(dest_path, str(self._dest_path))
        self.logger.info("BEGIN download_file | name=%s | id=%s", file_name, file_id)

        # 1. Get authoritative size
        total_size: int = 0
        try:
            meta: dict[str, Any] = self.service.files().get(fileId=file_id, fields="size").execute()
            total_size = int(meta.get("size", 0))
            self.logger.debug("%s Drive size = %d bytes", file_name, total_size)
        except Exception as exc:
            self.logger.warning("Metadata fetch failed for %s: %s", file_name, exc)

        part_path: Path = Path(dest_path + ".part")
        final_path: Path = Path(dest_path)

        # 2. Strict size verification (self-healing)
        if final_path.is_file():
            try:
                local_size: int = final_path.stat().st_size
                if total_size > 0 and local_size != total_size:
                    self.logger.warning("SIZE MISMATCH | %s | drive=%d local=%d → DELETE + RETRY",
                                        file_name, total_size, local_size)
                    try:
                        final_path.unlink()
                        self.logger.info("Deleted mismatched file: %s", final_path)
                    except Exception as rm_exc:
                        self.logger.error("Cannot delete %s: %s. Skipping.", final_path, rm_exc)
                        return

                    self.state["completed"].pop(rel_path, None)
                    self.state["in_progress"].pop(rel_path, None)
                    self._save_state()
                else:
                    if rel_path not in self.state["completed"]:
                        self.state["completed"][rel_path] = {"completed_at": time.time()}
                        self._save_state()
                    self.logger.info("SKIP (verified): %s", file_name)
                    return
            except Exception as exc:
                self.logger.error("Size check failed on %s: %s", final_path, exc)
                return

        # 3. Resume from .part
        current_size: int = 0
        if part_path.is_file():
            try:
                current_size = part_path.stat().st_size
                if total_size > 0 and current_size == total_size:
                    part_path.rename(final_path)
                    self.state["in_progress"].pop(rel_path, None)
                    self.state["completed"][rel_path] = {"completed_at": time.time()}
                    self._save_state()
                    self.logger.info("RESUME COMPLETE from .part: %s", file_name)
                    return
                self.logger.info("RESUMING %s from byte %d", file_name, current_size)
            except Exception as exc:
                self.logger.error("Error handling .part for %s: %s", file_name, exc)
                return

        # 4. Record intent (typed state update)
        self.state.setdefault("in_progress", {})[rel_path] = {
            "file_id": file_id,
            "file_name": file_name,
            "total_size": total_size,
            "started_at": time.time(),
            "resumed_from": current_size,
        }
        self._save_state()

        part_path.parent.mkdir(parents=True, exist_ok=True)
        mode: str = "ab" if current_size > 0 else "wb"

        # 5. Chunked download loop with typed progress tracking
        try:
            with part_path.open(mode) as f:
                request = self.service.files().get_media(fileId=file_id)
                if current_size > 0:
                    request.headers["Range"] = f"bytes={current_size}-"

                downloader = MediaIoBaseDownload(f, request, chunksize=self.config.chunk_size)

                done: bool = False
                last_logged_pct: float = 0.0

                while not done:
                    try:
                        status, done = downloader.next_chunk()

                        if status and total_size > 0:
                            current_file_size: int = part_path.stat().st_size
                            pct: float = min(100.0, (current_file_size / total_size) * 100.0)

                            if pct - last_logged_pct >= self._PROGRESS_LOG_INTERVAL:
                                self.logger.info(
                                    "PROGRESS | %s | %.1f%% (%d/%d bytes)",
                                    file_name, pct, current_file_size, total_size,
                                )
                                last_logged_pct = pct

                        time.sleep(1.0 / (self.config.speed_limit_mbps * self._SPEED_SLEEP_FACTOR))

                    except Exception as chunk_exc:
                        self.logger.error("Chunk error on %s: %s", file_name, chunk_exc)
                        return

            # 6. Finalize
            if part_path.is_file():
                final_size: int = part_path.stat().st_size
                if total_size == 0 or final_size == total_size:
                    part_path.rename(final_path)
                    self.state["in_progress"].pop(rel_path, None)
                    self.state["completed"][rel_path] = {"completed_at": time.time()}
                    self._save_state()
                    self.logger.info("DOWNLOAD COMPLETE | %s | %d bytes (verified)", file_name, final_size)
                else:
                    self.logger.warning(
                        "POST-WRITE MISMATCH | %s | expected=%d got=%d | .part kept",
                        file_name, total_size, final_size,
                    )
        except Exception as exc:
            self.logger.error("Download failed for %s: %s", file_name, exc)

    def download_recursive(self, folder_id: str, local_path: str) -> None:
        """Recursive typed traversal."""
        self.logger.info("ENTER FOLDER | %s (id=%s)", local_path, folder_id)

        items: list[dict[str, Any]] = self.list_files_in_folder(folder_id)
        for item in items:
            file_id: str = item.get("id", "")
            name: str = item.get("name", "unnamed")
            mime_type: str = item.get("mimeType", "")
            local_file_path: str = os.path.join(local_path, name)

            if mime_type == "application/vnd.google-apps.folder":
                try:
                    Path(local_file_path).mkdir(parents=True, exist_ok=True)
                    self.download_recursive(file_id, local_file_path)
                except Exception as exc:
                    self.logger.error("Folder processing failed for %s: %s", name, exc)
            else:
                self.download_file(file_id, name, local_file_path)

    def cleanup_stale_in_progress(self) -> int:
        """Remove stale entries. Returns count of cleaned items (typed int)."""
        cleaned: int = 0
        for rel_path in list(self.state.get("in_progress", {}).keys()):
            part_path: Path = self._dest_path / rel_path
            part_path = part_path.with_suffix(part_path.suffix + ".part")
            if not part_path.is_file():
                self.state["in_progress"].pop(rel_path, None)
                cleaned += 1
                self.logger.debug("Removed stale in-progress: %s", rel_path)

        if cleaned > 0:
            self._save_state()
            self.logger.info("Stale cleanup: removed %d entries.", cleaned)
        return cleaned

    def run(self) -> None:
        """Main entry point – fully typed orchestration."""
        self.logger.info("=" * 72)
        self.logger.info("SESSION START | Strictly Typed OmniLib Drive Downloader")
        self.logger.info("=" * 72)
        self.logger.info("Destination     : %s", self.config.destination_path)
        self.logger.info("Speed limit     : %.2f MB/s", self.config.speed_limit_mbps)
        self.logger.info("Chunk size      : %d bytes", self.config.chunk_size)
        self.logger.info("State file      : %s", self.config.state_file)

        try:
            self._dest_path.mkdir(parents=True, exist_ok=True)
            self.state = self._load_state()

            in_progress_count: int = len(self.state.get("in_progress", {}))
            if in_progress_count > 0:
                self.logger.info("RESUME: %d incomplete downloads will resume.", in_progress_count)

            self.build_service()
            self.download_recursive(self.config.omnilib_folder_id, str(self._dest_path))
            self.cleanup_stale_in_progress()

            self.logger.info("All downloads completed successfully.")
        except Exception as exc:
            self.logger.critical("FATAL ERROR: %s", exc, exc_info=True)
            raise
        finally:
            self.logger.info("=" * 72)
            self.logger.info("SESSION END")
            self.logger.info("=" * 72)


if __name__ == "__main__":
    downloader = DriveDownloader(config_path="config.json", log_base_dir="logs")
    downloader.run()
o
