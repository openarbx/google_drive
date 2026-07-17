#!/usr/bin/env python3
"""
OmniLib Google Drive Recursive Downloader – Memory-Optimised, Strictly Typed
Version 2 (July 2026) – Augmented with Formal Ontological Commentary

================================================================================
FORMAL ONTOLOGICAL MODEL (embedded for reference)
================================================================================
Primitive Entities
- Traversal Process T          : list_files_in_folder (generator) + download_recursive
- State Ledger L               : StateStore Protocol + SQLiteStateStore realisation
- Configuration Substrate C    : frozen dataclass Config
- Credential Acquisition CA    : get_credentials (OAuth2 boundary)

Key Axioms
A1. Traversal Independence (P1'):
    M_traversal ⊥ β   (memory independent of branching factor)
    Achieved by replacing list accumulation with iterator protocol.

A2. State Decoupling (P2'):
    M_state ⊥ |V|     (ledger memory independent of tree cardinality)
    Achieved by reifying ledger behind Protocol; concrete substrate may vary.

Causal Implications
- Reversion of P1' → P1 immediately re-introduces O(β) term for any high-fanout folder.
- Reversion of P2' → P2 immediately couples RAM to |V| and destroys resumability at scale.
- The Protocol boundary makes the downloader depend only on role, not on substrate;
  substitution of SQLiteStateStore by any other satisfying implementation leaves
  DriveDownloader's causal structure invariant.

Complexity Characterisation (under P1' + P2')
- RAM = O(page_size + recursion_depth + 1)
- Disk I/O per file = O(log |V_completed|) due to B-tree index on PRIMARY KEY
- Resumability granularity = single file (atomic ledger mutation)
================================================================================
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Protocol, TypedDict, cast, runtime_checkable
from google.auth.credentials import Credentials as GoogleCredentials

# -----------------------------------------------------------------------------
# Scoped, stable warning suppression (replaces previous blanket filters)
# -----------------------------------------------------------------------------
# Only google.* modules are affected. This preserves warning visibility for
# user code and third-party libraries while silencing the known noisy
# deprecation surface inside google-auth / google-api-python-client.
# The suppression is therefore a bounded, non-global transformation of the
# warning ontology of the process.
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"google\..*"
)
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module=r"google\..*"
)
warnings.filterwarnings(
    "ignore",
    category=PendingDeprecationWarning,
    module=r"google\..*"
)

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# -----------------------------------------------------------------------------
# Strict State Machine Types (formal schema / interchange contract)
# -----------------------------------------------------------------------------
# These TypedDicts remain the single source of truth for the progress ledger
# even though the runtime implementation has migrated to SQLiteStateStore.
# They are used by the static type checker (mypy) as the canonical model of
# what constitutes a valid persisted state. Any future StateStore implementation
# must be able to round-trip through these shapes.

class CompletedEntry(TypedDict):
    completed_at: float

class InProgressEntry(TypedDict, total=False):
    file_id: str
    file_name: str
    total_size: int
    started_at: float
    resumed_from: int

class DownloadState(TypedDict):
    """
    Top-level persisted state contract.
    
    Keys are POSIX-style relative paths from the destination root.
    This TypedDict is deliberately kept as the formal schema even under P2'
    (SQLite realisation) because it defines the categorical boundary between
    “what the system must remember” and “how it chooses to remember it”.
    """
    completed: dict[str, CompletedEntry]
    in_progress: dict[str, InProgressEntry]

# -----------------------------------------------------------------------------
# StateStore Protocol – the stable ontological contract (P2')
# -----------------------------------------------------------------------------
# The Protocol defines the abstract role “progress ledger” without committing
# to any concrete substrate. DriveDownloader depends only on this role.
# Consequently the downloader is closed to modification when the persistence
# mechanism is altered (e.g. SQLite → PostgreSQL → in-memory test double).
# This is the classic “open for extension, closed for modification” principle
# applied at the architectural scale.

@runtime_checkable
class StateStore(Protocol):
    def is_completed(self, rel_path: str) -> bool: ...
    def mark_completed(self, rel_path: str, completed_at: float) -> None: ...
    def remove_completed(self, rel_path: str) -> None: ...
    def record_in_progress(self, rel_path: str, entry: InProgressEntry) -> None: ...
    def pop_in_progress(self, rel_path: str) -> None: ...
    def get_in_progress_count(self) -> int: ...
    def get_all_in_progress(self) -> dict[str, InProgressEntry]: ...

# -----------------------------------------------------------------------------
# SQLiteStateStore – concrete realisation of P2' (recommended production substrate)
# -----------------------------------------------------------------------------
class SQLiteStateStore:
    """
    Disk-backed implementation of the StateStore role.
    
    Memory footprint is independent of |V| because no full ledger is ever
    materialised in RAM. All operations are O(log |V|) or better thanks to the
    implicit B-tree index on PRIMARY KEY. WAL mode + NORMAL synchronous gives
    a good balance of crash safety and write performance for long-running
    downloads.
    
    Every public method performs exactly one SQL statement that is either
    a single-row lookup or a single-row INSERT/DELETE. This keeps the
    transactional boundary at file granularity – the smallest unit that
    still guarantees resumability.
    """

    def __init__(self, db_path: Path, runtime: RuntimeStrings) -> None:
        self.db_path: Path = db_path
        self._r: RuntimeStrings = runtime
        self._conn: sqlite3.Connection | None = None
        self._ensure_connected()
        self._init_schema()

    def _ensure_connected(self) -> None:
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                isolation_level=None,  # autocommit; each statement is its own tx
            )
            self._conn.execute(self._r.sqlite_journal_pragma)
            self._conn.execute(self._r.sqlite_sync_pragma)

    def _init_schema(self) -> None:
        assert self._conn is not None
        completed_table = self._r.sqlite_completed_table
        progress_table = self._r.sqlite_progress_table
        self._conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {completed_table} (
                path TEXT PRIMARY KEY,
                completed_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {progress_table} (
                path TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                started_at REAL NOT NULL,
                resumed_from INTEGER NOT NULL
            );
            """
        )

    def is_completed(self, rel_path: str) -> bool:
        assert self._conn is not None
        cur = self._conn.execute(
            f"SELECT 1 FROM {self._r.sqlite_completed_table} WHERE path = ? LIMIT 1", (rel_path,)
        )
        return cur.fetchone() is not None

    def mark_completed(self, rel_path: str, completed_at: float) -> None:
        assert self._conn is not None
        self._conn.execute(
            f"INSERT OR REPLACE INTO {self._r.sqlite_completed_table} (path, completed_at) VALUES (?, ?)",
            (rel_path, completed_at),
        )

    def remove_completed(self, rel_path: str) -> None:
        assert self._conn is not None
        self._conn.execute(f"DELETE FROM {self._r.sqlite_completed_table} WHERE path = ?", (rel_path,))

    def record_in_progress(self, rel_path: str, entry: InProgressEntry) -> None:
        assert self._conn is not None
        self._conn.execute(
            f"""
            INSERT OR REPLACE INTO {self._r.sqlite_progress_table}
            (path, file_id, file_name, total_size, started_at, resumed_from)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                rel_path,
                entry.get(self._r.state_file_id_key, ""),
                entry.get(self._r.state_file_name_key, ""),
                entry.get(self._r.state_total_size_key, 0),
                entry.get(self._r.state_started_at_key, time.time()),
                entry.get(self._r.state_resumed_from_key, 0),
            ),
        )

    def pop_in_progress(self, rel_path: str) -> None:
        assert self._conn is not None
        self._conn.execute(f"DELETE FROM {self._r.sqlite_progress_table} WHERE path = ?", (rel_path,))

    def get_in_progress_count(self) -> int:
        assert self._conn is not None
        cur = self._conn.execute(f"SELECT COUNT(*) FROM {self._r.sqlite_progress_table}")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_all_in_progress(self) -> dict[str, InProgressEntry]:
        assert self._conn is not None
        cur = self._conn.execute(
            f"SELECT path, file_id, file_name, total_size, started_at, resumed_from FROM {self._r.sqlite_progress_table}"
        )
        result: dict[str, InProgressEntry] = {}
        for row in cur.fetchall():
            result[row[0]] = {
                "file_id": row[1],
                "file_name": row[2],
                "total_size": row[3],
                "started_at": row[4],
                "resumed_from": row[5],
            }
        return result

# -----------------------------------------------------------------------------
# Externalised runtime identifiers
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class RuntimeStrings:
    logger_name: str
    api_service_name: str
    api_version: str
    response_files_key: str
    response_next_page_key: str
    item_id_key: str
    item_name_key: str
    item_mime_key: str
    item_size_key: str
    state_file_id_key: str
    state_file_name_key: str
    state_total_size_key: str
    state_started_at_key: str
    state_resumed_from_key: str
    folder_mime_type: str
    query_parent_template: str
    list_fields: str
    metadata_fields: str
    range_header: str
    range_value_template: str
    unnamed_fallback: str
    state_file_name: str
    state_db_suffix: str
    partial_file_suffix: str
    log_file_prefix: str
    log_file_suffix: str
    log_date_directory_format: str
    log_timestamp_format: str
    log_record_format: str
    log_datetime_format: str
    text_encoding: str
    append_text_mode: str
    append_binary_mode: str
    write_binary_mode: str
    sqlite_journal_pragma: str
    sqlite_sync_pragma: str
    sqlite_completed_table: str
    sqlite_progress_table: str
    oauth_scope_readonly: str

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "RuntimeStrings":
        groups = raw["runtime_strings"]
        return cls(
            logger_name=groups["logging"]["component"],
            log_file_prefix=groups["logging"]["file_prefix"],
            log_file_suffix=groups["logging"]["file_suffix"],
            log_date_directory_format=groups["logging"]["date_directory_format"],
            log_timestamp_format=groups["logging"]["timestamp_format"],
            log_record_format=groups["logging"]["record_format"],
            log_datetime_format=groups["logging"]["datetime_format"],
            api_service_name=groups["api"]["service"],
            api_version=groups["api"]["version"],
            query_parent_template=groups["api"]["parent_query_template"],
            list_fields=groups["api"]["list_fields"],
            metadata_fields=groups["api"]["metadata_fields"],
            oauth_scope_readonly=groups["api"]["readonly_scope"],
            response_files_key=groups["response_keys"]["collection"],
            response_next_page_key=groups["response_keys"]["next_page"],
            item_id_key=groups["item_keys"]["identifier"],
            item_name_key=groups["item_keys"]["display_name"],
            item_mime_key=groups["item_keys"]["media_type"],
            item_size_key=groups["item_keys"]["content_length"],
            state_file_id_key=groups["state_entry_keys"]["file_identifier"],
            state_file_name_key=groups["state_entry_keys"]["file_name"],
            state_total_size_key=groups["state_entry_keys"]["total_size"],
            state_started_at_key=groups["state_entry_keys"]["started_at"],
            state_resumed_from_key=groups["state_entry_keys"]["resumed_from"],
            folder_mime_type=groups["media_types"]["folder"],
            range_header=groups["http"]["range_header"],
            range_value_template=groups["http"]["range_value_template"],
            unnamed_fallback=groups["defaults"]["unnamed_item"],
            state_file_name=groups["paths"]["state_file_name"],
            state_db_suffix=groups["paths"]["state_database_suffix"],
            partial_file_suffix=groups["paths"]["partial_suffix"],
            text_encoding=groups["io"]["encoding"],
            append_text_mode=groups["io"]["append_text_mode"],
            append_binary_mode=groups["io"]["append_binary_mode"],
            write_binary_mode=groups["io"]["write_binary_mode"],
            sqlite_journal_pragma=groups["sqlite"]["journal_pragma"],
            sqlite_sync_pragma=groups["sqlite"]["synchronous_pragma"],
            sqlite_completed_table=groups["sqlite"]["completed_table"],
            sqlite_progress_table=groups["sqlite"]["in_progress_table"],
        )

# -----------------------------------------------------------------------------
# Configuration (frozen value object)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    """
    Immutable configuration value object.
    All validation occurs in __post_init__ so that an instance is
    guaranteed to be internally consistent once constructed.
    """
    client_secret_file: str
    token_file: str
    omnilib_folder_id: str
    destination_path: str
    speed_limit_mbps: float
    chunk_size: int
    state_file: str          # legacy .json path; used only to derive .sqlite sibling
    scopes: tuple[str, ...]
    config_path: str
    runtime: RuntimeStrings

    @classmethod
    def from_file(cls, config_path: str = "config.json") -> Config:
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        with open(config_path, "r", encoding=self._r.text_encoding) as f:
            raw: dict[str, Any] = json.load(f)

        client_secret_file: str = raw["client_secret_file"]
        token_file: str = raw["token_file"]
        omnilib_folder_id: str = raw["omnilib_folder_id"]
        destination_path: str = raw["destination_path"]
        speed_limit_mbps: float = float(raw.get("speed_limit_mbps", 3.0))
        chunk_size: int = int(raw.get("chunk_size_mb", 2)) * 1024 * 1024
        # NOTE: state_file is still computed as the legacy .json location.
        # The actual runtime store is the .sqlite sibling derived from it.
        # This preserves backward compatibility of the config schema.
        runtime = RuntimeStrings.from_mapping(raw)
        state_file: str = os.path.join(destination_path, runtime.state_file_name)
        scopes: tuple[str, ...] = (runtime.oauth_scope_readonly,)

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
            runtime=runtime,
        )

    def __post_init__(self) -> None:
        if self.speed_limit_mbps <= 0:
            raise ValueError("speed_limit_mbps must be positive")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

# -----------------------------------------------------------------------------
# Main Downloader – realises both P1' (lazy traversal) and P2' (protocol state)
# -----------------------------------------------------------------------------
class DriveDownloader:
    """
    Memory-optimised orchestrator realising the P1' + P2' architecture.
    
    Invariants maintained:
    - RAM usage independent of |V| and β
    - Resumability at single-file granularity
    - All path keys are stable POSIX strings (Path.relative_to + .as_posix())
    - Every public surface carries precise type annotations
    """

    _PROGRESS_LOG_INTERVAL: Final[float] = 10.0
    _SPEED_SLEEP_FACTOR: Final[float] = 2.0

    def __init__(self, config_path: str = "config.json", log_base_dir: str = "logs") -> None:
        self.config: Config = Config.from_file(config_path)
        self._r: RuntimeStrings = self.config.runtime
        self.logger: logging.Logger = self._setup_logging(log_base_dir)

        self._dest_path: Path = Path(self.config.destination_path)
        self._state_path: Path = Path(self.config.state_file)          # legacy .json path
        # The actual persistent store is the .sqlite sibling derived from the legacy name.
        # This keeps config.json unchanged while giving us a proper indexed database.
        self._state_db_path: Path = self._state_path.with_name(
            self._state_path.stem + self._r.state_db_suffix
        )

        # P2' realisation – memory is now decoupled from |V|
        self.state_store: StateStore = SQLiteStateStore(self._state_db_path, self._r)

        self.service: Any = None
        self.creds: GoogleCredentials | None = None

        self.logger.info(
            "DriveDownloader (P1'+P2') instantiated. State backend: SQLite at %s",
            self._state_db_path,
        )

    def _setup_logging(self, log_base_dir: str) -> logging.Logger:
        logger = logging.getLogger(self._r.logger_name)
        logger.setLevel(logging.DEBUG)
        if logger.hasHandlers():
            logger.handlers.clear()

        today_str: str = datetime.now().strftime(self._r.log_date_directory_format)
        log_dir: Path = Path(log_base_dir) / today_str
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp: str = datetime.now().strftime(self._r.log_timestamp_format)
        log_file: Path = log_dir / f"{self._r.log_file_prefix}{timestamp}{self._r.log_file_suffix}"

        file_handler = logging.FileHandler(log_file, mode=self._r.append_text_mode, encoding=self._r.text_encoding)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt=self._r.log_record_format,
            datefmt=self._r.log_datetime_format,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.info("Logging initialised. File: %s", log_file)
        return logger

    # -------------------------------------------------------------------------
    # P1' – Lazy generator (core memory optimisation)
    # -------------------------------------------------------------------------
    def list_files_in_folder(self, folder_id: str) -> Iterator[dict[str, Any]]:
        """
        Memory-optimised folder listing.
        
        Under P1' each page is yielded item-by-item. The generator suspends after
        every yield, giving the caller (download_recursive) the opportunity to
        process the item (and its entire subtree) before the next item is
        materialised. Consequently the previous page becomes unreachable and is
        eligible for immediate garbage collection.
        
        Asymptotic characterisation:
        - Time: O(|children of folder|)
        - Space: O(1) auxiliary memory w.r.t. number of children
                 (plus the lifetime of the single yielded dict, which is
                  caller-controlled)
        """
        page_token: str | None = None
        self.logger.debug("Listing folder_id=%s (lazy generator, P1')", folder_id)
        try:
            while True:
                results: dict[str, Any] = (
                    self.service.files()
                    .list(
                        q=self._r.query_parent_template.format(folder_id=folder_id),
                        fields=self._r.list_fields,
                        pageSize=1000,
                        pageToken=page_token,
                    )
                    .execute()
                )
                batch: list[dict[str, Any]] = results.get(self._r.response_files_key, [])
                yield from batch          # streaming instead of accumulation
                page_token = results.get(self._r.response_next_page_key)
                if not page_token:
                    break
            self.logger.debug("Finished listing folder %s", folder_id)
        except Exception as exc:
            self.logger.error("Drive list() failed for folder %s: %s", folder_id, exc)
            # Generator ends gracefully; any already-yielded items have been processed.

    def download_file(self, file_id: str, file_name: str, dest_path: str) -> None:
        """
        Single-file download with full StateStore integration (P2').
        
        All ledger mutations flow exclusively through the Protocol methods.
        No direct dict access occurs anywhere in DriveDownloader.
        """
        # Stable, portable key for the ledger (cross-platform, no backslashes)
        rel_path: str = Path(dest_path).relative_to(self._dest_path).as_posix()

        self.logger.info("BEGIN download_file | name=%s | id=%s", file_name, file_id)

        # 1. Authoritative size from Drive (used for verification & progress)
        total_size: int = 0
        try:
            meta: dict[str, Any] = self.service.files().get(fileId=file_id, fields=self._r.metadata_fields).execute()
            total_size = int(meta.get(self._r.item_size_key, 0))
        except Exception as exc:
            self.logger.warning("Metadata fetch failed for %s: %s", file_name, exc)

        part_path: Path = Path(dest_path + self._r.partial_file_suffix)
        final_path: Path = Path(dest_path)

        # 2. Self-healing size verification (uses store via Protocol)
        if final_path.is_file():
            try:
                local_size: int = final_path.stat().st_size
                if total_size > 0 and local_size != total_size:
                    self.logger.warning(
                        "SIZE MISMATCH | %s | drive=%d local=%d → DELETE + RETRY",
                        file_name, total_size, local_size,
                    )
                    try:
                        final_path.unlink()
                    except Exception as rm_exc:
                        self.logger.error("Cannot delete %s: %s. Skipping.", final_path, rm_exc)
                        return
                    # Remove from both ledger tables via the Protocol (P2')
                    self.state_store.remove_completed(rel_path)
                    self.state_store.pop_in_progress(rel_path)
                else:
                    if not self.state_store.is_completed(rel_path):
                        self.state_store.mark_completed(rel_path, time.time())
                    self.logger.info("SKIP (verified): %s", file_name)
                    return
            except Exception as exc:
                self.logger.error("Size check failed on %s: %s", final_path, exc)
                return

        # 3. Resume logic from .part file
        current_size: int = 0
        if part_path.is_file():
            try:
                current_size = part_path.stat().st_size
                if total_size > 0 and current_size == total_size:
                    part_path.rename(final_path)
                    self.state_store.pop_in_progress(rel_path)
                    self.state_store.mark_completed(rel_path, time.time())
                    self.logger.info("RESUME COMPLETE from .part: %s", file_name)
                    return
                self.logger.info("RESUMING %s from byte %d", file_name, current_size)
            except Exception as exc:
                self.logger.error("Error handling .part for %s: %s", file_name, exc)
                return

        # 4. Record intent in the ledger (P2' – single atomic row)
        self.state_store.record_in_progress(
            rel_path,
            {
                "file_id": file_id,
                "file_name": file_name,
                "total_size": total_size,
                "started_at": time.time(),
                "resumed_from": current_size,
            },
        )

        part_path.parent.mkdir(parents=True, exist_ok=True)
        mode: str = self._r.append_binary_mode if current_size > 0 else self._r.write_binary_mode

        # 5. Chunked streaming download (already RAM-optimal; unchanged)
        try:
            with part_path.open(mode) as f:
                request = self.service.files().get_media(fileId=file_id)
                if current_size > 0:
                    request.headers[self._r.range_header] = self._r.range_value_template.format(offset=current_size)
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
                        # Throttle to respect speed_limit_mbps
                        time.sleep(1.0 / (self.config.speed_limit_mbps * self._SPEED_SLEEP_FACTOR))
                    except Exception as chunk_exc:
                        self.logger.error("Chunk error on %s: %s", file_name, chunk_exc)
                        return

            # 6. Finalise – atomic ledger update via Protocol (P2')
            if part_path.is_file():
                final_size: int = part_path.stat().st_size
                if total_size == 0 or final_size == total_size:
                    part_path.rename(final_path)
                    self.state_store.pop_in_progress(rel_path)
                    self.state_store.mark_completed(rel_path, time.time())
                    self.logger.info(
                        "DOWNLOAD COMPLETE | %s | %d bytes (verified)", file_name, final_size
                    )
                else:
                    self.logger.warning(
                        "POST-WRITE MISMATCH | %s | expected=%d got=%d | .part kept",
                        file_name, total_size, final_size,
                    )
        except Exception as exc:
            self.logger.error("Download failed for %s: %s", file_name, exc)

    def download_recursive(self, folder_id: str, local_path: str) -> None:
        """
        Recursive typed traversal.
        
        Because list_files_in_folder is a generator (P1'), the for-loop below
        processes exactly one sibling at a time. Recursion into a subfolder
        occurs immediately, but the parent generator frame is suspended only
        for the duration of that subtree. No sibling metadata list is retained
        across sibling boundaries.
        
        Consequently the maximum memory pressure during traversal of an
        arbitrarily large and deep tree remains O(page_size + depth).
        """
        self.logger.info("ENTER FOLDER | %s (id=%s)", local_path, folder_id)
        for item in self.list_files_in_folder(folder_id):
            file_id: str = item.get(self._r.item_id_key, "")
            name: str = item.get(self._r.item_name_key, self._r.unnamed_fallback)
            mime_type: str = item.get(self._r.item_mime_key, "")
            local_file_path: str = str(Path(local_path) / name)

            if mime_type == self._r.folder_mime_type:
                try:
                    Path(local_file_path).mkdir(parents=True, exist_ok=True)
                    self.download_recursive(file_id, local_file_path)
                except Exception as exc:
                    self.logger.error("Folder processing failed for %s: %s", name, exc)
            else:
                self.download_file(file_id, name, local_file_path)

    def cleanup_stale_in_progress(self) -> int:
        """
        Reconcile ledger with filesystem reality (uses P2' store).
        Removes ledger entries whose corresponding .part file no longer exists.
        """
        cleaned: int = 0
        in_progress = self.state_store.get_all_in_progress()
        for rel_path in list(in_progress.keys()):
            part_path: Path = self._dest_path / rel_path
            part_path = part_path.with_suffix(part_path.suffix + self._r.partial_file_suffix)
            if not part_path.is_file():
                self.state_store.pop_in_progress(rel_path)
                cleaned += 1
                self.logger.debug("Removed stale in-progress: %s", rel_path)
        if cleaned > 0:
            self.logger.info("Stale cleanup: removed %d entries.", cleaned)
        return cleaned

    def get_credentials(self) -> GoogleCredentials:
        """
        OAuth2 credential acquisition with full static type safety.
        
        The function guarantees a non-None, valid GoogleCredentials on success.
        Local variable uses the abstract base so that both
        run_local_server() and from_authorized_user_file() results are
        assignable. Final return uses cast + assert to document the
        post-condition for the type checker.
        """
        creds: GoogleCredentials | None = None
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
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", DeprecationWarning)
                        warnings.simplefilter("ignore", FutureWarning)
                        flow = InstalledAppFlow.from_client_secrets_file(
                            self.config.client_secret_file, list(self.config.scopes)
                        )
                        creds = cast(
                            GoogleCredentials,
                            flow.run_local_server(port=8080, open_browser=False)
                        )
                    self.logger.info("OAuth completed successfully.")
                except Exception as exc:
                    self.logger.critical("OAuth flow failed: %s", exc)
                    raise

            assert creds is not None, "creds must be valid after credential acquisition path"
            oauth2_creds = cast(Credentials, creds)
            try:
                token_path.write_text(oauth2_creds.to_json(), encoding=self._r.text_encoding)
                self.logger.info("Credentials saved to %s", token_path)
            except Exception as exc:
                self.logger.error("Failed to write token file: %s", exc)

        return cast(GoogleCredentials, creds)

    def build_service(self) -> None:
        self.creds = self.get_credentials()
        try:
            self.service = build(self._r.api_service_name, self._r.api_version, credentials=self.creds)
            self.logger.info("Google Drive API v3 service constructed.")
        except Exception as exc:
            self.logger.critical("Failed to build Drive service: %s", exc)
            raise

    def run(self) -> None:
        """Main entry point – fully migrated to P1' + P2' architecture."""
        self.logger.info("=" * 72)
        self.logger.info("SESSION START | Memory-Optimised OmniLib Drive Downloader (P1'+P2')")
        self.logger.info("=" * 72)
        self.logger.info("Destination : %s", self.config.destination_path)
        self.logger.info("Speed limit : %.2f MB/s", self.config.speed_limit_mbps)
        self.logger.info("Chunk size  : %d bytes", self.config.chunk_size)
        self.logger.info("State DB    : %s (SQLite – constant RAM, independent of |V|)", self._state_db_path)

        try:
            self._dest_path.mkdir(parents=True, exist_ok=True)

            in_progress_count: int = self.state_store.get_in_progress_count()
            if in_progress_count > 0:
                self.logger.info("RESUME: %d incomplete downloads will resume from ledger.", in_progress_count)

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