#!/usr/bin/env python3
"""
OmniLib Google Drive Recursive Downloader
=========================================

Object-oriented, strictly-typed, resumable Google Drive folder downloader.

Key properties:
  * Config path resolved from a `.env` file (CONFIG_PATH), falling back to
    ./config.json. Uses python-dotenv when installed; otherwise a minimal
    built-in parser (zero extra dependencies).
  * Every tunable (ints, floats, booleans, feature flags) is sourced from the
    config file: page size, OAuth port, retry counts, backoff, progress
    interval, log levels, JSON formatting, defaults.
  * Real bandwidth throttling via an average-rate limiter (true MB/s).
  * Correct resumable downloads using HTTP Range requests over an
    AuthorizedSession (byte-accurate resume; detects servers that ignore the
    Range header and restarts cleanly instead of corrupting the file).
  * Atomic state persistence (temp file + os.replace) so a crash mid-write
    can never corrupt the state file.
  * Retry with exponential backoff for transient API / network failures.
  * Filename sanitisation (path-traversal safe, illegal-char safe).
  * Google-native files (Docs/Sheets/Slides) are skipped or exported instead
    of crashing get_media.
  * Graceful Ctrl-C: state saved, .part files kept for resume.

Type-safety notes:
  * The persisted state machine uses TypedDicts with LITERAL keys internally
    ("completed", "in_progress", "file_id", ...). TypedDicts cannot be indexed
    with variables, so the configurable key names from config.json are applied
    only at the serialisation boundary (_serialize_state / _load_state).
  * Frozen dataclasses for all config groups (immutable, mypy-safe).
  * pathlib.Path for all filesystem operations.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

# Silence noisy deprecation warnings emitted by some google client versions.
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PendingDeprecationWarning)

from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Exceptions considered transient network failures (retried with backoff).
# Element type must be type[Exception] (not BaseException) so that
# `except _NETWORK_ERRORS as exc:` infers exc as Exception, matching the
# signatures of the retry helpers.
_NETWORK_ERRORS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)


# ===========================================================================
# .env handling
# ===========================================================================

def load_dotenv_file(env_path: str | os.PathLike[str] = ".env") -> None:
    """
    Populate os.environ from a .env file.

    Prefers python-dotenv when available; otherwise falls back to a minimal
    parser that understands `KEY=VALUE`, comments (`#`), blank lines and
    simple surrounding quotes. Existing environment variables are never
    overwritten, so real env vars always win over the file.
    """
    path = Path(env_path)

    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=str(path) if path.is_file() else None, override=False)
        return
    except ImportError:
        pass

    if not path.is_file():
        return

    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)
    except OSError as exc:
        # A malformed/unreadable .env should never be fatal.
        print(f"[warn] Could not read {path}: {exc}")


def resolve_config_path(env_var: str = "CONFIG_PATH", default: str = "config.json") -> str:
    """Resolve the config file path from the environment (populated from .env)."""
    return os.environ.get(env_var, default)


# ===========================================================================
# Persisted state machine (strict types, LITERAL keys)
# ===========================================================================

class CompletedEntry(TypedDict):
    completed_at: float  # Unix timestamp


class InProgressEntry(TypedDict, total=False):
    file_id: str
    file_name: str
    total_size: int
    started_at: float
    resumed_from: int


class DownloadState(TypedDict):
    completed: dict[str, CompletedEntry]
    in_progress: dict[str, InProgressEntry]


def empty_state() -> DownloadState:
    return {"completed": {}, "in_progress": {}}


# ===========================================================================
# Config groups (frozen dataclasses)
# ===========================================================================

@dataclass(frozen=True)
class RuntimeStrings:
    """All string literals used at runtime, sourced from config."""

    logger_name: str
    api_service_name: str
    api_version: str
    api_base_url: str
    media_url_template: str
    export_url_template: str
    response_files_key: str
    response_next_page_key: str
    item_id_key: str
    item_name_key: str
    item_mime_key: str
    item_size_key: str
    state_completed_key: str
    state_in_progress_key: str
    state_completed_at_key: str
    state_file_id_key: str
    state_file_name_key: str
    state_total_size_key: str
    state_started_at_key: str
    state_resumed_from_key: str
    folder_mime_type: str
    google_native_mime_prefix: str
    query_parent_template: str
    list_fields: str
    metadata_fields: str
    range_header: str
    range_value_template: str
    unnamed_fallback: str
    state_file_name: str
    partial_file_suffix: str
    log_file_prefix: str
    log_file_suffix: str
    log_date_directory_format: str
    log_timestamp_format: str
    log_record_format: str
    log_datetime_format: str
    text_encoding: str
    read_text_mode: str
    write_text_mode: str
    append_text_mode: str
    append_binary_mode: str
    write_binary_mode: str
    oauth_scope_readonly: str

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> RuntimeStrings:
        g: dict[str, Any] = raw.get("runtime_strings", {})
        logging_g: dict[str, Any] = g.get("logging", {})
        api_g: dict[str, Any] = g.get("api", {})
        resp_g: dict[str, Any] = g.get("response_keys", {})
        item_g: dict[str, Any] = g.get("item_keys", {})
        state_g: dict[str, Any] = g.get("state_keys", {})
        entry_g: dict[str, Any] = g.get("state_entry_keys", {})
        media_g: dict[str, Any] = g.get("media_types", {})
        http_g: dict[str, Any] = g.get("http", {})
        paths_g: dict[str, Any] = g.get("paths", {})
        io_g: dict[str, Any] = g.get("io", {})
        defaults_g: dict[str, Any] = g.get("defaults", {})

        return cls(
            logger_name=logging_g.get("component", "DriveDownloader"),
            log_file_prefix=logging_g.get("file_prefix", "drive_downloader_"),
            log_file_suffix=logging_g.get("file_suffix", ".log"),
            log_date_directory_format=logging_g.get("date_directory_format", "%Y-%m-%d"),
            log_timestamp_format=logging_g.get("timestamp_format", "%H%M%S"),
            log_record_format=logging_g.get(
                "record_format",
                "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
            ),
            log_datetime_format=logging_g.get("datetime_format", "%Y-%m-%d %H:%M:%S"),
            api_service_name=api_g.get("service", "drive"),
            api_version=api_g.get("version", "v3"),
            api_base_url=api_g.get("base_url", "https://www.googleapis.com/drive/v3"),
            media_url_template=api_g.get(
                "media_url_template", "{base_url}/files/{file_id}?alt=media"
            ),
            export_url_template=api_g.get(
                "export_url_template",
                "{base_url}/files/{file_id}/export?mimeType={export_mime}",
            ),
            query_parent_template=api_g.get(
                "parent_query_template", "'{folder_id}' in parents and trashed=false"
            ),
            list_fields=api_g.get(
                "list_fields", "nextPageToken, files(id, name, mimeType, size)"
            ),
            metadata_fields=api_g.get("metadata_fields", "size, name, mimeType"),
            oauth_scope_readonly=api_g.get(
                "readonly_scope", "https://www.googleapis.com/auth/drive.readonly"
            ),
            response_files_key=resp_g.get("collection", "files"),
            response_next_page_key=resp_g.get("next_page", "nextPageToken"),
            item_id_key=item_g.get("identifier", "id"),
            item_name_key=item_g.get("display_name", "name"),
            item_mime_key=item_g.get("media_type", "mimeType"),
            item_size_key=item_g.get("content_length", "size"),
            state_completed_key=state_g.get("completed", "completed"),
            state_in_progress_key=state_g.get("in_progress", "in_progress"),
            state_completed_at_key=entry_g.get("completed_at", "completed_at"),
            state_file_id_key=entry_g.get("file_identifier", "file_id"),
            state_file_name_key=entry_g.get("file_name", "file_name"),
            state_total_size_key=entry_g.get("total_size", "total_size"),
            state_started_at_key=entry_g.get("started_at", "started_at"),
            state_resumed_from_key=entry_g.get("resumed_from", "resumed_from"),
            folder_mime_type=media_g.get("folder", "application/vnd.google-apps.folder"),
            google_native_mime_prefix=media_g.get(
                "google_native_prefix", "application/vnd.google-apps."
            ),
            range_header=http_g.get("range_header", "Range"),
            range_value_template=http_g.get("range_value_template", "bytes={offset}-"),
            unnamed_fallback=defaults_g.get("unnamed_item", "unnamed"),
            state_file_name=paths_g.get("state_file_name", ".download_state.json"),
            partial_file_suffix=paths_g.get("partial_suffix", ".part"),
            text_encoding=io_g.get("encoding", "utf-8"),
            read_text_mode=io_g.get("read_text_mode", "r"),
            write_text_mode=io_g.get("write_text_mode", "w"),
            append_text_mode=io_g.get("append_text_mode", "a"),
            append_binary_mode=io_g.get("append_binary_mode", "ab"),
            write_binary_mode=io_g.get("write_binary_mode", "wb"),
        )


@dataclass(frozen=True)
class NetworkConfig:
    """Numeric / network tunables (previously hard-coded literals)."""

    oauth_port: int
    oauth_open_browser: bool
    list_page_size: int
    http_timeout_seconds: float
    max_retries: int
    retry_backoff_base_seconds: float
    retry_backoff_max_seconds: float
    retry_status_codes: tuple[int, ...]

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> NetworkConfig:
        g: dict[str, Any] = raw.get("network", {})
        return cls(
            oauth_port=int(g.get("oauth_port", 8080)),
            oauth_open_browser=bool(g.get("oauth_open_browser", False)),
            list_page_size=int(g.get("list_page_size", 1000)),
            http_timeout_seconds=float(g.get("http_timeout_seconds", 300.0)),
            max_retries=int(g.get("max_retries", 5)),
            retry_backoff_base_seconds=float(g.get("retry_backoff_base_seconds", 1.0)),
            retry_backoff_max_seconds=float(g.get("retry_backoff_max_seconds", 60.0)),
            retry_status_codes=tuple(
                int(c) for c in g.get("retry_status_codes", [429, 500, 502, 503, 504])
            ),
        )


@dataclass(frozen=True)
class BehaviorConfig:
    """Feature flags and behavioural numeric tunables."""

    progress_log_interval_percent: float
    speed_limit_mbps_default: float
    chunk_size_mb_default: int
    verify_size: bool
    delete_on_size_mismatch: bool
    skip_google_native_files: bool
    export_google_native_files: bool
    sanitize_filenames: bool
    google_native_export_map: dict[str, list[str]]

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> BehaviorConfig:
        g: dict[str, Any] = raw.get("behavior", {})
        default_export_map: dict[str, list[str]] = {
            "application/vnd.google-apps.document": ["application/pdf", ".pdf"],
            "application/vnd.google-apps.spreadsheet": [
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ".xlsx",
            ],
            "application/vnd.google-apps.presentation": [
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ".pptx",
            ],
        }
        return cls(
            progress_log_interval_percent=float(g.get("progress_log_interval_percent", 10.0)),
            speed_limit_mbps_default=float(g.get("speed_limit_mbps_default", 3.0)),
            chunk_size_mb_default=int(g.get("chunk_size_mb_default", 2)),
            verify_size=bool(g.get("verify_size", True)),
            delete_on_size_mismatch=bool(g.get("delete_on_size_mismatch", True)),
            skip_google_native_files=bool(g.get("skip_google_native_files", True)),
            export_google_native_files=bool(g.get("export_google_native_files", False)),
            sanitize_filenames=bool(g.get("sanitize_filenames", True)),
            google_native_export_map=dict(g.get("google_native_export_map", default_export_map)),
        )


@dataclass(frozen=True)
class LoggingConfig:
    """Log-handler tunables (levels, console mirroring, base directory)."""

    base_dir: str
    file_level: str
    console_level: str
    log_to_console: bool

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> LoggingConfig:
        g: dict[str, Any] = raw.get("logging_config", {})
        return cls(
            base_dir=str(g.get("base_dir", "logs")),
            file_level=str(g.get("file_level", "DEBUG")).upper(),
            console_level=str(g.get("console_level", "INFO")).upper(),
            log_to_console=bool(g.get("log_to_console", True)),
        )


@dataclass(frozen=True)
class JsonConfig:
    """State-file JSON serialisation options."""

    indent: int
    sort_keys: bool

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> JsonConfig:
        g: dict[str, Any] = raw.get("json", {})
        return cls(
            indent=int(g.get("indent", 2)),
            sort_keys=bool(g.get("sort_keys", True)),
        )


# ===========================================================================
# Top-level immutable Config
# ===========================================================================

class ConfigError(Exception):
    """Raised when the config file is missing required keys or is invalid."""


@dataclass(frozen=True)
class Config:
    client_secret_file: str
    token_file: str
    omnilib_folder_id: str
    destination_path: str
    speed_limit_mbps: float
    chunk_size: int
    state_file: str
    scopes: tuple[str, ...]
    config_path: str
    runtime: RuntimeStrings
    network: NetworkConfig
    behavior: BehaviorConfig
    logging: LoggingConfig
    json_opts: JsonConfig

    @classmethod
    def from_file(cls, config_path: str = "config.json") -> Config:
        path = Path(config_path)
        if not path.is_file():
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        try:
            raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Config file is not valid JSON ({config_path}): {exc}") from exc
        except OSError as exc:
            raise ConfigError(f"Could not read config file {config_path}: {exc}") from exc

        runtime = RuntimeStrings.from_mapping(raw)
        network = NetworkConfig.from_mapping(raw)
        behavior = BehaviorConfig.from_mapping(raw)
        logging_cfg = LoggingConfig.from_mapping(raw)
        json_opts = JsonConfig.from_mapping(raw)

        required = (
            "client_secret_file",
            "token_file",
            "omnilib_folder_id",
            "destination_path",
        )
        missing = [k for k in required if k not in raw]
        if missing:
            raise ConfigError(f"Missing required config keys: {', '.join(missing)}")

        destination_path = str(raw["destination_path"])
        speed_limit_mbps = float(raw.get("speed_limit_mbps", behavior.speed_limit_mbps_default))
        chunk_size = int(raw.get("chunk_size_mb", behavior.chunk_size_mb_default)) * 1024 * 1024
        state_file = str(Path(destination_path) / runtime.state_file_name)

        return cls(
            client_secret_file=str(raw["client_secret_file"]),
            token_file=str(raw["token_file"]),
            omnilib_folder_id=str(raw["omnilib_folder_id"]),
            destination_path=destination_path,
            speed_limit_mbps=speed_limit_mbps,
            chunk_size=chunk_size,
            state_file=state_file,
            scopes=(runtime.oauth_scope_readonly,),
            config_path=config_path,
            runtime=runtime,
            network=network,
            behavior=behavior,
            logging=logging_cfg,
            json_opts=json_opts,
        )

    def __post_init__(self) -> None:
        if self.speed_limit_mbps <= 0:
            raise ValueError("speed_limit_mbps must be positive")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.network.max_retries < 1:
            raise ValueError("network.max_retries must be >= 1")


# ===========================================================================
# Rate limiter
# ===========================================================================

class RateLimiter:
    """
    Average-rate limiter. After `consume(n)`, sleeps just enough that the
    cumulative throughput since construction does not exceed
    `bytes_per_second`. Create a fresh instance per file so idle gaps between
    files never count toward the budget.
    """

    def __init__(self, bytes_per_second: float) -> None:
        self._rate: float = max(0.0, bytes_per_second)
        self._start: float = time.monotonic()
        self._sent: int = 0

    def consume(self, n_bytes: int) -> None:
        if self._rate <= 0 or n_bytes <= 0:
            return
        self._sent += n_bytes
        expected_elapsed = self._sent / self._rate
        actual_elapsed = time.monotonic() - self._start
        drift = expected_elapsed - actual_elapsed
        if drift > 0:
            time.sleep(drift)


# ===========================================================================
# Main downloader
# ===========================================================================

class DriveDownloader:
    def __init__(self, config_path: str | None = None, log_base_dir: str | None = None) -> None:
        resolved_config = config_path or resolve_config_path()
        self.config: Config = Config.from_file(resolved_config)
        self._r: RuntimeStrings = self.config.runtime

        self.logger: logging.Logger = self._setup_logging(
            log_base_dir or self.config.logging.base_dir
        )

        # Internal state uses literal keys (TypedDict-safe). Config-defined
        # key names are applied only when reading/writing the state file.
        self.state: DownloadState = empty_state()

        # googleapiclient's Resource is dynamically generated -> honest Any.
        self.service: Any = None
        self.session: AuthorizedSession | None = None
        self.creds: Credentials | None = None

        self._dest_path: Path = Path(self.config.destination_path)
        self._state_path: Path = Path(self.config.state_file)

        self.logger.info("DriveDownloader initialised. Config source: %s", resolved_config)

    # -- logging ----------------------------------------------------------

    @staticmethod
    def _level(name: str) -> int:
        value = logging.getLevelName(name)
        return value if isinstance(value, int) else logging.INFO

    def _setup_logging(self, log_base_dir: str) -> logging.Logger:
        logger = logging.getLogger(self._r.logger_name)
        logger.setLevel(logging.DEBUG)
        if logger.hasHandlers():
            logger.handlers.clear()

        today = datetime.now().strftime(self._r.log_date_directory_format)
        log_dir = Path(log_base_dir) / today
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime(self._r.log_timestamp_format)
        log_file = log_dir / f"{self._r.log_file_prefix}{timestamp}{self._r.log_file_suffix}"

        formatter = logging.Formatter(
            fmt=self._r.log_record_format, datefmt=self._r.log_datetime_format
        )

        file_handler = logging.FileHandler(
            log_file, mode=self._r.append_text_mode, encoding=self._r.text_encoding
        )
        file_handler.setLevel(self._level(self.config.logging.file_level))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if self.config.logging.log_to_console:
            console = logging.StreamHandler()
            console.setLevel(self._level(self.config.logging.console_level))
            console.setFormatter(formatter)
            logger.addHandler(console)

        logger.info("Logging initialised. File: %s", log_file)
        return logger

    # -- filename hygiene -------------------------------------------------

    def _sanitize_name(self, name: str) -> str:
        """Make a Drive display name safe to use as a single path component."""
        if not self.config.behavior.sanitize_filenames:
            return name or self._r.unnamed_fallback
        cleaned = name.replace("/", "_").replace("\\", "_")
        cleaned = re.sub(r"[\x00-\x1f]", "", cleaned)  # strip control chars
        cleaned = cleaned.strip().strip(".")           # no leading/trailing dots/space
        if cleaned in ("", ".", ".."):
            cleaned = self._r.unnamed_fallback
        return cleaned[:255]  # typical filesystem component limit

    # -- state (de)serialisation with configurable key names --------------

    def _serialize_state(self) -> dict[str, Any]:
        """Translate the literal-keyed internal state to config key names."""
        completed_out: dict[str, dict[str, float]] = {
            rel: {self._r.state_completed_at_key: entry["completed_at"]}
            for rel, entry in self.state["completed"].items()
        }
        in_progress_out: dict[str, dict[str, Any]] = {}
        for rel, entry in self.state["in_progress"].items():
            row: dict[str, Any] = {}
            if "file_id" in entry:
                row[self._r.state_file_id_key] = entry["file_id"]
            if "file_name" in entry:
                row[self._r.state_file_name_key] = entry["file_name"]
            if "total_size" in entry:
                row[self._r.state_total_size_key] = entry["total_size"]
            if "started_at" in entry:
                row[self._r.state_started_at_key] = entry["started_at"]
            if "resumed_from" in entry:
                row[self._r.state_resumed_from_key] = entry["resumed_from"]
            in_progress_out[rel] = row
        return {
            self._r.state_completed_key: completed_out,
            self._r.state_in_progress_key: in_progress_out,
        }

    def _deserialize_completed(self, raw: dict[str, Any]) -> CompletedEntry:
        return {"completed_at": float(raw.get(self._r.state_completed_at_key, 0.0))}

    def _deserialize_in_progress(self, raw: dict[str, Any]) -> InProgressEntry:
        entry: InProgressEntry = {}
        if self._r.state_file_id_key in raw:
            entry["file_id"] = str(raw[self._r.state_file_id_key])
        if self._r.state_file_name_key in raw:
            entry["file_name"] = str(raw[self._r.state_file_name_key])
        if self._r.state_total_size_key in raw:
            entry["total_size"] = int(raw[self._r.state_total_size_key])
        if self._r.state_started_at_key in raw:
            entry["started_at"] = float(raw[self._r.state_started_at_key])
        if self._r.state_resumed_from_key in raw:
            entry["resumed_from"] = int(raw[self._r.state_resumed_from_key])
        return entry

    def _load_state(self) -> DownloadState:
        if not self._state_path.is_file():
            self.logger.debug("No state file present; starting fresh.")
            return empty_state()
        try:
            raw_state: dict[str, Any] = json.loads(
                self._state_path.read_text(encoding=self._r.text_encoding)
            )
            raw_completed: dict[str, Any] = raw_state.get(self._r.state_completed_key, {})
            raw_in_progress: dict[str, Any] = raw_state.get(self._r.state_in_progress_key, {})

            state: DownloadState = {
                "completed": {
                    rel: self._deserialize_completed(entry)
                    for rel, entry in raw_completed.items()
                    if isinstance(entry, dict)
                },
                "in_progress": {
                    rel: self._deserialize_in_progress(entry)
                    for rel, entry in raw_in_progress.items()
                    if isinstance(entry, dict)
                },
            }
            self.logger.debug(
                "State loaded: completed=%d, in_progress=%d",
                len(state["completed"]),
                len(state["in_progress"]),
            )
            return state
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            self.logger.warning("State file unreadable/corrupt; starting fresh: %s", exc)
            return empty_state()

    def _save_state(self) -> None:
        """Atomic write: serialise to a temp file, then os.replace into place."""
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_name(self._state_path.name + ".tmp")
            with tmp.open(self._r.write_text_mode, encoding=self._r.text_encoding) as f:
                json.dump(
                    self._serialize_state(),
                    f,
                    indent=self.config.json_opts.indent,
                    sort_keys=self.config.json_opts.sort_keys,
                )
            os.replace(tmp, self._state_path)  # atomic on POSIX and Windows
            self.logger.debug("State persisted (atomic).")
        except OSError as exc:
            self.logger.error("Failed to persist state: %s", exc)

    def _mark_completed(self, rel_path: str) -> None:
        self.state["in_progress"].pop(rel_path, None)
        self.state["completed"][rel_path] = {"completed_at": time.time()}
        self._save_state()

    # -- auth / service ---------------------------------------------------

    def get_credentials(self) -> Credentials:
        creds: Credentials | None = None
        token_path = Path(self.config.token_file)

        if token_path.is_file():
            try:
                creds = Credentials.from_authorized_user_file(
                    str(token_path), list(self.config.scopes)
                )
                self.logger.debug("Loaded cached credentials.")
            except (ValueError, OSError) as exc:
                self.logger.warning("Token file invalid: %s", exc)

        if creds is not None and not creds.valid and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                self.logger.info("Credentials refreshed.")
            except Exception as exc:  # refresh raises library-specific types
                self.logger.error("Refresh failed: %s", exc)
                creds = None

        if creds is None or not creds.valid:
            self.logger.info(
                "Starting OAuth consent flow (port %d, open_browser=%s).",
                self.config.network.oauth_port,
                self.config.network.oauth_open_browser,
            )
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config.client_secret_file, list(self.config.scopes)
                )
                # run_local_server is declared to return a union of
                # oauth2 Credentials and external-account Credentials; an
                # InstalledAppFlow with a client-secret file always yields the
                # oauth2 kind, so narrow with isinstance (typed + runtime-safe).
                flow_creds = flow.run_local_server(
                    port=self.config.network.oauth_port,
                    open_browser=self.config.network.oauth_open_browser,
                )
                if not isinstance(flow_creds, Credentials):
                    raise RuntimeError(
                        "OAuth flow returned unsupported credential type: "
                        f"{type(flow_creds).__name__}"
                    )
                creds = flow_creds
                self.logger.info("OAuth completed.")
            except Exception as exc:
                self.logger.critical("OAuth flow failed: %s", exc)
                raise

        if creds is None:
            raise RuntimeError("OAuth flow did not yield credentials.")

        try:
            token_path.write_text(creds.to_json(), encoding=self._r.text_encoding)
            self.logger.info("Credentials saved to %s", token_path)
        except OSError as exc:
            self.logger.error("Failed to write token file: %s", exc)

        return creds

    def build_service(self) -> None:
        creds = self.get_credentials()
        self.creds = creds
        try:
            self.service = build(
                self._r.api_service_name, self._r.api_version, credentials=creds
            )
            self.session = AuthorizedSession(creds)
            self.logger.info("Google Drive API service + media session constructed.")
        except Exception as exc:
            self.logger.critical("Failed to build Drive service: %s", exc)
            raise

    def _require_session(self) -> AuthorizedSession:
        if self.session is None:
            raise RuntimeError("Service not built. Call build_service() first.")
        return self.session

    # -- retry helper -----------------------------------------------------

    def _backoff_seconds(self, attempt: int) -> float:
        base = self.config.network.retry_backoff_base_seconds
        cap = self.config.network.retry_backoff_max_seconds
        return min(cap, base * (2 ** (attempt - 1)))

    def _sleep_before_retry(self, attempt: int, description: str, exc: Exception) -> None:
        wait = self._backoff_seconds(attempt)
        self.logger.warning(
            "Retry %d/%d for %s in %.1fs: %s",
            attempt, self.config.network.max_retries, description, wait, exc,
        )
        time.sleep(wait)

    @staticmethod
    def _http_status(exc: HttpError) -> int | None:
        resp = getattr(exc, "resp", None)
        status = getattr(resp, "status", None)
        return status if isinstance(status, int) else None

    def _execute_with_retry(self, request: Any, description: str) -> dict[str, Any]:
        """Run a googleapiclient request.execute() with exponential backoff."""
        max_retries = self.config.network.max_retries
        for attempt in range(1, max_retries + 1):
            try:
                result: dict[str, Any] = request.execute()
                return result
            except HttpError as exc:
                status = self._http_status(exc)
                retryable = status in self.config.network.retry_status_codes
                if not retryable or attempt >= max_retries:
                    raise
                self._sleep_before_retry(attempt, description, exc)
            except _NETWORK_ERRORS as exc:
                if attempt >= max_retries:
                    raise
                self._sleep_before_retry(attempt, description, exc)
        raise RuntimeError(f"Retry loop exhausted unexpectedly for {description}")

    # -- listing ----------------------------------------------------------

    def list_files_in_folder(self, folder_id: str) -> list[dict[str, Any]]:
        all_files: list[dict[str, Any]] = []
        page_token: str | None = None
        self.logger.debug("Listing folder_id=%s", folder_id)
        try:
            while True:
                request = self.service.files().list(
                    q=self._r.query_parent_template.format(folder_id=folder_id),
                    fields=self._r.list_fields,
                    pageSize=self.config.network.list_page_size,
                    pageToken=page_token,
                )
                results = self._execute_with_retry(request, f"list({folder_id})")
                batch: list[dict[str, Any]] = results.get(self._r.response_files_key, [])
                all_files.extend(batch)
                page_token = results.get(self._r.response_next_page_key)
                if not page_token:
                    break
            self.logger.debug("Listed %d items from %s", len(all_files), folder_id)
            return all_files
        except (HttpError, *_NETWORK_ERRORS) as exc:
            self.logger.error("Drive list() failed for %s: %s", folder_id, exc)
            return []

    # -- streaming download -----------------------------------------------

    def _stream_to_part(
        self, url: str, part_path: Path, total_size: int, file_name: str
    ) -> int:
        """
        Stream `url` into `part_path` with Range-based resume + throttling.
        Returns the final size of the .part file. Raises on failure so the
        caller's retry loop can resume from the bytes already written.
        """
        session = self._require_session()
        current_size = part_path.stat().st_size if part_path.is_file() else 0

        headers: dict[str, str] = {}
        if 0 < current_size and (total_size == 0 or current_size < total_size):
            headers[self._r.range_header] = self._r.range_value_template.format(
                offset=current_size
            )
            self.logger.info("RESUMING %s from byte %d", file_name, current_size)

        limiter = RateLimiter(self.config.speed_limit_mbps * 1024 * 1024)

        with session.get(
            url,
            headers=headers,
            stream=True,
            timeout=self.config.network.http_timeout_seconds,
        ) as resp:
            resp.raise_for_status()

            # If we asked for a range but the server ignored it (200, not 206),
            # restart from scratch to avoid appending onto stale bytes.
            append = bool(headers) and resp.status_code == 206
            mode = self._r.append_binary_mode if append else self._r.write_binary_mode
            written = current_size if append else 0

            interval = self.config.behavior.progress_log_interval_percent
            last_pct = 0.0

            part_path.parent.mkdir(parents=True, exist_ok=True)
            with part_path.open(mode) as f:
                for chunk in resp.iter_content(chunk_size=self.config.chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    written += len(chunk)
                    limiter.consume(len(chunk))
                    if total_size > 0:
                        pct = min(100.0, written / total_size * 100.0)
                        if pct - last_pct >= interval:
                            self.logger.info(
                                "PROGRESS | %s | %.1f%% (%d/%d bytes)",
                                file_name, pct, written, total_size,
                            )
                            last_pct = pct

        return part_path.stat().st_size if part_path.is_file() else 0

    # -- single file ------------------------------------------------------

    def download_file(self, file_id: str, file_name: str, dest_path: str) -> None:
        rel_path = os.path.relpath(dest_path, str(self._dest_path))
        final_path = Path(dest_path)
        part_path = Path(dest_path + self._r.partial_file_suffix)
        self.logger.info("BEGIN | name=%s | id=%s", file_name, file_id)

        # 1. Authoritative size from metadata.
        total_size = 0
        try:
            meta = self._execute_with_retry(
                self.service.files().get(fileId=file_id, fields=self._r.metadata_fields),
                f"metadata({file_id})",
            )
            total_size = int(meta.get(self._r.item_size_key, 0) or 0)
            self.logger.debug("%s size = %d bytes", file_name, total_size)
        except (HttpError, *_NETWORK_ERRORS) as exc:
            self.logger.warning("Metadata fetch failed for %s: %s", file_name, exc)

        # 2. Size verification / self-healing of an existing final file.
        if final_path.is_file():
            if not self.config.behavior.verify_size:
                self.logger.info("SKIP (exists, verify disabled): %s", file_name)
                return
            try:
                local_size = final_path.stat().st_size
            except OSError as exc:
                self.logger.error("Size check failed on %s: %s", final_path, exc)
                return

            if total_size > 0 and local_size != total_size:
                self.logger.warning(
                    "SIZE MISMATCH | %s | drive=%d local=%d",
                    file_name, total_size, local_size,
                )
                if not self.config.behavior.delete_on_size_mismatch:
                    return
                try:
                    final_path.unlink()
                    self.logger.info("Deleted mismatched file: %s", final_path)
                except OSError as rm_exc:
                    self.logger.error("Cannot delete %s: %s. Skipping.", final_path, rm_exc)
                    return
                self.state["completed"].pop(rel_path, None)
                self.state["in_progress"].pop(rel_path, None)
                self._save_state()
            else:
                if rel_path not in self.state["completed"]:
                    self._mark_completed(rel_path)
                self.logger.info("SKIP (verified): %s", file_name)
                return

        # 3. Fast path: a complete .part just needs renaming.
        if part_path.is_file() and total_size > 0 and part_path.stat().st_size == total_size:
            part_path.replace(final_path)
            self._mark_completed(rel_path)
            self.logger.info("RESUME COMPLETE from .part: %s", file_name)
            return

        # 4. Record intent.
        resumed_from = part_path.stat().st_size if part_path.is_file() else 0
        self.state["in_progress"][rel_path] = {
            "file_id": file_id,
            "file_name": file_name,
            "total_size": total_size,
            "started_at": time.time(),
            "resumed_from": resumed_from,
        }
        self._save_state()

        # 5. Download with retry; each attempt resumes from the current .part.
        url = self._r.media_url_template.format(base_url=self._r.api_base_url, file_id=file_id)
        max_retries = self.config.network.max_retries
        for attempt in range(1, max_retries + 1):
            try:
                final_size = self._stream_to_part(url, part_path, total_size, file_name)
            except _NETWORK_ERRORS as exc:
                if attempt >= max_retries:
                    self.logger.error(
                        "Giving up on %s after %d attempts: %s", file_name, attempt, exc
                    )
                    return
                self._sleep_before_retry(attempt, file_name, exc)
                continue
            except Exception as exc:
                # requests.HTTPError from raise_for_status lands here too;
                # treat unexpected errors as non-retryable to avoid loops.
                self.logger.error("Download failed for %s: %s", file_name, exc)
                return

            if total_size == 0 or final_size == total_size:
                part_path.replace(final_path)
                self._mark_completed(rel_path)
                self.logger.info("DOWNLOAD COMPLETE | %s | %d bytes", file_name, final_size)
            else:
                self.logger.warning(
                    "POST-WRITE MISMATCH | %s | expected=%d got=%d | .part kept",
                    file_name, total_size, final_size,
                )
            return

    # -- google-native handling -------------------------------------------

    def _handle_google_native(self, file_id: str, name: str, dest_path: str, mime: str) -> None:
        if self.config.behavior.export_google_native_files:
            mapping = self.config.behavior.google_native_export_map.get(mime)
            if mapping is not None and len(mapping) >= 2:
                export_mime, ext = mapping[0], mapping[1]
                url = self._r.export_url_template.format(
                    base_url=self._r.api_base_url, file_id=file_id, export_mime=export_mime
                )
                export_final = Path(dest_path + ext)
                export_part = Path(str(export_final) + self._r.partial_file_suffix)
                if export_final.is_file():
                    self.logger.info("SKIP export (exists): %s", export_final.name)
                    return
                self.logger.info("EXPORT native %s -> %s", name, export_final)
                try:
                    self._stream_to_part(url, export_part, 0, name)
                    export_part.replace(export_final)
                except Exception as exc:
                    self.logger.error("Export failed for %s: %s", name, exc)
                return
        self.logger.info("SKIP google-native (non-downloadable): %s (%s)", name, mime)

    # -- recursion ---------------------------------------------------------

    def download_recursive(self, folder_id: str, local_path: str) -> None:
        self.logger.info("ENTER FOLDER | %s (id=%s)", local_path, folder_id)
        for item in self.list_files_in_folder(folder_id):
            file_id: str = item.get(self._r.item_id_key, "")
            raw_name: str = item.get(self._r.item_name_key, self._r.unnamed_fallback)
            name = self._sanitize_name(raw_name)
            mime: str = item.get(self._r.item_mime_key, "")
            child_path = os.path.join(local_path, name)

            if mime == self._r.folder_mime_type:
                try:
                    Path(child_path).mkdir(parents=True, exist_ok=True)
                    self.download_recursive(file_id, child_path)
                except OSError as exc:
                    self.logger.error("Folder processing failed for %s: %s", name, exc)
            elif mime.startswith(self._r.google_native_mime_prefix):
                self._handle_google_native(file_id, name, child_path, mime)
            else:
                self.download_file(file_id, name, child_path)

    # -- cleanup -----------------------------------------------------------

    def cleanup_stale_in_progress(self) -> int:
        cleaned = 0
        for rel_path in list(self.state["in_progress"].keys()):
            part_path = self._dest_path / (rel_path + self._r.partial_file_suffix)
            if not part_path.is_file():
                self.state["in_progress"].pop(rel_path, None)
                cleaned += 1
                self.logger.debug("Removed stale in-progress: %s", rel_path)
        if cleaned:
            self._save_state()
            self.logger.info("Stale cleanup: removed %d entries.", cleaned)
        return cleaned

    # -- orchestration ------------------------------------------------------

    def run(self) -> None:
        sep = "=" * 72
        self.logger.info(sep)
        self.logger.info("SESSION START | OmniLib Drive Downloader")
        self.logger.info(sep)
        self.logger.info("Destination : %s", self.config.destination_path)
        self.logger.info("Speed limit : %.2f MB/s", self.config.speed_limit_mbps)
        self.logger.info("Chunk size  : %d bytes", self.config.chunk_size)
        self.logger.info("State file  : %s", self.config.state_file)

        try:
            self._dest_path.mkdir(parents=True, exist_ok=True)
            self.state = self._load_state()

            in_progress = len(self.state["in_progress"])
            if in_progress:
                self.logger.info("RESUME: %d incomplete downloads will resume.", in_progress)

            self.build_service()
            self.download_recursive(self.config.omnilib_folder_id, str(self._dest_path))
            self.cleanup_stale_in_progress()
            self.logger.info("All downloads completed successfully.")
        except KeyboardInterrupt:
            self.logger.warning("Interrupted by user. State saved; .part files kept for resume.")
            self._save_state()
        except Exception as exc:
            self.logger.critical("FATAL ERROR: %s", exc, exc_info=True)
            raise
        finally:
            self.logger.info(sep)
            self.logger.info("SESSION END")
            self.logger.info(sep)


def main() -> None:
    load_dotenv_file()  # populate os.environ from .env (CONFIG_PATH, etc.)
    downloader = DriveDownloader()  # config path resolved from env / default
    downloader.run()


if __name__ == "__main__":
    main()