# OmniLib Google Drive Recursive Downloader

**Extremely strictly typed, object-oriented, resumable Google Drive downloader with structured logging and self-healing size verification.**

This is a production-grade reimplementation of a Google Drive recursive folder downloader. It is designed for reliability, observability, and long-term maintainability when moving large libraries (OmniLib in this case) from Google Drive to local or NAS storage.

## Core Guarantees

- **Strict size verification**: A file is only marked complete when its local size exactly matches the size reported by the Drive API (or when size is unverifiable). Mismatched files are automatically deleted and re-downloaded. This eliminates silent truncation from previous crashes or interrupted chunk transfers.
- **Resumable downloads**: Uses `.part` files and HTTP `Range` requests. Interrupted downloads resume from the last written byte on the next run.
- **Crash-safe state**: Download intent is persisted to `.download_state.json` *before* any I/O that can be interrupted. Stale in-progress entries are cleaned automatically.
- **Zero console output**: All events, decisions, warnings, errors, and progress are written to timestamped log files inside dated directories. No `print()` statements exist in the codebase.
- **Extremely strict static typing**: The entire state machine, configuration, and path handling are expressed with `TypedDict`, frozen `dataclass`, `pathlib.Path`, and explicit annotations on every name. The code is intended to pass `mypy --strict` / `pyright` with minimal `Any` usage (only where the dynamic `googleapiclient` forces it).

## Requirements

- Python 3.8+
- `google-api-python-client`
- `google-auth-oauthlib`
- `google-auth`

```bash
pip install google-api-python-client google-auth-oauthlib google-auth
```

The script has been tested on Python 3.8 (current minimum) and is written to remain compatible while using modern typing constructs.

## Configuration (`config.json`)

Create a `config.json` file next to the script with the following structure:

```json
{
  "client_secret_file": "client_secret.json",
  "token_file": "token.json",
  "omnilib_folder_id": "1A2B3C4D5E6F7G8H9I0J",
  "destination_path": "/mnt/synology/OmniLib",
  "speed_limit_mbps": 3.0,
  "chunk_size_mb": 2
}
```

**Required keys**:
- `client_secret_file`: Path to your Google OAuth client secret JSON (downloaded from Google Cloud Console).
- `token_file`: Where the script will store the OAuth token after first authentication.
- `omnilib_folder_id`: The ID of the root Google Drive folder to mirror (found in the Drive URL).
- `destination_path`: Local directory where the folder tree will be created.

**Optional keys** (with sensible defaults):
- `speed_limit_mbps`: Crude rate limit (default 3.0). The implementation sleeps after each chunk; it is not a true token bucket.
- `chunk_size_mb`: Size of each download chunk in megabytes (default 2).

## Usage

```bash
python omnilib_drive_downloader.py
```

On first run the script will open a local OAuth flow on port 8080 (no browser auto-open). Complete authentication in your browser, then the download begins.

Subsequent runs reuse the token and automatically resume any incomplete files.

## Logging Architecture

All output is written to:

```
logs/
└── YYYY-MM-DD/
    └── drive_downloader_HHMMSS.log
```

Each run creates its own timestamped log file inside the date directory. This design provides perfect traceability across multiple invocations on the same day without interleaving.

Log levels used:
- `INFO`: Major milestones, folder entry, download start/complete, resume events, size verification passes.
- `WARNING`: Size mismatches (with exact byte counts), non-fatal issues that still allow progress.
- `ERROR`: Recoverable failures on individual files (download continues for other files).
- `CRITICAL`: Fatal errors that abort the entire run.
- `DEBUG`: Internal decisions, metadata fetches, state load/save, progress at 10% intervals.

The log format includes `funcName:lineno` so every line can be traced directly back to source.

## Design Principles (Theoretical)

This implementation treats the download process as a **state machine** with explicit invariants:

1. A file transitions from `in_progress` → `completed` only after a successful atomic rename of a fully size-verified file.
2. The `.part` file is the single source of truth for resume position.
3. State is persisted *before* any operation that can be interrupted by power loss, network drop, or process kill.
4. Size mismatch is treated as a hard failure that triggers self-healing rather than a silent skip.

The extreme typing exists for a reason: in long-running data movement tools, the most expensive bugs are those that corrupt state or produce silently truncated files. By making the state shape, path handling, and configuration immutable where possible, the type checker becomes a partial verifier of these invariants.

The architecture deliberately separates concerns:
- `Config` (frozen value object)
- `DriveDownloader` (orchestrator + state machine)
- Logging setup (dated directory + structured output)
- Google API interaction (isolated behind methods that can later be mocked or replaced)

## Limitations & Honest Assessment

- Rate limiting is implemented via `time.sleep()` after every chunk. It is effective but crude. A proper token-bucket or token-bucket-with-jitter implementation would be mathematically superior for both fairness and throughput.
- Downloads are strictly sequential (depth-first). For very large libraries this is slow. Bounded concurrency with `asyncio` + semaphore is a natural next step.
- No retry with exponential backoff + jitter on transient API errors. Transient failures currently just leave a `.part` file for manual or next-run resume.
- The `googleapiclient` library is dynamically typed; a small number of `Any` annotations are unavoidable without writing a full stub.
- Python 3.8 is EOL. While the code runs, you should migrate the execution environment to Python 3.10+ for long-term security and to unlock `slots=True` + other improvements.

## Forward-Looking Improvements (Recommended)

When time permits, the following extensions map cleanly onto the current structure:

- Replace naive sleep-based throttling with a token-bucket rate limiter.
- Add exponential backoff with full jitter on API errors (optimal policy for retry storms).
- Introduce `asyncio` + `Semaphore` for concurrent downloads (bounded parallelism per folder or globally).
- Persist state in SQLite instead of JSON for better concurrency safety and queryability.
- Emit structured JSON logs (or OpenTelemetry) for centralized aggregation and metrics (success rate, bytes/sec distribution, mismatch frequency).
- Add a Prometheus exporter for real-time monitoring of active downloads and queue depth.

The current design was intentionally written to make the above changes localized rather than invasive.

## Summary

This is not a quick script. It is a deliberately engineered component for reliable, observable, long-running data transfer from Google Drive. The combination of strict size verification, resumable `.part` logic, crash-safe state, and extremely precise typing makes it suitable for moving irreplaceable or very large libraries where data integrity matters more than raw speed.

Run it, read the logs, and you will have complete visibility into every decision the downloader made.
