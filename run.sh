#!/usr/bin/env bash
# Start the Google Drive downloader in the background.
# - Checks Python + required packages
# - Launches main.py detached
# - Records PID(s) in pid.json (alongside any still-running entries)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="${SCRIPT_DIR}/pid.json"
CONFIG="${CONFIG:-config.json}"
SYSTEM_PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${SCRIPT_DIR}/.venv"
LOG_DIR="${SCRIPT_DIR}/logs/runner"
mkdir -p "$LOG_DIR"
RUN_LOG="${LOG_DIR}/run_$(date +%Y%m%d_%H%M%S).log"

echo "==> Working directory: $SCRIPT_DIR"

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------
if ! command -v "$SYSTEM_PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: $SYSTEM_PYTHON_BIN not found on PATH" >&2
  exit 1
fi

# Use an isolated project venv rather than the system/--user site-packages.
# This avoids picking up mismatched system packages (e.g. an old apt
# python3-openssl alongside a newer pip-installed cryptography), which
# crashes pip/requests on import with errors like:
#   AttributeError: module 'lib' has no attribute 'X509_V_FLAG_NOTIFY_POLICY'
if [[ ! -x "${VENV_DIR}/bin/python3" ]]; then
  echo "==> Creating virtualenv at $VENV_DIR ..."
  "$SYSTEM_PYTHON_BIN" -m venv "$VENV_DIR"
fi
PYTHON_BIN="${VENV_DIR}/bin/python3"

echo "==> Using python: $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: config file not found: $CONFIG" >&2
  exit 1
fi

if [[ ! -f "main.py" ]]; then
  echo "ERROR: main.py not found in $SCRIPT_DIR" >&2
  exit 1
fi

REQ_FILE="${SCRIPT_DIR}/requirements.txt"
if [[ -f "$REQ_FILE" ]]; then
  echo "==> Checking Python packages from requirements.txt ..."
  mapfile -t REQS < <(grep -vE '^\s*(#|$)' "$REQ_FILE" | sed 's/[<>=!].*//' | tr -d ' \r')
else
  REQS=(google-api-python-client google-auth google-auth-oauthlib)
fi

MISSING=()
for pkg in "${REQS[@]}"; do
  [[ -z "$pkg" ]] && continue
  # Import name may differ slightly from dist name for these packages.
  case "$pkg" in
    google-api-python-client) import_name="googleapiclient" ;;
    google-auth-oauthlib)     import_name="google_auth_oauthlib" ;;
    google-auth)              import_name="google.auth" ;;
    *)                        import_name="${pkg//-/_}" ;;
  esac
  if ! "$PYTHON_BIN" -c "import ${import_name}" >/dev/null 2>&1; then
    MISSING+=("$pkg")
  fi
done

if ((${#MISSING[@]} > 0)); then
  echo "==> Missing packages: ${MISSING[*]}"
  echo "==> Installing via pip (into venv) ..."
  "$PYTHON_BIN" -m pip install --upgrade pip >/dev/null
  "$PYTHON_BIN" -m pip install "${MISSING[@]}"
  # Re-verify
  for pkg in "${MISSING[@]}"; do
    case "$pkg" in
      google-api-python-client) import_name="googleapiclient" ;;
      google-auth-oauthlib)     import_name="google_auth_oauthlib" ;;
      google-auth)              import_name="google.auth" ;;
      *)                        import_name="${pkg//-/_}" ;;
    esac
    if ! "$PYTHON_BIN" -c "import ${import_name}" >/dev/null 2>&1; then
      echo "ERROR: still cannot import ${import_name} after install" >&2
      exit 1
    fi
  done
  echo "==> Dependencies installed."
else
  echo "==> All Python dependencies present."
fi

# Optional: client secret / token presence (warn only)
CLIENT_SECRET="$("$PYTHON_BIN" -c "import json; print(json.load(open('${CONFIG}'))['client_secret_file'])" 2>/dev/null || true)"
if [[ -n "${CLIENT_SECRET:-}" && ! -f "$CLIENT_SECRET" ]]; then
  echo "WARNING: client_secret_file not found: $CLIENT_SECRET" >&2
fi

# ---------------------------------------------------------------------------
# Refuse to double-start the same config if an entry is still alive
# ---------------------------------------------------------------------------
if [[ -f "$PID_FILE" ]]; then
  ALIVE_COUNT=$("$PYTHON_BIN" - "$PID_FILE" <<'PY'
import json, os, sys
path = sys.argv[1]
try:
    data = json.load(open(path))
except Exception:
    data = {"processes": []}
alive = 0
for p in data.get("processes", []):
    pid = p.get("pid")
    if not pid:
        continue
    try:
        os.kill(int(pid), 0)
        alive += 1
    except OSError:
        pass
print(alive)
PY
)
  if [[ "${ALIVE_COUNT}" -gt 0 ]]; then
    echo "WARNING: ${ALIVE_COUNT} process(es) from pid.json still running."
    echo "         Use ./kill.sh first, or continue anyway by setting FORCE=1"
    if [[ "${FORCE:-0}" != "1" ]]; then
      echo "Aborted (set FORCE=1 to start another instance)."
      exit 1
    fi
  fi
fi

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------
echo "==> Starting main.py (config=$CONFIG) ..."
echo "==> Runner log: $RUN_LOG"

# nohup + redirect so SSH disconnect does not kill the job
nohup "$PYTHON_BIN" -u main.py "$CONFIG" >>"$RUN_LOG" 2>&1 &
NEW_PID=$!
disown "$NEW_PID" 2>/dev/null || true

# Brief sanity check
sleep 1
if ! kill -0 "$NEW_PID" 2>/dev/null; then
  echo "ERROR: process $NEW_PID exited immediately. Check $RUN_LOG" >&2
  tail -n 40 "$RUN_LOG" 2>/dev/null || true
  exit 1
fi

# ---------------------------------------------------------------------------
# Update pid.json (append, prune dead PIDs)
# ---------------------------------------------------------------------------
"$PYTHON_BIN" - "$PID_FILE" "$NEW_PID" "$CONFIG" "$RUN_LOG" <<'PY'
import json, os, sys, time
from pathlib import Path

pid_file, new_pid, config, run_log = sys.argv[1:5]
new_pid = int(new_pid)

if Path(pid_file).is_file():
    try:
        data = json.load(open(pid_file, encoding="utf-8"))
    except Exception:
        data = {"processes": []}
else:
    data = {"processes": []}

procs = data.get("processes") or []
live = []
for p in procs:
    pid = p.get("pid")
    if not pid:
        continue
    try:
        os.kill(int(pid), 0)
        live.append(p)
    except OSError:
        pass

live.append({
    "pid": new_pid,
    "config": config,
    "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    "started_ts": time.time(),
    "cwd": os.getcwd(),
    "command": f"python3 -u main.py {config}",
    "run_log": run_log,
})

out = {
    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    "processes": live,
}
with open(pid_file, "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)
    f.write("\n")
print(f"Wrote {pid_file} ({len(live)} live process(es))")
PY

echo "==> Started PID $NEW_PID"
echo "==> PIDs file: $PID_FILE"
echo "==> Tail live output: tail -f $RUN_LOG"
echo "==> Stop with: ./kill.sh"
