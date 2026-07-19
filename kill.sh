#!/usr/bin/env bash
# Read pid.json and terminate every recorded process (and optionally its process group).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="${SCRIPT_DIR}/pid.json"
PYTHON_BIN="${PYTHON_BIN:-python3}"
# SIGTERM first, then SIGKILL after grace period (seconds)
GRACE_SECONDS="${GRACE_SECONDS:-8}"
# If SIGNAL=KILL, send SIGKILL immediately
SIGNAL="${SIGNAL:-TERM}"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No pid file at $PID_FILE — nothing to kill."
  exit 0
fi

echo "==> Reading PIDs from $PID_FILE"

"$PYTHON_BIN" - "$PID_FILE" "$SIGNAL" "$GRACE_SECONDS" <<'PY'
import json
import os
import signal
import sys
import time
from pathlib import Path

pid_file = Path(sys.argv[1])
sig_name = sys.argv[2].upper()
grace = float(sys.argv[3])

try:
    data = json.load(pid_file.open(encoding="utf-8"))
except Exception as exc:
    print(f"ERROR: cannot read {pid_file}: {exc}", file=sys.stderr)
    sys.exit(1)

procs = data.get("processes") or []
if not procs:
    print("pid.json has no processes listed.")
    sys.exit(0)

first_sig = signal.SIGKILL if sig_name == "KILL" else signal.SIGTERM
print(f"Sending {first_sig.name} to {len(procs)} recorded PID(s) ...")

still_alive = []
for p in procs:
    pid = p.get("pid")
    if not pid:
        continue
    pid = int(pid)
    meta = f"pid={pid} config={p.get('config', '?')} started={p.get('started_at', '?')}"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        print(f"  already dead: {meta}")
        continue
    except PermissionError:
        print(f"  no permission: {meta}", file=sys.stderr)
        still_alive.append(pid)
        continue

    try:
        # Prefer killing the whole process group if this PID is a group leader
        try:
            os.killpg(pid, first_sig)
            print(f"  killpg({first_sig.name}) → {meta}")
        except (ProcessLookupError, PermissionError, OSError):
            os.kill(pid, first_sig)
            print(f"  kill({first_sig.name}) → {meta}")
        still_alive.append(pid)
    except ProcessLookupError:
        print(f"  exited during signal: {meta}")
    except PermissionError as exc:
        print(f"  permission denied {meta}: {exc}", file=sys.stderr)
        still_alive.append(pid)

if first_sig == signal.SIGKILL or grace <= 0:
    remaining = []
    for pid in still_alive:
        try:
            os.kill(pid, 0)
            remaining.append(pid)
        except OSError:
            pass
else:
    deadline = time.time() + grace
    remaining = list(still_alive)
    while remaining and time.time() < deadline:
        nxt = []
        for pid in remaining:
            try:
                os.kill(pid, 0)
                nxt.append(pid)
            except OSError:
                print(f"  exited: pid={pid}")
        remaining = nxt
        if remaining:
            time.sleep(0.25)

    if remaining:
        print(f"Escalating SIGKILL for {len(remaining)} stubborn PID(s) ...")
        for pid in remaining:
            try:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError, OSError):
                    os.kill(pid, signal.SIGKILL)
                print(f"  kill(SIGKILL) → pid={pid}")
            except ProcessLookupError:
                print(f"  already gone: pid={pid}")
            except PermissionError as exc:
                print(f"  cannot kill pid={pid}: {exc}", file=sys.stderr)

        time.sleep(0.3)
        final = []
        for pid in remaining:
            try:
                os.kill(pid, 0)
                final.append(pid)
            except OSError:
                pass
        remaining = final

# Rewrite pid.json keeping only processes still alive (should be empty)
live_entries = []
for p in procs:
    pid = p.get("pid")
    if not pid:
        continue
    try:
        os.kill(int(pid), 0)
        live_entries.append(p)
    except OSError:
        pass

out = {
    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    "processes": live_entries,
    "last_kill_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
}
with pid_file.open("w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)
    f.write("\n")

if live_entries:
    print(f"WARNING: {len(live_entries)} process(es) still alive; see {pid_file}")
    sys.exit(1)

print(f"All recorded processes stopped. Updated {pid_file}")
PY

echo "==> Done."
