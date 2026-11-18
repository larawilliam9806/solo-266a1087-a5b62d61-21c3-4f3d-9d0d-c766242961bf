"""Runtime configuration driven by environment variables."""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DEMUX_DATA_DIR", "/data")).resolve()
DB_PATH = DATA_DIR / "demux.db"
RUNS_DIR = DATA_DIR / "runs"

# Uploads are spooled to disk by python-multipart beyond this threshold.
MAX_UPLOAD_BYTES = int(os.environ.get("DEMUX_MAX_UPLOAD_BYTES", str(8 * 1024**3)))
BARCODE_LEN = 8
BARCODE_ALPHABET = set("ACGT")

# Processing rows are leased by the worker that owns them.  A background
# heartbeat refreshes ``versions.heartbeat_at`` every HEARTBEAT_SECONDS; a row
# whose heartbeat is older than LEASE_TIMEOUT_SECONDS is considered owned by a
# dead process and may be reaped by startup recovery or another worker.
HEARTBEAT_SECONDS = float(os.environ.get("DEMUX_HEARTBEAT_SECONDS", "2"))
LEASE_TIMEOUT_SECONDS = float(os.environ.get("DEMUX_LEASE_TIMEOUT_SECONDS", "30"))


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
