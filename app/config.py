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


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
