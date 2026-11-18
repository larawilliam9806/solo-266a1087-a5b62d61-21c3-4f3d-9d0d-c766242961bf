"""SQLite persistence layer.

All access goes through short-lived connections so that WAL mode and the
busy-timeout handle concurrent workers/processes uniformly.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    current_ver   INTEGER NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('processing', 'ready', 'failed')),
    error         TEXT
);

CREATE TABLE IF NOT EXISTS versions (
    run_id            TEXT NOT NULL,
    version           INTEGER NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('processing', 'ready', 'failed')),
    error             TEXT,
    total_pairs       INTEGER NOT NULL DEFAULT 0,
    assigned_pairs    INTEGER NOT NULL DEFAULT 0,
    unassigned_pairs  INTEGER NOT NULL DEFAULT 0,
    far_pairs         INTEGER NOT NULL DEFAULT 0,
    tie_pairs         INTEGER NOT NULL DEFAULT 0,
    worker_id         TEXT,
    heartbeat_at      REAL,
    PRIMARY KEY (run_id, version),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS samples (
    run_id    TEXT NOT NULL,
    version   INTEGER NOT NULL,
    sample_id TEXT NOT NULL,
    barcode   TEXT NOT NULL,
    reads     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, version, sample_id),
    UNIQUE (run_id, version, barcode)
);
"""


def connect() -> sqlite3.Connection:
    config.ensure_dirs()
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=30.0,
        isolation_level=None,  # autocommit; transactions managed explicitly
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    """Add lease columns to databases created before the liveness work."""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(versions)").fetchall()}
    if "worker_id" not in cols:
        conn.execute("ALTER TABLE versions ADD COLUMN worker_id TEXT")
    if "heartbeat_at" not in cols:
        # NULL heartbeat marks rows written by versions without a heartbeat;
        # they are treated as stale (recoverable), preserving old semantics.
        conn.execute("ALTER TABLE versions ADD COLUMN heartbeat_at REAL")


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---------- queries ----------

def get_run(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()


def get_version(conn: sqlite3.Connection, run_id: str, version: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM versions WHERE run_id = ? AND version = ?",
        (run_id, version),
    ).fetchone()


def list_samples(conn: sqlite3.Connection, run_id: str, version: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT sample_id, barcode, reads FROM samples "
        "WHERE run_id = ? AND version = ? ORDER BY sample_id",
        (run_id, version),
    ).fetchall()


def max_version(conn: sqlite3.Connection, run_id: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(version) AS m FROM versions WHERE run_id = ?", (run_id,)
    ).fetchone()
    return row["m"]
