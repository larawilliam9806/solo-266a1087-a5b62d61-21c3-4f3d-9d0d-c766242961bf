"""Service layer: run lifecycle, versioned re-demultiplexing and recovery."""
from __future__ import annotations

import re
import sqlite3
import threading
from pathlib import Path

from . import config, database
from .demux import DemuxStats, demultiplex
from .errors import ConflictError, NotFoundError, ServiceError, StorageError, ValidationError
from .sample_sheet import parse_sample_sheet

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")
_SPOOL_CHUNK = 1024 * 1024

# Serialise same-run corrections inside this process.  Cross-process safety is
# additionally guaranteed by the BEGIN IMMEDIATE SQLite transaction.
_run_locks: dict[str, threading.Lock] = {}
_run_locks_guard = threading.Lock()


def _run_lock(run_id: str) -> threading.Lock:
    with _run_locks_guard:
        lock = _run_locks.get(run_id)
        if lock is None:
            lock = threading.Lock()
            _run_locks[run_id] = lock
        return lock


# ---------- paths ----------

def run_dir(run_id: str) -> Path:
    return config.RUNS_DIR / run_id


def originals_dir(run_id: str) -> Path:
    return run_dir(run_id) / "originals"


def version_dir(run_id: str, version: int) -> Path:
    return run_dir(run_id) / f"v{version}"


def samples_dir(run_id: str, version: int) -> Path:
    return version_dir(run_id, version) / "samples"


# ---------- helpers ----------

def _validate_run_id(run_id: str) -> None:
    if not run_id or not _RUN_ID_RE.match(run_id):
        raise ValidationError(
            "run_id must be 1-128 chars from letters, digits, '_', '.', '-'"
        )


def _save_upload(src, dest: Path, label: str) -> None:
    """Stream an UploadFile's spooled content to disk with a size cap."""
    written = 0
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as fh:
            while True:
                chunk = src.read(_SPOOL_CHUNK)
                if not chunk:
                    break
                written += len(chunk)
                if written > config.MAX_UPLOAD_BYTES:
                    raise ServiceError(
                        413,
                        f"uploaded {label} exceeds limit of {config.MAX_UPLOAD_BYTES} bytes",
                    )
                fh.write(chunk)
            fh.flush()
    except ServiceError:
        _unlink_quiet(dest)
        raise
    except OSError as exc:
        _unlink_quiet(dest)
        raise StorageError(f"failed storing uploaded {label}: {exc}") from exc
    if written == 0:
        _unlink_quiet(dest)
        raise ValidationError(f"uploaded {label} is empty")


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


def _persist_stats(
    conn: sqlite3.Connection,
    run_id: str,
    version: int,
    samples: list[tuple[str, str]],
    stats: DemuxStats,
) -> None:
    conn.execute(
        "UPDATE versions SET status='ready', error=NULL, total_pairs=?, "
        "assigned_pairs=?, unassigned_pairs=?, far_pairs=?, tie_pairs=? "
        "WHERE run_id=? AND version=?",
        (
            stats.total_pairs,
            stats.assigned_pairs,
            stats.unassigned_pairs,
            stats.far_pairs,
            stats.tie_pairs,
            run_id,
            version,
        ),
    )
    conn.executemany(
        "INSERT INTO samples(run_id, version, sample_id, barcode, reads) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (run_id, version, sample_id, barcode, stats.sample_reads.get(sample_id, 0))
            for sample_id, barcode in samples
        ],
    )


def _mark_failed(conn: sqlite3.Connection, run_id: str, version: int, message: str,
                 new_run: bool) -> None:
    conn.execute(
        "UPDATE versions SET status='failed', error=? WHERE run_id=? AND version=?",
        (message, run_id, version),
    )
    if new_run:
        conn.execute(
            "UPDATE runs SET status='failed', error=? WHERE run_id=?", (message, run_id)
        )


# ---------- create run ----------

def create_run(run_id: str, r1_upload, r2_upload, sheet_raw: bytes) -> int:
    _validate_run_id(run_id)
    samples = parse_sample_sheet(sheet_raw)

    odir = originals_dir(run_id)
    r1_path = odir / "R1.fastq"
    r2_path = odir / "R2.fastq"
    sheet_path = odir / "sample_sheet_v1.csv"

    lock = _run_lock(run_id)
    with lock:
        conn = database.connect()
        created = False
        try:
            with database.transaction(conn):
                existing = database.get_run(conn, run_id)
                if existing is not None:
                    # Nothing has been inserted: rollback is a no-op and the
                    # existing run must be left completely untouched.
                    raise ConflictError(f"run {run_id!r} already exists")
                conn.execute(
                    "INSERT INTO runs(run_id, current_ver, status) VALUES (?, 1, 'processing')",
                    (run_id,),
                )
                conn.execute(
                    "INSERT INTO versions(run_id, version, status) VALUES (?, 1, 'processing')",
                    (run_id,),
                )
            created = True
            # Stream the spooled uploads straight to disk (flat memory use);
            # a crash before completion is cleaned up by recover() on restart.
            _save_upload(r1_upload.file, r1_path, "R1 FASTQ")
            _save_upload(r2_upload.file, r2_path, "R2 FASTQ")
            try:
                sheet_path.write_bytes(sheet_raw)
            except OSError as exc:
                raise StorageError(f"failed storing sample sheet: {exc}") from exc

            stats = demultiplex(r1_path, r2_path, samples, version_dir(run_id, 1))
            with database.transaction(conn):
                _persist_stats(conn, run_id, 1, samples, stats)
                conn.execute(
                    "UPDATE runs SET current_ver=1, status='ready', error=NULL WHERE run_id=?",
                    (run_id,),
                )
            return 1
        except ServiceError:
            if created:
                _abort_run(conn, run_id, 1, new_run=True)
            raise
        except Exception as exc:  # defensive: never leave an active version dangling
            if created:
                _abort_run(conn, run_id, 1, new_run=True)
            raise StorageError(f"unexpected failure: {exc}") from exc
        finally:
            conn.close()


def _abort_run(conn: sqlite3.Connection, run_id: str, version: int, new_run: bool) -> None:
    try:
        with database.transaction(conn):
            _mark_failed(conn, run_id, version, _safe_detail(), new_run)
    except sqlite3.Error:
        pass
    # Remove half-written artifacts; originals of a failed first upload go too.
    import shutil

    for path in (version_dir(run_id, version), version_dir(run_id, version).with_name(
            f"v{version}.tmp")):
        shutil.rmtree(path, ignore_errors=True)
    if new_run:
        shutil.rmtree(run_dir(run_id), ignore_errors=True)


def _safe_detail() -> str:
    import sys

    exc = sys.exc_info()[1]
    if isinstance(exc, ServiceError):
        return exc.detail
    return str(exc) or exc.__class__.__name__


# ---------- correction ----------

def correct_sheet(run_id: str, base_version: int, sheet_raw: bytes) -> int:
    _validate_run_id(run_id)
    samples = parse_sample_sheet(sheet_raw)

    lock = _run_lock(run_id)
    with lock:
        conn = database.connect()
        new_version: int | None = None
        try:
            with database.transaction(conn):
                # Every check happens after acquiring the write lock, so the
                # decision cannot be invalidated by a concurrent committer.
                run = database.get_run(conn, run_id)
                if run is None:
                    raise NotFoundError(f"run {run_id!r} not found")
                base = database.get_version(conn, run_id, base_version)
                if base is None:
                    raise ConflictError(
                        f"stale correction: version {base_version} does not exist "
                        f"for run {run_id!r}"
                    )
                if base["status"] != "ready":
                    raise ConflictError(
                        f"stale correction: version {base_version} is not ready "
                        f"(status={base['status']})"
                    )
                if run["current_ver"] != base_version:
                    raise ConflictError(
                        f"stale correction: seen version {base_version} is no longer current "
                        f"(current={run['current_ver']})"
                    )
                # Cross-worker guard: runs.current_ver only changes after the
                # (potentially long) re-demultiplex finishes, so an in-flight
                # correction on another worker is detected via its processing
                # version row.  Failed rows are ignored: they belong to crashed
                # attempts and must not block new corrections.
                in_flight = conn.execute(
                    "SELECT COUNT(*) AS n FROM versions "
                    "WHERE run_id=? AND version > ? AND status IN ('processing', 'ready')",
                    (run_id, base_version),
                ).fetchone()["n"]
                if in_flight:
                    raise ConflictError(
                        f"stale correction: a correction newer than version {base_version} "
                        f"is already in progress for run {run_id!r}"
                    )
                new_version = (database.max_version(conn, run_id) or 0) + 1
                # Insert first, then re-check under the write lock.  In a
                # multi-worker deployment another process may have committed
                # a correction between our snapshot and BEGIN IMMEDIATE; the
                # rollback below discards this whole attempt (no failed row is
                # left behind), so exactly one same-version correction wins.
                conn.execute(
                    "INSERT INTO versions(run_id, version, status) VALUES (?, ?, 'processing')",
                    (run_id, new_version),
                )

            r1_path = originals_dir(run_id) / "R1.fastq"
            r2_path = originals_dir(run_id) / "R2.fastq"
            if not r1_path.exists() or not r2_path.exists():
                raise StorageError("original FASTQ files are missing; cannot re-demultiplex")
            try:
                (originals_dir(run_id) / f"sample_sheet_v{new_version}.csv").write_bytes(sheet_raw)
            except OSError as exc:
                raise StorageError(f"failed storing sample sheet: {exc}") from exc

            stats = demultiplex(r1_path, r2_path, samples, version_dir(run_id, new_version))
            with database.transaction(conn):
                _persist_stats(conn, run_id, new_version, samples, stats)
                conn.execute(
                    "UPDATE runs SET current_ver=?, status='ready', error=NULL WHERE run_id=?",
                    (new_version, run_id),
                )
            return new_version
        except ServiceError:
            _abort_correction(conn, run_id, new_version)
            raise
        except Exception as exc:
            _abort_correction(conn, run_id, new_version)
            raise StorageError(f"unexpected failure: {exc}") from exc
        finally:
            conn.close()


def _abort_correction(conn: sqlite3.Connection, run_id: str, version: int | None) -> None:
    if version is None:
        return
    try:
        with database.transaction(conn):
            conn.execute(
                "UPDATE versions SET status='failed', error=? WHERE run_id=? AND version=?",
                (_safe_detail(), run_id, version),
            )
    except sqlite3.Error:
        pass
    import shutil

    shutil.rmtree(version_dir(run_id, version), ignore_errors=True)
    shutil.rmtree(
        version_dir(run_id, version).with_name(f"v{version}.tmp"), ignore_errors=True
    )
    _unlink_quiet(originals_dir(run_id) / f"sample_sheet_v{version}.csv")


# ---------- reads ----------

def _require_ready_version(run_id: str, version: int | None) -> tuple[sqlite3.Row, int]:
    conn = database.connect()
    try:
        run = database.get_run(conn, run_id)
        if run is None:
            raise NotFoundError(f"run {run_id!r} not found")
        ver = run["current_ver"] if version is None else version
        row = database.get_version(conn, run_id, ver)
        if row is None:
            raise NotFoundError(f"version {ver} of run {run_id!r} not found")
        if row["status"] != "ready":
            raise ConflictError(
                f"version {ver} is not readable (status={row['status']}, error={row['error']!r})"
            )
        return row, ver
    finally:
        conn.close()


def get_run_view(run_id: str) -> dict:
    conn = database.connect()
    try:
        run = database.get_run(conn, run_id)
        if run is None:
            raise NotFoundError(f"run {run_id!r} not found")
        versions = [
            r["version"]
            for r in conn.execute(
                "SELECT version FROM versions WHERE run_id=? ORDER BY version", (run_id,)
            ).fetchall()
        ]
        return {
            "run_id": run_id,
            "status": run["status"],
            "current_version": run["current_ver"],
            "error": run["error"],
            "versions": versions,
        }
    finally:
        conn.close()


def list_runs() -> list[dict]:
    conn = database.connect()
    try:
        rows = conn.execute(
            "SELECT run_id, status, current_ver, error FROM runs ORDER BY run_id"
        ).fetchall()
        return [
            {
                "run_id": r["run_id"],
                "status": r["status"],
                "current_version": r["current_ver"],
                "error": r["error"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_version_view(run_id: str, version: int | None) -> dict:
    row, ver = _require_ready_version(run_id, version)
    conn = database.connect()
    try:
        samples = database.list_samples(conn, run_id, ver)
        return {
            "run_id": run_id,
            "version": ver,
            "status": row["status"],
            "error": row["error"],
            "total_pairs": row["total_pairs"],
            "assigned_pairs": row["assigned_pairs"],
            "unassigned_pairs": row["unassigned_pairs"],
            "far_pairs": row["far_pairs"],
            "tie_pairs": row["tie_pairs"],
            "samples": [dict(s) for s in samples],
        }
    finally:
        conn.close()


def _resolve_within(base: Path, name: str) -> Path:
    """Resolve ``name`` under ``base`` and reject any path escape."""
    base = base.resolve()
    target = (base / name).resolve()
    if base != target and base not in target.parents:
        raise ValidationError(f"invalid path component: {name!r}")
    return target


def sample_file_path(run_id: str, version: int | None, sample_id: str, mate: str) -> Path:
    if mate not in ("R1", "R2"):
        raise ValidationError("mate must be R1 or R2")
    _, ver = _require_ready_version(run_id, version)
    path = _resolve_within(samples_dir(run_id, ver), f"{sample_id}_{mate}.fastq")
    if not path.is_file():
        raise NotFoundError(
            f"sample {sample_id!r} not found in version {ver} of run {run_id!r}"
        )
    return path


def undetermined_file_path(run_id: str, version: int | None, mate: str) -> Path:
    if mate not in ("R1", "R2"):
        raise ValidationError("mate must be R1 or R2")
    _, ver = _require_ready_version(run_id, version)
    path = _resolve_within(version_dir(run_id, ver), f"undetermined_{mate}.fastq")
    if not path.is_file():
        raise NotFoundError(f"undetermined {mate} file missing for version {ver}")
    return path


# ---------- startup recovery ----------

def recover() -> None:
    """Resolve leftovers from crashed processes.

    A 'processing' row can never become ready without its atomic output rename,
    so on restart it is marked failed and its partial ``.tmp`` directory is
    discarded.  The run pointer is moved back to its most recent ready version
    when one exists; otherwise the run itself is failed (its first upload never
    completed).  Ready rows keep their renamed output and stay readable.
    """
    import shutil

    config.ensure_dirs()
    database.init_db()
    conn = database.connect()
    try:
        rows = conn.execute(
            "SELECT run_id, version FROM versions WHERE status='processing'"
        ).fetchall()
        for row in rows:
            run_id, version = row["run_id"], row["version"]
            with database.transaction(conn):
                conn.execute(
                    "UPDATE versions SET status='failed', error=? "
                    "WHERE run_id=? AND version=? AND status='processing'",
                    ("interrupted by service restart", run_id, version),
                )
                latest_ready = conn.execute(
                    "SELECT MAX(version) AS m FROM versions "
                    "WHERE run_id=? AND status='ready'",
                    (run_id,),
                ).fetchone()["m"]
                if latest_ready is None:
                    conn.execute(
                        "UPDATE runs SET status='failed', error=? WHERE run_id=?",
                        ("interrupted by service restart", run_id),
                    )
                else:
                    conn.execute(
                        "UPDATE runs SET current_ver=?, status='ready', error=NULL "
                        "WHERE run_id=?",
                        (latest_ready, run_id),
                    )
            shutil.rmtree(version_dir(run_id, version), ignore_errors=True)
            shutil.rmtree(
                version_dir(run_id, version).with_name(f"v{version}.tmp"),
                ignore_errors=True,
            )
            _unlink_quiet(originals_dir(run_id) / f"sample_sheet_v{version}.csv")

        # Remove any stray .tmp directories with no DB row.
        if config.RUNS_DIR.exists():
            for rdir in config.RUNS_DIR.iterdir():
                if not rdir.is_dir():
                    continue
                for child in rdir.iterdir():
                    if child.is_dir() and child.name.endswith(".tmp"):
                        shutil.rmtree(child, ignore_errors=True)
    finally:
        conn.close()
