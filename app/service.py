"""Service layer: run lifecycle, versioned re-demultiplexing and recovery.

Liveness and ownership
----------------------
A ``processing`` row is *leased* by the worker that created it.  A background
heartbeat thread keeps ``versions.heartbeat_at`` fresh while the (potentially
long) demultiplexing runs.  Startup recovery -- or another worker -- may only
reap a processing row whose lease has gone stale: a heartbeat that is still
fresh proves another process is actively working on it, so its directories are
never touched.

Before a version is published the owning worker re-checks, inside a write
transaction, that it still owns the processing row.  A late executor whose row
was reaped while it worked cannot republish: it discards its own output and
returns a conflict.  Publishing additionally verifies that every expected
output file is on disk, so database counts and downloadable files always
describe the same complete version, and ``runs.current_ver`` only moves once
the whole version is durable.
"""
from __future__ import annotations

import os
import re
import shutil
import sqlite3
import threading
import time
from contextlib import contextmanager
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


# ---------- worker identity & leasing ----------

def _worker_id() -> str:
    return f"{os.uname().nodename}:{os.getpid()}:{threading.get_ident()}"


def _lease_expired(heartbeat_at: float | None, now: float | None = None) -> bool:
    """A row is stale when it has no heartbeat or its lease has timed out."""
    if heartbeat_at is None:
        return True
    return (time.time() if now is None else now) - heartbeat_at >= config.LEASE_TIMEOUT_SECONDS


class _Heartbeat:
    """Keep a processing row's lease fresh from a daemon thread."""

    def __init__(self, run_id: str, version: int, worker_id: str):
        self.run_id = run_id
        self.version = version
        self.worker_id = worker_id
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="demux-heartbeat", daemon=True)

    def start(self) -> None:
        self._beat()  # make the lease immediately visible to other workers
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(config.HEARTBEAT_SECONDS):
            self._beat()

    def _beat(self) -> None:
        try:
            conn = database.connect()
            try:
                conn.execute(
                    "UPDATE versions SET heartbeat_at=? "
                    "WHERE run_id=? AND version=? AND status='processing' AND worker_id=?",
                    (time.time(), self.run_id, self.version, self.worker_id),
                )
            finally:
                conn.close()
        except Exception:
            # A missed beat is tolerable: the lease only matters after
            # LEASE_TIMEOUT_SECONDS of silence, and the next beat retries.
            pass

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


@contextmanager
def _lease(run_id: str, version: int, worker_id: str):
    hb = _Heartbeat(run_id, version, worker_id)
    hb.start()
    try:
        yield
    finally:
        hb.stop()


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


def _rmtree_quiet(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def _remove_version_artifacts(run_id: str, version: int) -> None:
    """Delete every on-disk trace of one version (results, tmp, sheet copy)."""
    _rmtree_quiet(version_dir(run_id, version))
    _rmtree_quiet(version_dir(run_id, version).with_name(f"v{version}.tmp"))
    _unlink_quiet(originals_dir(run_id) / f"sample_sheet_v{version}.csv")


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


def _verify_outputs(run_id: str, version: int, samples: list[tuple[str, str]]) -> None:
    """Fail loudly unless the published directory is a complete version.

    The counts about to be persisted describe exactly these files, so every
    per-sample R1/R2 and both undetermined files must exist before
    ``current_ver`` is allowed to point at the version.
    """
    vdir = version_dir(run_id, version)
    expected: list[Path] = [vdir / "undetermined_R1.fastq", vdir / "undetermined_R2.fastq"]
    expected += [
        samples_dir(run_id, version) / f"{sample_id}_R1.fastq"
        for sample_id, _ in samples
    ]
    expected += [
        samples_dir(run_id, version) / f"{sample_id}_R2.fastq"
        for sample_id, _ in samples
    ]
    missing = [str(p) for p in expected if not p.is_file()]
    if missing or not vdir.is_dir():
        raise StorageError(
            f"version {version} output is incomplete on disk "
            f"({len(missing)} missing file(s)); refusing to publish it"
        )


# ---------- create run ----------

def create_run(run_id: str, r1_upload, r2_upload, sheet_raw: bytes) -> int:
    _validate_run_id(run_id)
    samples = parse_sample_sheet(sheet_raw)

    odir = originals_dir(run_id)
    r1_path = odir / "R1.fastq"
    r2_path = odir / "R2.fastq"
    sheet_path = odir / "sample_sheet_v1.csv"
    worker_id = _worker_id()

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
                    "INSERT INTO versions(run_id, version, status, worker_id, heartbeat_at) "
                    "VALUES (?, 1, 'processing', ?, ?)",
                    (run_id, worker_id, time.time()),
                )
            created = True
            # The heartbeat proves this v1 is live while uploads stream and the
            # first demultiplexing runs, so another worker's recovery leaves it
            # alone; a crash instead leaves a stale, reapable row.
            with _lease(run_id, 1, worker_id):
                # Stream the spooled uploads straight to disk (flat memory use).
                _save_upload(r1_upload.file, r1_path, "R1 FASTQ")
                _save_upload(r2_upload.file, r2_path, "R2 FASTQ")
                try:
                    sheet_path.write_bytes(sheet_raw)
                except OSError as exc:
                    raise StorageError(f"failed storing sample sheet: {exc}") from exc

                stats = demultiplex(r1_path, r2_path, samples, version_dir(run_id, 1))
            _publish_version(conn, run_id, 1, samples, stats, worker_id, new_run=True)
            return 1
        except ServiceError:
            if created:
                _abort_run(conn, run_id, 1, worker_id, new_run=True)
            raise
        except Exception as exc:  # defensive: never leave an active version dangling
            if created:
                _abort_run(conn, run_id, 1, worker_id, new_run=True)
            raise StorageError(f"unexpected failure: {exc}") from exc
        finally:
            conn.close()


def _abort_run(
    conn: sqlite3.Connection, run_id: str, version: int, worker_id: str, new_run: bool
) -> None:
    _claim_and_fail(conn, run_id, version, worker_id, _safe_detail(), mark_run=new_run)
    # Remove half-written artifacts; originals of a failed first upload go too.
    _remove_version_artifacts(run_id, version)
    if new_run:
        _rmtree_quiet(run_dir(run_id))


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
        worker_id = _worker_id()
        try:
            # Genuinely interrupted corrections (stale leases) are resolved
            # before anything else: they become failed rows and their residue
            # is removed.  Fresh leases belong to live workers and stay put.
            reaped = _reap_stale(conn, run_id)

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
                    "INSERT INTO versions(run_id, version, status, worker_id, heartbeat_at) "
                    "VALUES (?, ?, 'processing', ?, ?)",
                    (run_id, new_version, worker_id, time.time()),
                )

            # Cleanup of rows reaped above happens after their commit so a
            # failure here cannot roll back the failure markers.
            for rv in reaped:
                _remove_version_artifacts(run_id, rv)

            r1_path = originals_dir(run_id) / "R1.fastq"
            r2_path = originals_dir(run_id) / "R2.fastq"
            if not r1_path.exists() or not r2_path.exists():
                raise StorageError("original FASTQ files are missing; cannot re-demultiplex")
            try:
                (originals_dir(run_id) / f"sample_sheet_v{new_version}.csv").write_bytes(sheet_raw)
            except OSError as exc:
                raise StorageError(f"failed storing sample sheet: {exc}") from exc

            with _lease(run_id, new_version, worker_id):
                stats = demultiplex(
                    r1_path, r2_path, samples, version_dir(run_id, new_version)
                )
            _publish_version(conn, run_id, new_version, samples, stats, worker_id)
            return new_version
        except ServiceError:
            _abort_correction(conn, run_id, new_version, worker_id)
            raise
        except Exception as exc:
            _abort_correction(conn, run_id, new_version, worker_id)
            raise StorageError(f"unexpected failure: {exc}") from exc
        finally:
            conn.close()


def _abort_correction(
    conn: sqlite3.Connection, run_id: str, version: int | None, worker_id: str
) -> None:
    if version is None:
        return
    # Only the owner may flip its own row; if recovery already reaped it the
    # failure marker belongs to recovery and must not be overwritten.
    _claim_and_fail(conn, run_id, version, worker_id, _safe_detail(), mark_run=False)
    _remove_version_artifacts(run_id, version)


# ---------- shared ownership transitions ----------

def _claim_and_fail(
    conn: sqlite3.Connection,
    run_id: str,
    version: int,
    worker_id: str,
    message: str,
    mark_run: bool,
) -> bool:
    """Mark ``version`` failed, but only while this worker still owns it.

    Returns True when the row was claimed here, False when it had already been
    reaped by recovery/another worker.
    """
    try:
        with database.transaction(conn):
            cur = conn.execute(
                "UPDATE versions SET status='failed', error=? "
                "WHERE run_id=? AND version=? AND status='processing' AND worker_id=?",
                (message, run_id, version, worker_id),
            )
            if cur.rowcount == 0:
                return False
            if mark_run:
                conn.execute(
                    "UPDATE runs SET status='failed', error=? WHERE run_id=?",
                    (message, run_id),
                )
    except sqlite3.Error:
        return False
    return True


def _publish_version(
    conn: sqlite3.Connection,
    run_id: str,
    version: int,
    samples: list[tuple[str, str]],
    stats: DemuxStats,
    worker_id: str,
    new_run: bool = False,
) -> None:
    """Publish a finished version if and only if it is still ours and complete.

    The ownership re-check inside the write transaction closes the window in
    which a late executor could republish a version that recovery marked
    failed.  The on-disk completeness check guarantees the counts being
    committed describe files that actually exist, so ``current_ver`` only ever
    points at a fully readable version.
    """
    _verify_outputs(run_id, version, samples)
    with database.transaction(conn):
        cur = conn.execute(
            "UPDATE versions SET heartbeat_at=NULL "
            "WHERE run_id=? AND version=? AND status='processing' AND worker_id=?",
            (run_id, version, worker_id),
        )
        if cur.rowcount == 0:
            row = database.get_version(conn, run_id, version)
            status = row["status"] if row is not None else "missing"
            raise ConflictError(
                f"version {version} of run {run_id!r} is no longer owned by this worker "
                f"(status={status}); its results were discarded"
            )
        _persist_stats(conn, run_id, version, samples, stats)
        if new_run:
            conn.execute(
                "UPDATE runs SET current_ver=?, status='ready', error=NULL WHERE run_id=?",
                (version, run_id),
            )
        else:
            # current_ver moves only in the same transaction that makes the
            # complete version ready, so readers can never observe a pointer
            # to an unfinished or missing version.
            conn.execute(
                "UPDATE runs SET current_ver=?, status='ready', error=NULL "
                "WHERE run_id=? AND current_ver < ?",
                (version, run_id, version),
            )


def _reap_stale_locked(conn: sqlite3.Connection, run_id: str, message: str) -> list[int]:
    """Mark stale processing rows failed; caller must already hold a txn."""
    now = time.time()
    stale = [
        row
        for row in conn.execute(
            "SELECT version, heartbeat_at FROM versions "
            "WHERE run_id=? AND status='processing'",
            (run_id,),
        ).fetchall()
        if _lease_expired(row["heartbeat_at"], now)
    ]
    reaped: list[int] = []
    for row in stale:
        version = row["version"]
        conn.execute(
            "UPDATE versions SET status='failed', error=?, worker_id=NULL, heartbeat_at=NULL "
            "WHERE run_id=? AND version=? AND status='processing'",
            (message, run_id, version),
        )
        latest_ready = conn.execute(
            "SELECT MAX(version) AS m FROM versions WHERE run_id=? AND status='ready'",
            (run_id,),
        ).fetchone()["m"]
        if latest_ready is None:
            conn.execute(
                "UPDATE runs SET status='failed', error=? WHERE run_id=?",
                (message, run_id),
            )
        else:
            conn.execute(
                "UPDATE runs SET current_ver=?, status='ready', error=NULL WHERE run_id=?",
                (latest_ready, run_id),
            )
        reaped.append(version)
    return reaped


def _reap_stale(conn: sqlite3.Connection, run_id: str) -> list[int]:
    """Resolve interrupted corrections of one run outside the hot path."""
    with database.transaction(conn):
        return _reap_stale_locked(conn, run_id, "interrupted: processing lease expired")


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

_TMP_RE = re.compile(r"^v(\d+)\.tmp$")


def _tmp_is_live(conn: sqlite3.Connection, run_id: str, version: int, now: float) -> bool:
    """A .tmp directory is protected while its processing row holds a fresh lease."""
    row = database.get_version(conn, run_id, version)
    return (
        row is not None
        and row["status"] == "processing"
        and not _lease_expired(row["heartbeat_at"], now)
    )


def recover() -> None:
    """Resolve leftovers from crashed processes without touching live work.

    Processing rows whose lease is still fresh belong to live workers in this
    or another process: they are skipped completely.  Stale rows -- no
    heartbeat, or silence longer than the lease timeout -- can never become
    ready without their atomic output rename, so they are marked failed and
    their partial ``.tmp`` directory and sheet copy are discarded.  The run
    pointer moves back to its most recent ready version when one exists;
    otherwise the run itself is failed (its first upload never completed).
    Ready rows keep their renamed output and stay readable.
    """
    config.ensure_dirs()
    database.init_db()
    conn = database.connect()
    try:
        run_ids = [
            r["run_id"]
            for r in conn.execute("SELECT DISTINCT run_id FROM versions").fetchall()
        ]
        for run_id in run_ids:
            with database.transaction(conn):
                reaped = _reap_stale_locked(
                    conn, run_id, "interrupted by service restart"
                )
            for version in reaped:
                _remove_version_artifacts(run_id, version)

        # Remove stray .tmp directories with no live processing row.  A
        # directory protected by a fresh lease belongs to a correction that is
        # still running in another process and must survive this startup.
        now = time.time()
        if config.RUNS_DIR.exists():
            for rdir in config.RUNS_DIR.iterdir():
                if not rdir.is_dir():
                    continue
                run_id = rdir.name
                for child in rdir.iterdir():
                    if not (child.is_dir() and child.name.endswith(".tmp")):
                        continue
                    match = _TMP_RE.match(child.name)
                    if match is not None and _tmp_is_live(
                        conn, run_id, int(match.group(1)), now
                    ):
                        continue
                    _rmtree_quiet(child)
    finally:
        conn.close()
