"""Tests for the correction / startup-recovery interleaving.

These tests reproduce the incident where a second worker's startup recovery
mistook a live correction for an interrupted one, deleted its files and left a
"ready" version whose samples could not be downloaded.
"""
from __future__ import annotations

import io
import threading
import time

import pytest

from app import config, database, service
from app.errors import ConflictError, StorageError


def _upload(client, run_id, r1: bytes, r2: bytes, sheet: bytes):
    return client.post(
        "/runs",
        data={"run_id": run_id},
        files={
            "r1": ("R1.fastq", io.BytesIO(r1), "text/plain"),
            "r2": ("R2.fastq", io.BytesIO(r2), "text/plain"),
            "sample_sheet": ("sheet.csv", io.BytesIO(sheet), "text/csv"),
        },
    )


def _correct(client, run_id, base_version, sheet: bytes):
    return client.post(
        f"/runs/{run_id}/corrections",
        data={"base_version": base_version},
        files={"sample_sheet": ("sheet.csv", io.BytesIO(sheet), "text/csv")},
    )


def _insert_processing(run_id: str, version: int, heartbeat_at, worker_id: str = "w-other"):
    conn = database.connect()
    conn.execute(
        "INSERT INTO versions(run_id, version, status, worker_id, heartbeat_at) "
        "VALUES (?, ?, 'processing', ?, ?)",
        (run_id, version, worker_id, heartbeat_at),
    )
    conn.close()


@pytest.fixture()
def ready_run(client, builders, sheet):
    r1, r2 = builders.pair("x", "AAAAAAAA")
    assert _upload(client, "runX", r1, r2, sheet([("s1", "AAAAAAAA")])).status_code == 201
    return "runX"


# ---------- recovery vs a live correction ----------

def test_recover_skips_processing_rows_with_fresh_lease(client, ready_run, data_dir):
    # A live worker holds v2 with a fresh heartbeat and a populated tmp dir.
    now = time.time()
    _insert_processing("runX", 2, now)
    tmp = service.version_dir("runX", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)
    (tmp / "junk").write_text("partial output of a live correction")

    service.recover()

    conn = database.connect()
    row = conn.execute(
        "SELECT status, error, worker_id, heartbeat_at FROM versions "
        "WHERE run_id='runX' AND version=2"
    ).fetchone()
    conn.close()
    # Live work is neither failed nor cleaned up.
    assert row["status"] == "processing"
    assert row["error"] is None
    assert row["worker_id"] == "w-other"
    assert row["heartbeat_at"] == now
    assert tmp.exists() and (tmp / "junk").exists()
    assert client.get("/runs/runX").json()["current_version"] == 1


def test_recover_marks_stale_lease_failed_and_clears_residue(client, ready_run, data_dir):
    # Heartbeat older than the lease timeout -> the owning process is dead.
    stale = time.time() - config.LEASE_TIMEOUT_SECONDS - 5
    _insert_processing("runX", 2, stale)
    tmp = service.version_dir("runX", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)
    (tmp / "junk").write_text("partial")
    sheet_copy = service.originals_dir("runX") / "sample_sheet_v2.csv"
    sheet_copy.write_text("s1,AAAAAAAA\n")

    service.recover()

    detail = client.get("/runs/runX").json()
    assert detail["current_version"] == 1
    assert detail["status"] == "ready"
    v2 = client.get("/runs/runX/versions/2")
    assert v2.status_code == 409  # failed versions are not readable
    assert not tmp.exists()
    assert not service.version_dir("runX", 2).exists()
    assert not sheet_copy.exists()
    # The previous, complete version stays fully readable.
    assert client.get("/runs/runX/versions/1/samples/s1/R1").status_code == 200


def test_recover_treats_missing_heartbeat_as_stale(client, ready_run, data_dir):
    # Rows written without heartbeat (older writers / manual fixtures) are
    # considered dead and reaped, matching the pre-lease recovery semantics.
    conn = database.connect()
    conn.execute(
        "INSERT INTO versions(run_id, version, status) VALUES ('runX', 2, 'processing')"
    )
    conn.close()
    tmp = service.version_dir("runX", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)

    service.recover()

    conn = database.connect()
    status = conn.execute(
        "SELECT status FROM versions WHERE run_id='runX' AND version=2"
    ).fetchone()["status"]
    conn.close()
    assert status == "failed"
    assert not tmp.exists()


def test_stray_tmp_of_live_correction_survives_recovery(client, ready_run, data_dir):
    now = time.time()
    _insert_processing("runX", 2, now)
    tmp = service.version_dir("runX", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)
    (tmp / "partial").write_text("x")
    # An unrelated stray tmp with no matching row is still swept.
    other = service.version_dir("runX", 9).with_name("v9.tmp")
    other.mkdir(parents=True)

    service.recover()

    assert tmp.exists()
    assert not other.exists()


# ---------- the exact incident, end to end ----------

class _DemuxGate:
    """Block the demux engine right after its tmp directory is created."""

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()

    def hook(self, tmp_dir):
        self.entered.set()
        if not self.release.wait(timeout=10):  # pragma: no cover
            raise RuntimeError("test gate never released")


def test_live_correction_survives_other_workers_recovery(client, builders, sheet, ready_run):
    from app import demux

    gate = _DemuxGate()
    demux._ON_TMP_READY = gate.hook
    result: dict = {}
    new_sheet = sheet([("s1", "AAAAAAAT")])

    # Drive the service directly from a worker thread: TestClient's portal is
    # bound to the main thread, while service-level calls exercise exactly the
    # same cross-thread/cross-process locking and leasing primitives.
    def worker():
        try:
            version = service.correct_sheet("runX", 1, new_sheet)
            result["version"] = version
        except Exception as exc:  # pragma: no cover - debugging aid
            result["error"] = repr(exc)

    t = threading.Thread(target=worker)
    t.start()
    try:
        assert gate.entered.wait(timeout=10)
        # The correction's lease must be fresh while it is actively running.
        conn = database.connect()
        row = conn.execute(
            "SELECT status, worker_id, heartbeat_at FROM versions "
            "WHERE run_id='runX' AND version=2"
        ).fetchone()
        conn.close()
        assert row["status"] == "processing"
        assert row["worker_id"] is not None
        assert time.time() - row["heartbeat_at"] < config.LEASE_TIMEOUT_SECONDS
        tmp = service.version_dir("runX", 2).with_name("v2.tmp")
        assert tmp.exists()

        # Another worker starting up must not kill this in-flight correction.
        service.recover()
        conn = database.connect()
        row = conn.execute(
            "SELECT status FROM versions WHERE run_id='runX' AND version=2"
        ).fetchone()
        conn.close()
        assert row["status"] == "processing"
        assert tmp.exists()
    finally:
        gate.release.set()
        t.join(timeout=15)
        demux._ON_TMP_READY = None

    assert "error" not in result
    assert result["version"] == 2
    # The reported symptom: ready version + counts, but missing files, must be
    # impossible -- every download of the published version works.
    detail = client.get("/runs/runX").json()
    assert detail["current_version"] == 2 and detail["status"] == "ready"
    assert client.get("/runs/runX/versions/2").status_code == 200
    for url in (
        "/runs/runX/versions/2/samples/s1/R1",
        "/runs/runX/versions/2/samples/s1/R2",
        "/runs/runX/versions/2/undetermined/R1",
        "/runs/runX/versions/2/undetermined/R2",
    ):
        assert client.get(url).status_code == 200, url
    # And the old version remains readable.
    assert client.get("/runs/runX/versions/1/samples/s1/R1").status_code == 200


# ---------- a late executor must not republish a failed version ----------

def test_publish_refused_after_owner_lost_the_row(client, builders, sheet, ready_run, monkeypatch):
    # Another process reaps the processing row while the demultiplexing is
    # finishing, i.e. before this worker gets to publish.  The late executor
    # must notice it no longer owns the row instead of republishing it.
    real_demux = service.demultiplex

    def demux_then_get_reaped(r1_path, r2_path, samples, out_dir):
        stats = real_demux(r1_path, r2_path, samples, out_dir)
        conn = database.connect()
        try:
            conn.execute(
                "UPDATE versions SET status='failed', error='reaped by another worker', "
                "worker_id=NULL, heartbeat_at=NULL "
                "WHERE run_id='runX' AND version=2 AND status='processing'"
            )
        finally:
            conn.close()
        return stats

    monkeypatch.setattr(service, "demultiplex", demux_then_get_reaped)
    with pytest.raises(ConflictError) as exc:
        service.correct_sheet("runX", 1, sheet([("s1", "AAAAAAAT")]))
    assert "no longer owned" in exc.value.detail

    # The row stays failed, current_ver stays on the old version, and the late
    # worker's orphaned output directory was removed rather than published.
    assert client.get("/runs/runX").json()["current_version"] == 1
    v2 = client.get("/runs/runX/versions/2")
    assert v2.status_code == 409
    assert "reaped by another worker" in v2.json()["detail"]
    assert not service.version_dir("runX", 2).exists()
    assert not service.version_dir("runX", 2).with_name("v2.tmp").exists()


def test_late_workers_abort_does_not_overwrite_recovery_marker(
    client, builders, sheet, ready_run, monkeypatch
):
    # Recovery fails the row while demux is still running; the worker then
    # hits its own error path.  Recovery's marker must be preserved.
    real_demux = service.demultiplex

    def demux_then_reap_and_fail(r1_path, r2_path, samples, out_dir):
        real_demux(r1_path, r2_path, samples, out_dir)
        conn = database.connect()
        try:
            conn.execute(
                "UPDATE versions SET status='failed', error='recovery marker', "
                "worker_id=NULL, heartbeat_at=NULL "
                "WHERE run_id='runX' AND version=2"
            )
        finally:
            conn.close()
        # Simulate a subsequent engine-level failure in the late worker.
        raise StorageError("late worker disk error")

    monkeypatch.setattr(service, "demultiplex", demux_then_reap_and_fail)
    with pytest.raises(StorageError, match="late worker disk error"):
        service.correct_sheet("runX", 1, sheet([("s1", "AAAAAAAT")]))

    conn = database.connect()
    row = conn.execute(
        "SELECT status, error FROM versions WHERE run_id='runX' AND version=2"
    ).fetchone()
    conn.close()
    assert row["status"] == "failed"
    assert row["error"] == "recovery marker"  # not overwritten by the late worker
    assert client.get("/runs/runX").json()["current_version"] == 1


def test_incomplete_outputs_never_become_current(client, builders, sheet, ready_run, monkeypatch):
    real_demux = service.demultiplex

    def demux_then_delete(r1_path, r2_path, samples, out_dir):
        stats = real_demux(r1_path, r2_path, samples, out_dir)
        # Simulate the file system losing one sample file before publish.
        (out_dir / "samples" / "s1_R1.fastq").unlink()
        return stats

    monkeypatch.setattr(service, "demultiplex", demux_then_delete)
    with pytest.raises(StorageError) as exc:
        service.correct_sheet("runX", 1, sheet([("s1", "AAAAAAAT")]))
    assert "incomplete" in exc.value.detail

    # current_ver must not move to a version whose files do not all exist.
    detail = client.get("/runs/runX").json()
    assert detail["current_version"] == 1 and detail["status"] == "ready"
    assert client.get("/runs/runX/versions/2").status_code == 409
    assert client.get("/runs/runX/versions/1").status_code == 200


# ---------- stale corrections are reaped on the next request ----------

def test_correction_reaps_stale_predecessor_then_succeeds(client, builders, sheet, ready_run):
    stale = time.time() - config.LEASE_TIMEOUT_SECONDS - 5
    _insert_processing("runX", 2, stale)
    tmp = service.version_dir("runX", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)
    (tmp / "junk").write_text("partial")

    resp = _correct(client, "runX", 1, sheet([("s1", "AAAAAAAC")]))
    assert resp.status_code == 201, resp.text
    assert resp.json()["version"] == 3

    conn = database.connect()
    rows = {
        r["version"]: r["status"]
        for r in conn.execute(
            "SELECT version, status FROM versions WHERE run_id='runX'"
        ).fetchall()
    }
    conn.close()
    assert rows == {1: "ready", 2: "failed", 3: "ready"}
    assert not tmp.exists()
    assert client.get("/runs/runX").json()["current_version"] == 3
    assert client.get("/runs/runX/versions/3/samples/s1/R1").status_code == 200


def test_fresh_lease_blocks_correction_and_is_not_reaped(client, builders, sheet, ready_run):
    _insert_processing("runX", 2, time.time())
    tmp = service.version_dir("runX", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)

    resp = _correct(client, "runX", 1, sheet([("s1", "AAAAAAAC")]))
    assert resp.status_code == 409
    conn = database.connect()
    row = conn.execute(
        "SELECT status FROM versions WHERE run_id='runX' AND version=2"
    ).fetchone()
    conn.close()
    assert row["status"] == "processing"
    assert tmp.exists()
    assert client.get("/runs/runX").json()["current_version"] == 1


def test_crashed_higher_version_does_not_permanently_block(client, builders, sheet, ready_run):
    # A stale, higher-version processing row would trip the in-flight guard if
    # recovery semantics were wrong; it must be reaped first, then v3 proceeds.
    stale = time.time() - config.LEASE_TIMEOUT_SECONDS - 10
    _insert_processing("runX", 5, stale)
    tmp = service.version_dir("runX", 5).with_name("v5.tmp")
    tmp.mkdir(parents=True)
    (tmp / "junk").write_text("dead worker output")

    resp = _correct(client, "runX", 1, sheet([("s1", "AAAAAAAC")]))
    assert resp.status_code == 201, resp.text
    assert resp.json()["version"] == 6

    conn = database.connect()
    rows = {
        r["version"]: r["status"]
        for r in conn.execute(
            "SELECT version, status FROM versions WHERE run_id='runX'"
        ).fetchall()
    }
    conn.close()
    assert rows == {1: "ready", 5: "failed", 6: "ready"}
    assert not tmp.exists()
    assert (service.version_dir("runX", 6) / "samples" / "s1_R1.fastq").is_file()
    assert client.get("/runs/runX/versions/6/samples/s1/R1").status_code == 200


# ---------- same-version concurrency still has a single winner ----------

def test_concurrent_corrections_one_winner_and_loser_leaves_no_failed_row(
    client, builders, sheet, ready_run
):
    results = []

    def worker(tag):
        results.append(_correct(client, "runX", 1, sheet([("s1", tag)])))

    t1 = threading.Thread(target=worker, args=("AAAAAAAT",))
    t2 = threading.Thread(target=worker, args=("AAAAAAAC",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert sorted(r.status_code for r in results) == [201, 409]
    detail = client.get("/runs/runX").json()
    assert detail["current_version"] == 2
    assert detail["versions"] == [1, 2]  # loser rolls back fully, no failed row
    assert client.get("/runs/runX/versions/2/samples/s1/R1").status_code == 200


# ---------- initial uploads are leased too ----------

def test_live_first_upload_is_not_recovered(client, builders, sheet, data_dir):
    # Simulate an in-flight v1 upload with a fresh lease and partial files.
    conn = database.connect()
    conn.execute(
        "INSERT INTO runs(run_id, current_ver, status) VALUES ('runV', 1, 'processing')"
    )
    conn.execute(
        "INSERT INTO versions(run_id, version, status, worker_id, heartbeat_at) "
        "VALUES ('runV', 1, 'processing', 'w-other', ?)",
        (time.time(),),
    )
    conn.close()
    rdir = service.run_dir("runV")
    rdir.mkdir(parents=True)
    tmp = rdir / "v1.tmp"
    tmp.mkdir()

    service.recover()

    conn = database.connect()
    run = conn.execute("SELECT status FROM runs WHERE run_id='runV'").fetchone()
    ver = conn.execute(
        "SELECT status FROM versions WHERE run_id='runV' AND version=1"
    ).fetchone()
    conn.close()
    assert run["status"] == "processing"
    assert ver["status"] == "processing"
    assert tmp.exists()


def test_stale_first_upload_fails_the_run_and_clears_everything(client, data_dir):
    stale = time.time() - config.LEASE_TIMEOUT_SECONDS - 5
    conn = database.connect()
    conn.execute(
        "INSERT INTO runs(run_id, current_ver, status) VALUES ('runW', 1, 'processing')"
    )
    conn.execute(
        "INSERT INTO versions(run_id, version, status, worker_id, heartbeat_at) "
        "VALUES ('runW', 1, 'processing', 'dead', ?)",
        (stale,),
    )
    conn.close()
    rdir = service.run_dir("runW")
    rdir.mkdir(parents=True)
    (rdir / "v1.tmp").mkdir()
    (rdir / "originals").mkdir()
    (rdir / "originals" / "R1.fastq").write_text("partial")

    service.recover()

    detail = client.get("/runs/runW").json()
    assert detail["status"] == "failed"
    assert client.get("/runs/runW/versions/1").status_code == 409
    assert not (rdir / "v1.tmp").exists()
