"""End-to-end tests for the demultiplexing API."""
from __future__ import annotations

import io

import pytest


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


SHEET = None  # populated via fixture below


def test_full_demux_flow_counts_and_outputs(client, builders, sheet):
    # s1/s2 differ at exactly one position, which makes a distance-1 tie possible.
    rows = [("s1", "AAAAAAAA"), ("s2", "AAAAAAAC")]
    r1 = b"".join([
        builders.pair("r1", "AAAAAAAA")[0],
        builders.pair("r2", "AAAAAAAC")[0],
        # one mismatch from AAAAAAAA -> unique nearest s1
        builders.pair("r3", "AAAAAAAT")[0],
        # far: closest sample is 2 mismatches away
        builders.pair("r4", "AAAAAAGT")[0],
        # tie: distance 1 from both s1 (AAAAAAAA) and s2 (AAAAAAAC)
        builders.pair("r5", "AAAAAAAG")[0],
    ])
    r2 = b"".join([builders.pair(n, "AAAAAAAA")[1] for n in
                   ("r1", "r2", "r3", "r4", "r5")])

    resp = _upload(client, "runA", r1, r2, sheet(rows))
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["version"] == 1
    assert body["total_pairs"] == 5
    assert body["assigned_pairs"] == 2
    assert body["unassigned_pairs"] == 3
    assert body["far_pairs"] == 1
    assert body["tie_pairs"] == 2
    reads = {s["sample_id"]: s["reads"] for s in body["samples"]}
    assert reads == {"s1": 1, "s2": 1}

    # R1 tag stripped (12 -> 4 bases), R2 untouched.
    s1_r1 = client.get("/runs/runA/versions/1/samples/s1/R1").text
    s1_r2 = client.get("/runs/runA/versions/1/samples/s1/R2").text
    assert s1_r1.count("@") == 1
    for line in s1_r1.splitlines()[1::4]:
        assert len(line) == 4 and "A" in line
    for line in s1_r2.splitlines()[1::4]:
        assert len(line) == 4

    und1 = client.get("/runs/runA/versions/1/undetermined/R1").text
    assert und1.count("@") == 3
    # undetermined R1 also has its tag stripped
    assert all(len(line) == 4 for line in und1.splitlines()[1::4])


def test_run_metadata_and_versions_listed(client, builders, sheet):
    rows = [("s1", "AAAAAAAA")]
    r1, r2 = builders.pair("x", "AAAAAAAA")
    assert _upload(client, "runB", r1, r2, sheet(rows)).status_code == 201

    runs = client.get("/runs").json()
    assert any(r["run_id"] == "runB" and r["current_version"] == 1 for r in runs)

    detail = client.get("/runs/runB").json()
    assert detail["versions"] == [1]
    assert detail["status"] == "ready"


def test_correction_redemuxes_from_originals_and_keeps_old_version(client, builders, sheet):
    rows_v1 = [("s1", "AAAAAAAA"), ("s2", "CCCCCCCC")]
    r1 = b"".join([
        builders.pair("r1", "AAAAAAAA")[0],
        builders.pair("r2", "CCCCCCCC")[0],
    ])
    r2 = b"".join([builders.pair(n, "AAAAAAAA")[1] for n in ("r1", "r2")])
    assert _upload(client, "runC", r1, r2, sheet(rows_v1)).status_code == 201

    # Corrected sheet: same tag AAAAAAAA now belongs to sample alpha.
    rows_v2 = [("alpha", "AAAAAAAA"), ("beta", "GGGGGGGG")]
    resp = _correct(client, "runC", 1, sheet(rows_v2))
    assert resp.status_code == 201, resp.text
    assert resp.json()["version"] == 2

    detail = client.get("/runs/runC").json()
    assert detail["current_version"] == 2
    assert detail["versions"] == [1, 2]

    v1 = client.get("/runs/runC/versions/1").json()
    v2 = client.get("/runs/runC/versions/2").json()
    assert {s["sample_id"] for s in v1["samples"]} == {"s1", "s2"}
    assert {s["sample_id"] for s in v2["samples"]} == {"alpha", "beta"}
    assert v2["assigned_pairs"] == 1
    assert v2["far_pairs"] == 1

    # Old version's files remain readable; new sample name resolves too.
    assert client.get("/runs/runC/versions/1/samples/s1/R1").status_code == 200
    assert client.get("/runs/runC/versions/2/samples/alpha/R1").status_code == 200
    assert client.get("/runs/runC/versions/2/samples/s1/R1").status_code == 404


def test_stale_correction_rejected(client, builders, sheet):
    rows = [("s1", "AAAAAAAA")]
    r1, r2 = builders.pair("x", "AAAAAAAA")
    _upload(client, "runD", r1, r2, sheet(rows))
    _correct(client, "runD", 1, sheet([("s1", "AAAAAAAT")]))

    # A second correction carrying the now-stale version 1 must fail.
    resp = _correct(client, "runD", 1, sheet([("s1", "AAAAAAAC")]))
    assert resp.status_code == 409
    assert "stale" in resp.json()["detail"]

    # Current version untouched.
    assert client.get("/runs/runD").json()["current_version"] == 2


def test_concurrent_same_version_corrections_only_one_wins(client, builders, sheet):
    import threading

    rows = [("s1", "AAAAAAAA")]
    r1, r2 = builders.pair("x", "AAAAAAAA")
    _upload(client, "runE", r1, r2, sheet(rows))

    results = []
    errors = []

    def worker(tag):
        try:
            results.append(_correct(client, "runE", 1, sheet([("s1", tag)])))
        except Exception as exc:  # pragma: no cover - debugging aid
            errors.append(exc)

    t1 = threading.Thread(target=worker, args=("AAAAAAAT",))
    t2 = threading.Thread(target=worker, args=("AAAAAAAC",))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors
    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409]
    detail = client.get("/runs/runE").json()
    assert detail["current_version"] == 2
    assert detail["versions"] == [1, 2]


def test_invalid_sample_sheet_rules(client, builders, sheet):
    r1, r2 = builders.pair("x", "AAAAAAAA")
    # duplicate sample id
    resp = _upload(client, "runF", r1, r2, sheet([("s1", "AAAAAAAA"), ("s1", "CCCCCCCC")]))
    assert resp.status_code == 422 and "duplicate sample_id" in resp.json()["detail"]
    # duplicate barcode
    resp = _upload(client, "runF", r1, r2, sheet([("s1", "AAAAAAAA"), ("s2", "AAAAAAAA")]))
    assert resp.status_code == 422 and "duplicate barcode" in resp.json()["detail"]
    # bad barcode alphabet/length
    resp = _upload(client, "runF", r1, r2, sheet([("s1", "AAAAAAAN")]))
    assert resp.status_code == 422
    resp = _upload(client, "runF", r1, r2, sheet([("s1", "AAAAAAA")]))
    assert resp.status_code == 422
    # nothing was created
    assert client.get("/runs/runF").status_code == 404


def test_fastq_structure_errors_leave_no_current_version(client, builders, sheet, data_dir):
    good_r1, good_r2 = builders.pair("x", "AAAAAAAA")
    # sequence/quality length mismatch
    bad_r1 = builders.record("x/1", "AAAAAAAAAAAA", qual="IIII")
    resp = _upload(client, "runG", bad_r1, good_r2, sheet([("s1", "AAAAAAAA")]))
    assert resp.status_code == 422
    assert "quality length" in resp.json()["detail"]
    # run marked failed, not exposed as ready; no output directory
    detail = client.get("/runs/runG").json()
    assert detail["status"] == "failed"
    run_path = data_dir / "runs" / "runG"
    assert not (run_path / "v1").exists()
    assert not (run_path / "v1.tmp").exists()


def test_read_name_pairing_validation(client, builders, sheet):
    r1 = builders.record("x/1", "AAAAAAAA")
    r2 = builders.record("y/2", "CCCC")
    resp = _upload(client, "runH", r1, r2, sheet([("s1", "AAAAAAAA")]))
    assert resp.status_code == 422 and "do not pair" in resp.json()["detail"]

    r1 = builders.record("x", "AAAAAAAA")
    r2 = builders.record("x/2", "CCCC")
    resp = _upload(client, "runH2", r1, r2, sheet([("s1", "AAAAAAAA")]))
    assert resp.status_code == 422 and "/1" in resp.json()["detail"]


def test_record_count_mismatch(client, builders, sheet):
    r1 = builders.pair("x", "AAAAAAAA")[0] + builders.pair("y", "AAAAAAAA")[0]
    r2 = builders.pair("x", "AAAAAAAA")[1]
    resp = _upload(client, "runI", r1, r2, sheet([("s1", "AAAAAAAA")]))
    assert resp.status_code == 422 and "count mismatch" in resp.json()["detail"]


def test_duplicate_run_id_conflicts(client, builders, sheet):
    r1, r2 = builders.pair("x", "AAAAAAAA")
    assert _upload(client, "runJ", r1, r2, sheet([("s1", "AAAAAAAA")])).status_code == 201
    resp = _upload(client, "runJ", r1, r2, sheet([("s1", "AAAAAAAA")]))
    assert resp.status_code == 409
    # The existing run and its outputs must survive the rejected re-upload.
    detail = client.get("/runs/runJ").json()
    assert detail["status"] == "ready" and detail["current_version"] == 1
    assert client.get("/runs/runJ/versions/1/samples/s1/R1").status_code == 200


def test_correction_failure_does_not_change_current_version(client, builders, sheet):
    rows = [("s1", "AAAAAAAA")]
    r1, r2 = builders.pair("x", "AAAAAAAA")
    _upload(client, "runK", r1, r2, sheet(rows))

    # Invalid sheet in correction -> v2 fails, current version stays 1 and
    # remains fully readable.
    resp = _correct(client, "runK", 1, sheet([("s1", "BAD-BAD!")]))
    assert resp.status_code == 422
    detail = client.get("/runs/runK").json()
    assert detail["current_version"] == 1
    assert detail["status"] == "ready"
    assert client.get("/runs/runK/versions/1").status_code == 200


def test_empty_sample_outputs_are_visible(client, builders, sheet):
    r1 = builders.pair("x", "CCCCCCCC")[0]
    r2 = builders.pair("x", "CCCCCCCC")[1]
    _upload(client, "runL", r1, r2, sheet([("s1", "AAAAAAAA"), ("s2", "CCCCCCCC")]))
    body = client.get("/runs/runL/versions/1").json()
    reads = {s["sample_id"]: s["reads"] for s in body["samples"]}
    assert reads == {"s1": 0, "s2": 1}
    empty = client.get("/runs/runL/versions/1/samples/s1/R1")
    assert empty.status_code == 200
    assert empty.text == ""


def test_restart_recovers_half_finished_version(client, builders, sheet, data_dir):
    from app import service

    rows = [("s1", "AAAAAAAA")]
    r1, r2 = builders.pair("x", "AAAAAAAA")
    _upload(client, "runM", r1, r2, sheet(rows))

    # Simulate a crash mid-correction: processing row + leftover tmp dir.
    conn = service.database.connect()
    conn.execute(
        "INSERT INTO versions(run_id, version, status) VALUES ('runM', 2, 'processing')"
    )
    conn.close()
    tmp = service.version_dir("runM", 2).with_name("v2.tmp")
    tmp.mkdir(parents=True)
    (tmp / "junk").write_text("partial")

    service.recover()

    detail = client.get("/runs/runM").json()
    assert detail["current_version"] == 1
    assert detail["status"] == "ready"
    v2 = client.get("/runs/runM/versions/2")
    assert v2.status_code == 409  # failed versions are not exposed as results
    assert not tmp.exists()
    # v1 outputs still readable
    assert client.get("/runs/runM/versions/1/samples/s1/R1").status_code == 200


def test_restart_after_failed_first_upload_exposes_nothing_ready(client, data_dir):
    from app import service

    # Simulate a crash during the initial v1 upload.
    conn = service.database.connect()
    conn.execute("INSERT INTO runs(run_id, current_ver, status) VALUES ('runP', 1, 'processing')")
    conn.execute(
        "INSERT INTO versions(run_id, version, status) VALUES ('runP', 1, 'processing')"
    )
    conn.close()
    tmp = service.version_dir("runP", 1).with_name("v1.tmp")
    tmp.mkdir(parents=True)
    (tmp / "partial").write_text("x")

    service.recover()

    detail = client.get("/runs/runP").json()
    assert detail["status"] == "failed"
    assert detail["current_version"] == 1
    assert client.get("/runs/runP/versions/1").status_code == 409
    assert not tmp.exists()


def test_in_flight_processing_version_blocks_correction(client, builders, sheet):
    import time

    from app import service

    r1, r2 = builders.pair("x", "AAAAAAAA")
    _upload(client, "runS", r1, r2, sheet([("s1", "AAAAAAAA")]))

    # Simulate another worker that has already inserted an in-flight v2 whose
    # lease is still fresh: it must block a competing correction.
    conn = service.database.connect()
    conn.execute(
        "INSERT INTO versions(run_id, version, status, worker_id, heartbeat_at) "
        "VALUES ('runS', 2, 'processing', 'other-worker', ?)",
        (time.time(),),
    )
    conn.close()

    resp = _correct(client, "runS", 1, sheet([("s1", "AAAAAAAT")]))
    assert resp.status_code == 409
    assert "in progress" in resp.json()["detail"]
    # Current version and the live processing row are untouched.
    assert client.get("/runs/runS").json()["current_version"] == 1
    conn = service.database.connect()
    row = conn.execute(
        "SELECT status, worker_id FROM versions WHERE run_id='runS' AND version=2"
    ).fetchone()
    conn.close()
    assert row["status"] == "processing" and row["worker_id"] == "other-worker"


def test_failed_version_does_not_block_fresh_correction(client, builders, sheet):
    from app import service

    r1, r2 = builders.pair("x", "AAAAAAAA")
    _upload(client, "runT", r1, r2, sheet([("s1", "AAAAAAAA")]))

    # A crashed (failed) v2 must not block retrying a correction.
    conn = service.database.connect()
    conn.execute(
        "INSERT INTO versions(run_id, version, status, error) "
        "VALUES ('runT', 2, 'failed', 'interrupted by service restart')"
    )
    conn.close()

    resp = _correct(client, "runT", 1, sheet([("s1", "AAAAAAAT")]))
    assert resp.status_code == 201, resp.text
    assert resp.json()["version"] == 3
    detail = client.get("/runs/runT").json()
    assert detail["current_version"] == 3
    assert detail["versions"] == [1, 2, 3]
    assert client.get("/runs/runT/versions/3").status_code == 200


def test_disk_write_failure_on_upload_marks_run_failed(client, builders, sheet, data_dir):
    from app import service

    r1, r2 = builders.pair("x", "AAAAAAAA")

    def _boom(*_a, **_k):
        raise OSError("simulated disk failure")

    orig = service.demultiplex
    service.demultiplex = _boom
    try:
        resp = _upload(client, "runN", r1, r2, sheet([("s1", "AAAAAAAA")]))
    finally:
        service.demultiplex = orig
    assert resp.status_code == 507
    detail = client.get("/runs/runN").json()
    assert detail["status"] == "failed"
    # no half output exposed
    assert not (data_dir / "runs" / "runN" / "v1").exists()


def test_upload_size_limit_enforced(client, builders, sheet, monkeypatch, data_dir):
    from app import config

    monkeypatch.setattr(config, "MAX_UPLOAD_BYTES", 30)
    r1, r2 = builders.pair("x", "AAAAAAAA")
    # R1 record above is larger than 30 bytes: streaming aborts mid-upload.
    resp = _upload(client, "runQ", r1, r2, sheet([("s1", "AAAAAAAA")]))
    assert resp.status_code == 413
    # The run is recorded as failed but no result version is exposed.
    detail = client.get("/runs/runQ").json()
    assert detail["status"] == "failed"
    assert not (data_dir / "runs" / "runQ" / "v1").exists()


def test_sample_id_path_escape_rejected(client, builders, sheet, data_dir):
    rows = [("..", "AAAAAAAA")]
    r1, r2 = builders.pair("x", "AAAAAAAA")
    # The sheet validator itself rejects '..'-style ids; regardless, the file
    # resolver must refuse traversal characters.
    resp = _upload(client, "runR", r1, r2, sheet(rows))
    assert resp.status_code == 422

    # Defence in depth: resolver rejects escaping names even if one slipped in.
    from app import service
    rows2 = [("s1", "AAAAAAAA")]
    _upload(client, "runR2", r1, r2, sheet(rows2))
    with pytest.raises(service.ValidationError):
        service.sample_file_path("runR2", 1, "../escape", "R1")
