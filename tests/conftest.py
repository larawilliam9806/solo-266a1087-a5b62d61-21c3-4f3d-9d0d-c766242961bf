"""Shared pytest fixtures: isolated data dir + TestClient + FASTQ builders."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _qual(length: int) -> str:
    return "I" * length


def make_record(header: str, seq: str, qual: str | None = None) -> bytes:
    if qual is None:
        qual = _qual(len(seq))
    return f"@{header}\n{seq}\n+\n{qual}\n".encode()


def make_pair(name: str, tag: str, r1_rest: str = "AAAA", r2_seq: str = "CCCC") -> tuple[bytes, bytes]:
    r1 = make_record(f"{name}/1", tag + r1_rest)
    r2 = make_record(f"{name}/2", r2_seq)
    return r1, r2


def sheet_bytes(rows, header=True) -> bytes:
    lines = ["sample_id,barcode"] if header else []
    lines += [f"{sid},{bc}" for sid, bc in rows]
    return ("\n".join(lines) + "\n").encode()


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """Redirect every config path at an isolated per-test directory."""
    from app import config

    path = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_DIR", path, raising=True)
    monkeypatch.setattr(config, "DB_PATH", path / "demux.db", raising=True)
    monkeypatch.setattr(config, "RUNS_DIR", path / "runs", raising=True)
    config.ensure_dirs()
    return path


@pytest.fixture()
def client(data_dir):
    from fastapi.testclient import TestClient
    from app import database
    from app.main import app

    database.init_db()
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def builders():
    class B:
        record = staticmethod(make_record)
        pair = staticmethod(make_pair)

    return B


@pytest.fixture()
def sheet():
    return sheet_bytes
