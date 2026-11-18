"""Pydantic response models."""
from __future__ import annotations

from pydantic import BaseModel


class SampleOut(BaseModel):
    sample_id: str
    barcode: str
    reads: int


class VersionOut(BaseModel):
    run_id: str
    version: int
    status: str
    error: str | None = None
    total_pairs: int
    assigned_pairs: int
    unassigned_pairs: int
    far_pairs: int
    tie_pairs: int
    samples: list[SampleOut]


class RunOut(BaseModel):
    run_id: str
    status: str
    current_version: int
    error: str | None = None
    versions: list[int]


class CorrectionAck(BaseModel):
    run_id: str
    version: int
    status: str
    supersedes: int
