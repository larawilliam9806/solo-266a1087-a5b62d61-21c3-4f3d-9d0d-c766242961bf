"""Sample-sheet parsing and validation."""
from __future__ import annotations

import csv
import io
import re

from . import config
from .errors import ValidationError

_SAMPLE_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_BARCODE_RE = re.compile(r"^[ACGT]{8}$")


def parse_sample_sheet(raw: bytes) -> list[tuple[str, str]]:
    """Parse a CSV sample sheet.

    Accepted layout: optional header line (``sample_id,barcode``) followed by
    one ``sample_id,barcode`` pair per line.  Whitespace around fields is
    trimmed and blank lines are ignored.
    """
    if not raw or not raw.strip():
        raise ValidationError("sample sheet is empty")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"sample sheet is not valid UTF-8: {exc}") from exc

    rows: list[tuple[str, str]] = []
    reader = csv.reader(io.StringIO(text))
    for line_no, row in enumerate(reader, start=1):
        if not row or all(not c.strip() for c in row):
            continue
        if len(row) != 2:
            raise ValidationError(
                f"sample sheet line {line_no}: expected 2 columns (sample_id,barcode), "
                f"got {len(row)}"
            )
        sample_id, barcode = (c.strip() for c in row)
        # Tolerate a header row instead of rejecting it.
        if line_no == 1 and barcode.lower() in {"barcode", "index", "tag"}:
            continue
        if not sample_id or not barcode:
            raise ValidationError(f"sample sheet line {line_no}: empty sample_id or barcode")
        rows.append((sample_id, barcode.upper()))

    if not rows:
        raise ValidationError("sample sheet contains no sample rows")

    seen_samples: set[str] = set()
    seen_barcodes: set[str] = set()
    for sample_id, barcode in rows:
        if not _SAMPLE_RE.match(sample_id):
            raise ValidationError(
                f"invalid sample_id {sample_id!r}: only letters, digits, '_', '.', '-' allowed"
            )
        if sample_id in {".", ".."}:
            raise ValidationError(f"invalid sample_id {sample_id!r}: reserved name not allowed")
        if sample_id in seen_samples:
            raise ValidationError(f"duplicate sample_id in sample sheet: {sample_id!r}")
        seen_samples.add(sample_id)
        if not _BARCODE_RE.match(barcode):
            raise ValidationError(
                f"invalid barcode {barcode!r} for sample {sample_id!r}: "
                f"must be exactly {config.BARCODE_LEN} characters from A/C/G/T"
            )
        if barcode in seen_barcodes:
            raise ValidationError(f"duplicate barcode in sample sheet: {barcode!r}")
        seen_barcodes.add(barcode)
    return rows
