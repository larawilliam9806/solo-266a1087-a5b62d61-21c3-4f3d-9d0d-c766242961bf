"""FASTQ validation and demultiplexing engine.

The engine streams the original (uncompressed) paired FASTQ files, validates
the four-line structure and /1-/2 pairing, strips the 8-base tag from R1 and
routes every pair to a sample or the undetermined files.

All output is written into a ``.tmp`` directory and atomically renamed on
success, so a crash can never expose a half-written version.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from . import config
from .errors import StorageError, ValidationError


@dataclass
class DemuxStats:
    total_pairs: int = 0
    assigned_pairs: int = 0
    unassigned_pairs: int = 0
    far_pairs: int = 0
    tie_pairs: int = 0
    sample_reads: dict[str, int] = field(default_factory=dict)


# ---------- FASTQ parsing ----------

def _read_record(fh, name: str, pair_index: int) -> tuple[str, str, str, str] | None:
    """Read one four-line FASTQ record; return None at clean EOF."""
    lines: list[str] = []
    for slot in range(4):
        line = fh.readline()
        if line == "":
            if slot == 0 and not lines:
                return None
            raise ValidationError(
                f"{name}: truncated FASTQ record at pair {pair_index} "
                f"(expected 4 lines, got {slot})"
            )
        lines.append(line.rstrip("\r\n"))
    header, seq, plus, qual = lines
    if not header.startswith("@"):
        raise ValidationError(
            f"{name}: pair {pair_index} header must start with '@', got {header[:40]!r}"
        )
    if not plus.startswith("+"):
        raise ValidationError(
            f"{name}: pair {pair_index} third line must start with '+', got {plus[:40]!r}"
        )
    if not seq or not qual:
        raise ValidationError(f"{name}: pair {pair_index} has empty sequence or quality line")
    if any(ch.isspace() for ch in seq) or any(ch.isspace() for ch in qual):
        raise ValidationError(f"{name}: pair {pair_index} sequence/quality contains whitespace")
    if len(seq) != len(qual):
        raise ValidationError(
            f"{name}: pair {pair_index} sequence length {len(seq)} != quality length {len(qual)}"
        )
    return header, seq, plus, qual


def _iter_records(path: Path, name: str):
    try:
        fh = path.open("r", encoding="utf-8", newline="")
    except OSError as exc:
        raise StorageError(f"cannot open {name} {path}: {exc}") from exc
    pair_index = 0
    with fh:
        while True:
            pair_index += 1
            record = _read_record(fh, name, pair_index)
            if record is None:
                return
            yield pair_index, record


# ---------- barcode matching ----------

def hamming_distance(a: str, b: str) -> int:
    return sum(1 for x, y in zip(a, b) if x != y)


def _build_classifier(barcodes: list[str]):
    """Return a callable mapping an observed tag to (barcode index, reason).

    reason is None for a unique nearest sample within distance 1, otherwise
    ``"far"`` or ``"tie"``.  Results for repeated tags are memoised.
    """
    cache: dict[str, tuple[int | None, str | None]] = {}

    def classify(tag: str) -> tuple[int | None, str | None]:
        if tag in cache:
            return cache[tag]
        distances = [(hamming_distance(tag, bc), i) for i, bc in enumerate(barcodes)]
        within = [(d, i) for d, i in distances if d <= config.BARCODE_LEN and d <= 1]
        if not within:
            result: tuple[int | None, str | None] = (None, "far")
        else:
            best = min(d for d, _ in within)
            nearest = [i for d, i in within if d == best]
            result = (nearest[0], None) if len(nearest) == 1 else (None, "tie")
        cache[tag] = result
        return result

    return classify


def _write_record(fh, header: str, seq: str, plus: str, qual: str) -> None:
    try:
        fh.write(f"{header}\n{seq}\n{plus}\n{qual}\n")
    except OSError as exc:
        raise StorageError(f"failed writing output: {exc}") from exc


# ---------- orchestration ----------

def demultiplex(
    r1_path: Path,
    r2_path: Path,
    samples: list[tuple[str, str]],
    out_dir: Path,
) -> DemuxStats:
    """Demultiplex one paired run into ``out_dir``.

    ``out_dir`` must not exist yet; it is created as ``out_dir.tmp`` and
    renamed into place only after every file is flushed and fsynced.
    """
    sample_ids = [s for s, _ in samples]
    barcodes = [b for _, b in samples]
    stats = DemuxStats(sample_reads={s: 0 for s in sample_ids})
    classify = _build_classifier(barcodes)

    tmp_dir = out_dir.with_name(out_dir.name + ".tmp")
    if tmp_dir.exists():
        # Leftover from a previous crashed attempt; never mix old/new output.
        _safe_rmtree(tmp_dir)
    samples_dir = tmp_dir / "samples"
    try:
        samples_dir.mkdir(parents=True)
        r1_handles: dict[str, object] = {}
        r2_handles: dict[str, object] = {}
        for sample_id in sample_ids:
            r1_handles[sample_id] = (samples_dir / f"{sample_id}_R1.fastq").open("w", encoding="utf-8", newline="")
            r2_handles[sample_id] = (samples_dir / f"{sample_id}_R2.fastq").open("w", encoding="utf-8", newline="")
        und_r1 = (tmp_dir / "undetermined_R1.fastq").open("w", encoding="utf-8", newline="")
        und_r2 = (tmp_dir / "undetermined_R2.fastq").open("w", encoding="utf-8", newline="")
    except OSError as exc:
        _safe_rmtree(tmp_dir)
        raise StorageError(f"cannot prepare output directory {tmp_dir}: {exc}") from exc

    try:
        # Test/injection hook: fires once the output tmp directory exists but
        # before any record is processed.  Kept inside the try so a raising
        # hook still triggers the normal tmp cleanup.
        on_tmp_ready = globals().get("_ON_TMP_READY")
        if on_tmp_ready is not None:
            try:
                on_tmp_ready(tmp_dir)
            except Exception:
                _close_quietly(r1_handles, r2_handles, und_r1, und_r2)
                _safe_rmtree(tmp_dir)
                raise
        r1_iter = _iter_records(r1_path, "R1")
        r2_iter = _iter_records(r2_path, "R2")
        while True:
            try:
                r1_item = next(r1_iter)
            except StopIteration:
                r1_item = None
            try:
                r2_item = next(r2_iter)
            except StopIteration:
                r2_item = None
            if r1_item is None and r2_item is None:
                break
            if r1_item is None or r2_item is None:
                side = "R1" if r1_item is None else "R2"
                raise ValidationError(
                    f"R1/R2 record count mismatch: {side} ended before its mate "
                    f"at pair {stats.total_pairs + 1}"
                )
            pair_no, (h1, s1, p1, q1) = r1_item
            _, (h2, s2, p2, q2) = r2_item

            if not h1.endswith("/1") or not h2.endswith("/2"):
                raise ValidationError(
                    f"pair {pair_no}: read names must end with /1 (R1) and /2 (R2); "
                    f"got {h1[:60]!r} / {h2[:60]!r}"
                )
            if h1[:-2] != h2[:-2]:
                raise ValidationError(
                    f"pair {pair_no}: read names do not pair: "
                    f"{h1[:60]!r} vs {h2[:60]!r}"
                )
            if len(s1) < config.BARCODE_LEN:
                raise ValidationError(
                    f"pair {pair_no}: R1 sequence shorter than {config.BARCODE_LEN} "
                    f"tag bases (length {len(s1)})"
                )

            stats.total_pairs += 1
            tag = s1[: config.BARCODE_LEN]
            stripped_s1 = s1[config.BARCODE_LEN :]
            stripped_q1 = q1[config.BARCODE_LEN :]

            idx, reason = classify(tag)
            if reason == "far":
                stats.far_pairs += 1
                stats.unassigned_pairs += 1
                _write_record(und_r1, h1, stripped_s1, p1, stripped_q1)
                _write_record(und_r2, h2, s2, p2, q2)
            elif reason == "tie":
                stats.tie_pairs += 1
                stats.unassigned_pairs += 1
                _write_record(und_r1, h1, stripped_s1, p1, stripped_q1)
                _write_record(und_r2, h2, s2, p2, q2)
            else:
                sample_id = sample_ids[idx]  # type: ignore[index]
                stats.assigned_pairs += 1
                stats.sample_reads[sample_id] += 1
                _write_record(r1_handles[sample_id], h1, stripped_s1, p1, stripped_q1)
                _write_record(r2_handles[sample_id], h2, s2, p2, q2)

        all_handles = list(r1_handles.values()) + list(r2_handles.values()) + [und_r1, und_r2]
        for fh in all_handles:
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()

        os.rename(tmp_dir, out_dir)
        _fsync_dir(out_dir.parent)
    except ValidationError:
        _close_quietly(r1_handles, r2_handles, und_r1, und_r2)
        _safe_rmtree(tmp_dir)
        raise
    except StorageError:
        _close_quietly(r1_handles, r2_handles, und_r1, und_r2)
        _safe_rmtree(tmp_dir)
        raise
    except OSError as exc:
        _close_quietly(r1_handles, r2_handles, und_r1, und_r2)
        _safe_rmtree(tmp_dir)
        raise StorageError(f"disk failure while demultiplexing: {exc}") from exc
    finally:
        for fh in list(r1_handles.values()) + list(r2_handles.values()) + [und_r1, und_r2]:
            if not fh.closed:
                fh.close()

    return stats


def _close_quietly(r1_handles, r2_handles, und_r1, und_r2) -> None:
    for fh in list(r1_handles.values()) + list(r2_handles.values()) + [und_r1, und_r2]:
        try:
            if not fh.closed:
                fh.close()
        except OSError:
            pass


def _safe_rmtree(path: Path) -> None:
    import shutil

    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except OSError:
        pass


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
