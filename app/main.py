"""FastAPI application and HTTP routes.

POST handlers are plain ``def`` functions: FastAPI runs them in a worker
thread, so the blocking disk/database work never stalls the event loop.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from . import config, database, service
from .errors import ServiceError
from .schemas import CorrectionAck, RunOut, VersionOut


def _check_size(name: str, data: bytes) -> None:
    if len(data) > config.MAX_UPLOAD_BYTES:
        raise ServiceError(
            413,
            f"uploaded {name} ({len(data)} bytes) exceeds limit "
            f"{config.MAX_UPLOAD_BYTES} bytes",
        )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    database.init_db()
    service.recover()
    yield


app = FastAPI(
    title="FASTQ Demultiplexing Service",
    version="1.0.0",
    description="Upload mixed paired-end FASTQ runs and split them per sample sheet version.",
    lifespan=lifespan,
)


@app.exception_handler(ServiceError)
def _service_error_handler(_request, exc: ServiceError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.post("/runs", response_model=VersionOut, status_code=201)
def create_run(
    run_id: str = Form(...),
    r1: UploadFile = File(..., description="Uncompressed R1 FASTQ"),
    r2: UploadFile = File(..., description="Uncompressed R2 FASTQ"),
    sample_sheet: UploadFile = File(..., description="CSV: sample_id,barcode per row"),
):
    # Sample sheets are small; FASTQ bodies are streamed inside the service.
    sheet_raw = sample_sheet.file.read()
    _check_size("sample_sheet", sheet_raw)
    version = service.create_run(run_id, r1, r2, sheet_raw)
    return service.get_version_view(run_id, version)


@app.post("/runs/{run_id}/corrections", response_model=CorrectionAck, status_code=201)
def correct_sheet(
    run_id: str,
    base_version: int = Form(..., description="Version number the client has seen"),
    sample_sheet: UploadFile = File(...),
):
    sheet_raw = sample_sheet.file.read()
    _check_size("sample_sheet", sheet_raw)
    new_version = service.correct_sheet(run_id, base_version, sheet_raw)
    return CorrectionAck(
        run_id=run_id, version=new_version, status="processing_done", supersedes=base_version
    )


@app.get("/runs", response_model=list[RunOut])
def list_runs():
    return [RunOut(**service.get_run_view(r["run_id"])) for r in service.list_runs()]


@app.get("/runs/{run_id}", response_model=RunOut)
def get_run(run_id: str):
    return RunOut(**service.get_run_view(run_id))


@app.get("/runs/{run_id}/versions/{version}", response_model=VersionOut)
def get_version(run_id: str, version: int):
    return service.get_version_view(run_id, version)


@app.get("/runs/{run_id}/versions/{version}/samples/{sample_id}/{mate}")
def download_sample(run_id: str, version: int, sample_id: str, mate: str):
    path = service.sample_file_path(run_id, version, sample_id, mate)
    return FileResponse(
        path,
        media_type="text/plain",
        filename=f"{run_id}_v{version}_{sample_id}_{mate}.fastq",
    )


@app.get("/runs/{run_id}/versions/{version}/undetermined/{mate}")
def download_undetermined(run_id: str, version: int, mate: str):
    path = service.undetermined_file_path(run_id, version, mate)
    return FileResponse(
        path,
        media_type="text/plain",
        filename=f"{run_id}_v{version}_undetermined_{mate}.fastq",
    )


@app.get("/health")
def health():
    return {"status": "ok"}
