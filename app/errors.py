"""Application-level errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class ServiceError(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class ValidationError(ServiceError):
    def __init__(self, detail: str):
        super().__init__(422, detail)


class NotFoundError(ServiceError):
    def __init__(self, detail: str):
        super().__init__(404, detail)


class ConflictError(ServiceError):
    def __init__(self, detail: str):
        super().__init__(409, detail)


class StorageError(ServiceError):
    def __init__(self, detail: str):
        super().__init__(507, detail)
