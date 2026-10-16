"""Stable, transport-neutral runtime failures."""


class CMMSRuntimeError(RuntimeError):
    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


class RuntimeNotReadyError(CMMSRuntimeError):
    def __init__(self):
        super().__init__("E_RUNTIME_NOT_READY")


class RuntimeClosingError(CMMSRuntimeError):
    def __init__(self):
        super().__init__("E_RUNTIME_CLOSING")


class RuntimeFailedError(CMMSRuntimeError):
    def __init__(self):
        super().__init__("E_RUNTIME_FAILED")


class RuntimeShutdownTimeoutError(CMMSRuntimeError):
    def __init__(self):
        super().__init__("E_RUNTIME_SHUTDOWN_TIMEOUT")


class RuntimeCleanupFailedError(CMMSRuntimeError):
    def __init__(self):
        super().__init__("E_RUNTIME_CLEANUP_FAILED")


class RuntimeFlushFailedError(CMMSRuntimeError):
    def __init__(self):
        super().__init__("E_RUNTIME_FLUSH_FAILED")


# Compatibility names for the provisional neutral API.
RuntimeShutdownTimeout = RuntimeShutdownTimeoutError
RuntimeCleanupFailed = RuntimeCleanupFailedError
RuntimeFlushFailed = RuntimeFlushFailedError


class StorageOwnershipError(CMMSRuntimeError):
    def __init__(self, cause: Exception) -> None:
        code = getattr(cause, "code", "E_LOCK_RELEASE_UNCERTAIN")
        self.cause = cause
        super().__init__(code)
        self.__cause__ = cause
