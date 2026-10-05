"""服务层可观察错误。"""


class ServiceError(RuntimeError):
    code = "service_error"
    status = 400


class NotFound(ServiceError):
    code = "not_found"
    status = 404


class Conflict(ServiceError):
    code = "conflict"
    status = 409


class Forbidden(ServiceError):
    code = "forbidden"
    status = 403


class InvalidState(ServiceError):
    code = "invalid_state"
    status = 409


class ValidationFailed(ServiceError):
    """携带字段级拒绝原因的契约失败。"""

    code = "validation_failed"
    status = 422

    def __init__(self, message: str, rejections: list[dict] | None = None) -> None:
        super().__init__(message)
        self.rejections = rejections or []
