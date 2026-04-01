class TradingDataSDKError(Exception):
    def __init__(self, message: str, code: str = "", error_type: str = "", status_code: int = 0, trace_id: str = ""):
        super().__init__(message)
        self.message = message
        self.code = code
        self.error_type = error_type
        self.status_code = status_code
        self.trace_id = trace_id


class InvalidDateFormatError(TradingDataSDKError):
    pass


class InvalidArgumentError(TradingDataSDKError):
    pass


class UnauthorizedError(TradingDataSDKError):
    pass


class ForbiddenError(TradingDataSDKError):
    pass


class NotFoundError(TradingDataSDKError):
    pass


class JobLockConflictError(TradingDataSDKError):
    pass


class RateLimitedError(TradingDataSDKError):
    pass


class InternalServerError(TradingDataSDKError):
    pass


class TransportError(TradingDataSDKError):
    pass


ERROR_CLASS_BY_CODE = {
    "INVALID_DATE_FORMAT": InvalidDateFormatError,
    "INVALID_ARGUMENT": InvalidArgumentError,
    "UNAUTHORIZED": UnauthorizedError,
    "FORBIDDEN": ForbiddenError,
    "NOT_FOUND": NotFoundError,
    "JOB_LOCK_CONFLICT": JobLockConflictError,
    "RATE_LIMITED": RateLimitedError,
    "INTERNAL_ERROR": InternalServerError,
}
