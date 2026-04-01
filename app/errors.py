from fastapi import HTTPException


ERROR_CODE_DICT = {
    "BAD_REQUEST": {"type": "validation_error", "status": 400},
    "INVALID_DATE_FORMAT": {"type": "validation_error", "status": 400},
    "INVALID_ARGUMENT": {"type": "validation_error", "status": 400},
    "UNAUTHORIZED": {"type": "auth_error", "status": 401},
    "FORBIDDEN": {"type": "auth_error", "status": 403},
    "NOT_FOUND": {"type": "resource_error", "status": 404},
    "JOB_LOCK_CONFLICT": {"type": "conflict_error", "status": 409},
    "RATE_LIMITED": {"type": "throttle_error", "status": 429},
    "INTERNAL_ERROR": {"type": "internal_error", "status": 500},
}


def error_detail(code: str, message: str, trace_id: str = "") -> dict:
    item = ERROR_CODE_DICT.get(code, ERROR_CODE_DICT["INTERNAL_ERROR"])
    return {"code": code, "type": item["type"], "message": message, "trace_id": trace_id}


def raise_api_error(code: str, message: str, trace_id: str = ""):
    item = ERROR_CODE_DICT.get(code, ERROR_CODE_DICT["INTERNAL_ERROR"])
    raise HTTPException(status_code=item["status"], detail=error_detail(code=code, message=message, trace_id=trace_id))
