"""Private immediate source reads for the owner's authenticated TFB Site."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from core.platform_source_auth import SOURCE_READ_PATH, platform_source_authorized
from integrations.platform_source_read import (
    MAX_BODY_BYTES, MAX_READ_SECONDS, MAX_RESPONSE_BYTES, SourceReadError,
    decode_request, read_source,
)

router = APIRouter()
_HEADERS = {"Cache-Control": "no-store, private", "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff"}


def _error(code: str, status: int) -> JSONResponse:
    headers = dict(_HEADERS)
    if status == 429:
        headers["Retry-After"] = "2"
    return JSONResponse({"status": "error", "error": code}, status_code=status, headers=headers)


@router.post(SOURCE_READ_PATH, include_in_schema=False)
async def platform_source_read(request: Request) -> JSONResponse:
    if not platform_source_authorized(request):
        return _error("unauthorized", 401)
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        return _error("source_request_invalid", 415)
    try:
        raw = bytearray()
        async for chunk in request.stream():
            if len(raw) + len(chunk) > MAX_BODY_BYTES:
                raise SourceReadError("source_request_too_large", 413)
            raw.extend(chunk)
        body = decode_request(bytes(raw))
        data = await asyncio.wait_for(asyncio.to_thread(read_source, body), timeout=MAX_READ_SECONDS + 5)
        response = JSONResponse({"status": "success", "data": data}, headers=_HEADERS)
        if len(response.body) > MAX_RESPONSE_BYTES:
            raise SourceReadError("source_response_too_large")
        return response
    except SourceReadError as error:
        return _error(error.code, error.status)
    except asyncio.TimeoutError:
        return _error("source_read_timeout", 504)
    except Exception:
        return _error("source_unavailable", 502)
