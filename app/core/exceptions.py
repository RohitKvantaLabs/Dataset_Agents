import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger("neuro_platform")


class UpstreamServiceError(Exception):
    """Raised when a connector, LLM call, or link check fails after retries."""

    def __init__(self, service: str, detail: str):
        self.service = service
        self.detail = detail
        super().__init__(f"{service}: {detail}")


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(UpstreamServiceError)
    async def upstream_error_handler(request: Request, exc: UpstreamServiceError):
        logger.error("Upstream service failed: %s - %s", exc.service, exc.detail)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "upstream_service_error",
                "service": exc.service,
                "detail": exc.detail,
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception):
        logger.exception("Unhandled error on %s", request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "internal_server_error"},
        )
