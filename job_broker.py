"""Accept jobs and run the GPU broker simulator over FastAPI."""

import json
import os
import sqlite3
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Literal

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi import Path as PathParameter
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


if __package__:
    from .broker_core import Broker, Clock, RequestError, service_lock
    from .broker_core.submission import validate_job
else:
    from broker_core import Broker, Clock, RequestError, service_lock
    from broker_core.submission import validate_job


DB_PATH = Path(__file__).with_name("submissions.sqlite")
MAX_BODY_BYTES = 16_384


SQLITE_MAX_ID = 2**63 - 1
JobId = Annotated[int, PathParameter(ge=1, le=SQLITE_MAX_ID)]
Cursor = Annotated[int, Query(ge=0, le=SQLITE_MAX_ID)]
PageSize = Annotated[int, Query(ge=1, le=500)]


class Completion(BaseModel):
    outcome: Literal["succeeded", "failed"]


class AdvanceTime(BaseModel):
    seconds: Annotated[int, Field(strict=True, ge=1, le=86400)]


async def read_json(request):
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise ValueError("Use Content-Type: application/json")

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_BODY_BYTES:
            raise HTTPException(413, "Request body must contain at most 16,384 bytes")
        body.extend(chunk)

    try:
        return json.loads(body, parse_float=Decimal)
    except (ValueError, InvalidOperation, RecursionError):
        raise ValueError("Request body must be valid JSON") from None


def get_broker(request: Request) -> Broker:
    return request.app.state.broker


BrokerDependency = Annotated[Broker, Depends(get_broker)]


def public_job(job):
    return {key: value for key, value in job.items() if key != "client_key"}


def page(items, limit):
    return {"items": items, "next_after": items[-1]["id"] if len(items) == limit else None}


def create_app(db_path, *, idle_seconds=300, clock_speed=60):
    db_path = Path(db_path).expanduser().resolve()
    if idle_seconds < 0 or not 0 <= clock_speed <= 3600:
        raise ValueError("Idle seconds must be nonnegative and clock speed must be between 0 and 3600")

    @asynccontextmanager
    async def lifespan(app):
        with service_lock(db_path):
            broker = Broker(db_path, idle_seconds=idle_seconds)
            broker.advance(seconds=0)
            app.state.broker = broker
            clock = Clock(broker, clock_speed)
            clock.start()
            try:
                yield
            finally:
                clock.close()

    app = FastAPI(title="Job Broker", lifespan=lifespan)

    @app.exception_handler(RequestError)
    async def broker_error(_request, exc):
        return JSONResponse({"error": str(exc)}, status_code=exc.status)

    @app.exception_handler(sqlite3.OperationalError)
    async def database_error(_request, _exc):
        return JSONResponse({"error": "Database unavailable. Retry shortly."}, status_code=503)

    @app.post(
        "/jobs",
        status_code=201,
        responses={200: {"description": "Existing job returned for a matching retry"}},
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {
                    "schema": {"type": "object"},
                    "example": {
                        "team": "vision", "gpus": 2, "estimated_minutes": 15, "priority": "normal",
                    },
                }},
            },
        },
    )
    async def post_job(
        request: Request,
        broker: BrokerDependency,
        client_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        """Submit a job with a required client key scoped to the team."""
        try:
            fields = validate_job(await read_json(request), client_key)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)

        job, created = await run_in_threadpool(broker.submit, fields)
        return JSONResponse({"id": job["id"], "status": job["status"]}, status_code=201 if created else 200)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: JobId, broker: BrokerDependency):
        """Get a job and its current lifecycle state."""
        return public_job(broker.job(job_id))

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: JobId, broker: BrokerDependency):
        """Cancel a queued or running job and release any reserved GPUs."""
        return public_job(broker.finish(job_id, "cancelled"))

    @app.post("/jobs/{job_id}/complete")
    def complete_job(job_id: JobId, result: Completion, broker: BrokerDependency):
        """Report success or failure for a running job."""
        return public_job(broker.finish(job_id, result.outcome))

    @app.get("/queue")
    def list_queue(broker: BrokerDependency, after: Cursor = 0, limit: PageSize = 100):
        """List queued jobs, including jobs reserved on booting nodes."""
        return page([public_job(job) for job in broker.queue(after, limit)], limit)

    @app.get("/nodes")
    def list_nodes(
        broker: BrokerDependency, after: Cursor = 0, limit: PageSize = 100, include_stopped: bool = False,
    ):
        """Inspect node states and GPU allocations. Include stopped nodes to see reaping."""
        return page(broker.nodes(after, limit, include_stopped), limit)

    if clock_speed == 0:
        @app.post("/sim/advance")
        def advance_clock(advance: AdvanceTime, broker: BrokerDependency):
            """Advance the simulated clock and process provider, job, and reaper events."""
            return {"sim_time": broker.advance(seconds=advance.seconds)}

    @app.get("/costs")
    def cost_report(
        broker: BrokerDependency,
        start: Annotated[str, Query(alias="from", description="ISO 8601 time or Unix seconds")],
        end: Annotated[str, Query(alias="to", description="ISO 8601 time or Unix seconds")],
    ):
        """Report GPU-hours and US dollar charges per team for a start-inclusive, end-exclusive window."""
        return broker.cost_report(start, end)

    return app


app = create_app(
    os.environ.get("BROKER_DB", DB_PATH),
    idle_seconds=int(os.environ.get("BROKER_IDLE_SECONDS", "300")),
    clock_speed=int(os.environ.get("BROKER_CLOCK_SPEED", "60")),
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8081)
