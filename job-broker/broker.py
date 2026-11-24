"""Accept a job once and save it in the queue."""

import json
import sqlite3
from contextlib import asynccontextmanager, closing
from decimal import Decimal, InvalidOperation
from math import ceil
from pathlib import Path
from time import time
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse


DB_PATH = Path(__file__).with_name("submissions.sqlite")
MAX_BODY_BYTES = 16_384
MAX_ESTIMATE_MINUTES = 5_256_000


class IdempotencyConflict(Exception):
    """The key already belongs to a different submission for this team."""


def initialize_db(db_path):
    with closing(sqlite3.connect(db_path)) as db, db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY,
                team TEXT NOT NULL,
                client_key TEXT NOT NULL,
                gpus INTEGER NOT NULL CHECK (gpus IN (1, 2, 4, 8)),
                estimated_seconds INTEGER NOT NULL CHECK (estimated_seconds > 0),
                priority TEXT NOT NULL CHECK (priority IN ('low', 'normal', 'high')),
                status TEXT NOT NULL DEFAULT 'queued',
                submitted_at INTEGER NOT NULL,
                UNIQUE (team, client_key)
            )
            """
        )


def validate_job(body, client_key):
    if not isinstance(body, dict):
        raise ValueError("Expected a JSON object")

    team = body.get("team")
    gpus = body.get("gpus")
    priority = body.get("priority", "normal")

    if not isinstance(team, str) or not 1 <= len(team.strip()) <= 100:
        raise ValueError("team must contain 1 to 100 characters")
    if not isinstance(client_key, str) or not 1 <= len(client_key.strip()) <= 200:
        raise ValueError("Idempotency-Key must contain 1 to 200 characters")
    if type(gpus) is not int or gpus not in (1, 2, 4, 8):
        raise ValueError("gpus must be 1, 2, 4, or 8")
    if priority not in ("low", "normal", "high"):
        raise ValueError("priority must be low, normal, or high")

    try:
        minutes = Decimal(str(body.get("estimated_minutes")))
    except InvalidOperation:
        raise ValueError("estimated_minutes must be a number") from None
    if not minutes.is_finite() or not 0 < minutes <= MAX_ESTIMATE_MINUTES:
        raise ValueError("estimated_minutes must be positive and at most 5,256,000")

    return team.strip(), client_key.strip(), gpus, max(1, ceil(minutes * 60)), priority


def submit_job(db_path, team, client_key, gpus, estimated_seconds, priority):
    # Let SQLite enforce uniqueness even when two requests arrive together.
    with closing(sqlite3.connect(db_path)) as db, db:
        inserted = db.execute(
            """
            INSERT INTO jobs (team, client_key, gpus, estimated_seconds, priority, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (team, client_key) DO NOTHING
            """,
            (team, client_key, gpus, estimated_seconds, priority, int(time())),
        )
        if inserted.rowcount == 1:
            return inserted.lastrowid, True

        job_id, saved_gpus, saved_seconds, saved_priority = db.execute(
            "SELECT id, gpus, estimated_seconds, priority FROM jobs WHERE team = ? AND client_key = ?",
            (team, client_key),
        ).fetchone()
        if (saved_gpus, saved_seconds, saved_priority) != (gpus, estimated_seconds, priority):
            raise IdempotencyConflict("Use a new Idempotency-Key for different job details")
        return job_id, False


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


def create_app(db_path):
    @asynccontextmanager
    async def lifespan(app):
        initialize_db(db_path)
        yield

    app = FastAPI(title="Job Broker", lifespan=lifespan)

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
        client_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ):
        """Submit a job with a required client key scoped to the team."""
        try:
            fields = validate_job(await read_json(request), client_key)
            job_id, created = await run_in_threadpool(submit_job, db_path, *fields)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        except IdempotencyConflict as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        except sqlite3.OperationalError:
            return JSONResponse({"error": "Database unavailable. Retry with the same key."}, status_code=503)

        return JSONResponse({"id": job_id, "status": "queued"}, status_code=201 if created else 200)

    return app


app = create_app(DB_PATH)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8081)
