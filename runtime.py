"""Serve the existing broker with operational probes and metrics."""

import logging

from fastapi import HTTPException
from fastapi.responses import PlainTextResponse

from broker_core import store
from job_broker import BrokerDependency, app

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@app.get("/livez", include_in_schema=False)
def live():
    return {"status": "alive"}


@app.get("/readyz", include_in_schema=False)
def ready(broker: BrokerDependency):
    with store.transaction(broker.path) as db:
        state = db.execute("SELECT sim_time FROM broker_state WHERE id=1").fetchone()
    if state is None:
        raise HTTPException(503, "Broker state is unavailable")
    return {"status": "ready", "sim_time": state["sim_time"]}


@app.get("/metrics", include_in_schema=False)
def metrics(broker: BrokerDependency):
    with store.transaction(broker.path) as db:
        state = db.execute("SELECT sim_time, capacity_errors FROM broker_state WHERE id=1").fetchone()
        if state is None:
            raise HTTPException(503, "Broker state is unavailable")
        jobs = dict(db.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status"))
        nodes = dict(db.execute("SELECT status, COUNT(*) FROM nodes GROUP BY status"))
        oldest = db.execute("SELECT MIN(submitted_at) FROM jobs WHERE status='queued'").fetchone()[0]

    queued_age = max(0, state["sim_time"] - oldest) if oldest is not None else 0
    lines = [
        "# TYPE job_broker_sim_time_seconds gauge",
        f'job_broker_sim_time_seconds {state["sim_time"]}',
        "# TYPE job_broker_oldest_queued_seconds gauge",
        f"job_broker_oldest_queued_seconds {queued_age}",
        "# TYPE job_broker_capacity_errors_total counter",
        f'job_broker_capacity_errors_total {state["capacity_errors"]}',
    ]
    for name, counts, statuses in (
        ("jobs", jobs, ("queued", "running", "succeeded", "failed", "cancelled")),
        ("nodes", nodes, ("booting", "ready", "failed", "terminated")),
    ):
        lines.append(f"# TYPE job_broker_{name} gauge")
        for status in statuses:
            lines.append(f'job_broker_{name}{{state="{status}"}} {counts.get(status, 0)}')
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
