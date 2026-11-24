"""SQLite schema, transactions, and submission-only upgrades."""

import fcntl
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path

VERSION = 1
SCHEMA = """
CREATE TABLE IF NOT EXISTS broker_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    sim_time INTEGER NOT NULL,
    seed INTEGER NOT NULL,
    launch_retry_at INTEGER,
    backoff_attempt INTEGER NOT NULL DEFAULT 0,
    capacity_errors INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    gpus INTEGER NOT NULL,
    hourly_cents INTEGER NOT NULL,
    launch_job_id INTEGER NOT NULL REFERENCES jobs(id),
    status TEXT NOT NULL CHECK (status IN ('booting','ready','failed','terminated')),
    launched_at INTEGER NOT NULL,
    ready_at INTEGER,
    idle_since INTEGER,
    stopped_at INTEGER
);
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team TEXT NOT NULL,
    client_key TEXT NOT NULL,
    gpus INTEGER NOT NULL CHECK (gpus IN (1,2,4,8)),
    estimated_seconds INTEGER NOT NULL CHECK (estimated_seconds > 0),
    priority TEXT NOT NULL CHECK (priority IN ('low','normal','high')),
    status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
    node_id INTEGER REFERENCES nodes(id),
    submitted_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    UNIQUE (team, client_key)
);
CREATE TABLE IF NOT EXISTS sim_events (
    id INTEGER PRIMARY KEY,
    due_at INTEGER,
    job_id INTEGER UNIQUE REFERENCES jobs(id),
    node_id INTEGER UNIQUE REFERENCES nodes(id),
    outcome TEXT NOT NULL,
    runtime_seconds INTEGER,
    CHECK ((job_id IS NULL) != (node_id IS NULL))
);
CREATE INDEX IF NOT EXISTS due_events ON sim_events(due_at);
CREATE INDEX IF NOT EXISTS job_allocations ON jobs(node_id, status);
CREATE INDEX IF NOT EXISTS job_queue ON jobs(status, submitted_at, id);
CREATE INDEX IF NOT EXISTS live_nodes ON nodes(status);
"""


def connect(path):
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize(path, start, seed):
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect(path)) as db:
        db.execute("PRAGMA journal_mode = WAL")

    with transaction(path, write=True) as db:
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, VERSION):
            raise ValueError(f"Unsupported database schema version {version}")

        columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)")}
        legacy_jobs = version == 0 and bool(columns) and "node_id" not in columns
        if legacy_jobs:
            db.execute("ALTER TABLE jobs ADD COLUMN node_id INTEGER REFERENCES nodes(id)")
            db.execute("ALTER TABLE jobs ADD COLUMN started_at INTEGER")
            db.execute("ALTER TABLE jobs ADD COLUMN finished_at INTEGER")

        for statement in SCHEMA.strip().rstrip(";").split(";"):
            db.execute(statement)

        if legacy_jobs:
            db.execute(
                """INSERT INTO sim_events (job_id, outcome, runtime_seconds)
                   SELECT id, 'succeeded', estimated_seconds FROM jobs WHERE status = 'queued'"""
            )
        db.execute(
            "INSERT OR IGNORE INTO broker_state(id,sim_time,seed) VALUES (1,?,?)",
            (start, seed),
        )
        db.execute(f"PRAGMA user_version = {VERSION}")


@contextmanager
def transaction(path, write=False):
    with closing(connect(path)) as conn:
        conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


@contextmanager
def service_lock(path):
    lock_path = Path(str(path) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another broker or restore holds the database lock") from None
        yield
