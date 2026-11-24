"""Stream SQLite snapshots out of or into the broker's data volume."""

import argparse
import os
import shutil
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from broker_core import store

TABLES = {"broker_state", "jobs", "nodes", "sim_events"}


def read_only(path):
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)


def validate_snapshot(db):
    version = db.execute("PRAGMA user_version").fetchone()[0]
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if version != store.VERSION or not TABLES.issubset(tables):
        raise ValueError("Unexpected broker database schema")
    if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise ValueError("SQLite integrity check failed")
    if db.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("SQLite foreign-key check failed")
    if db.execute("SELECT sim_time FROM broker_state WHERE id=1").fetchone() is None:
        raise ValueError("Snapshot is missing the broker state")


def backup(database):
    with TemporaryDirectory(prefix="broker-backup-") as temp:
        snapshot = Path(temp) / "snapshot.sqlite"
        with closing(read_only(database)) as source, closing(sqlite3.connect(snapshot)) as copy:
            source.backup(copy)
            copy.execute("PRAGMA journal_mode = DELETE")
            validate_snapshot(copy)
        with snapshot.open("rb") as stream:
            shutil.copyfileobj(stream, sys.stdout.buffer)
        sys.stdout.buffer.flush()


def restore(database, replace):
    with store.service_lock(database):
        if database.exists() and not replace:
            raise ValueError("Database already exists. Use --replace to restore over it")
        with TemporaryDirectory(prefix="broker-restore-") as temp:
            snapshot = Path(temp) / "snapshot.sqlite"
            with snapshot.open("wb") as stream:
                shutil.copyfileobj(sys.stdin.buffer, stream)
            with closing(read_only(snapshot)) as source:
                validate_snapshot(source)
                with closing(store.connect(database)) as target:
                    source.backup(target)
        print(f"Restored {database}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=os.environ.get("BROKER_DB", "/data/broker.sqlite"))
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("backup", help="Write a consistent snapshot to stdout")
    restore_parser = actions.add_parser("restore", help="Restore a snapshot from stdin with the broker stopped")
    restore_parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    database = args.db.expanduser().resolve()
    try:
        if args.action == "backup":
            backup(database)
        else:
            restore(database, args.replace)
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
