"""Rebuild team GPU usage and charges from the persisted job and node history."""

from collections import Counter, defaultdict
from datetime import datetime, timezone
from fractions import Fraction
from re import fullmatch


MINIMUM_BILLED_SECONDS = 600
MICRODOLLARS_PER_DOLLAR = 1_000_000


def parse_timestamp(value):
    """Use whole Unix seconds for report windows. Interpret an omitted time zone as UTC."""
    text = str(value).strip()
    try:
        if fullmatch(r"-?[0-9]{1,19}", text):
            seconds = int(text)
            if not -(2**63) <= seconds < 2**63:
                raise ValueError()
            return seconds
        if not fullmatch(r"\d{4}-\d\d-\d\d[T ]\d\d:\d\d(?::\d\d)?(?:Z|[+-]\d\d:\d\d)?", text):
            raise ValueError()
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return int(stamp.timestamp())
    except (ValueError, OverflowError, OSError):
        raise ValueError("Use ISO 8601 date and time at second precision or Unix seconds") from None


def charge_by_team(charges, amount, weights):
    total = sum(weights.values())
    for team, weight in weights.items():
        charges[team] += amount * weight / total


def report(conn, start, end, now):
    """Reconstruct node charges from the history and split before rounding."""
    gpu_seconds = Counter()
    charges = defaultdict(Fraction)
    nodes = conn.execute(
        """SELECT nodes.*, jobs.team AS sponsor FROM nodes
           JOIN jobs ON jobs.id = nodes.launch_job_id
           WHERE launched_at < ? AND (stopped_at IS NULL OR stopped_at >= ?)""",
        (min(end, now), start),
    )
    for node in nodes:
        launch, stopped = node["launched_at"], node["stopped_at"]
        through = min(end, now, stopped if stopped is not None else now)
        changes = defaultdict(Counter)
        for job in conn.execute(
            "SELECT * FROM jobs WHERE node_id=? AND started_at IS NOT NULL AND started_at < ?",
            (node["id"], through),
        ):
            began = job["started_at"]
            finished = job["finished_at"]
            last = min(through, finished if finished is not None else through)
            if last > began:
                changes[began][job["team"]] += job["gpus"]
                changes[last][job["team"]] -= job["gpus"]
                gpu_seconds[job["team"]] += max(0, last - max(start, began)) * job["gpus"]

        rate = Fraction(node["hourly_cents"], 100 * 3600)
        active = Counter()
        busy_usage = Counter()
        last_usage = {node["sponsor"]: 1}
        lifetime_charges = defaultdict(Fraction)
        previous = launch
        for boundary in sorted(set(changes) | {through}):
            elapsed = boundary - previous
            if elapsed > 0:
                weights = active or last_usage
                charge_by_team(lifetime_charges, rate * elapsed, weights)
                in_window = boundary - max(previous, start)
                if in_window > 0:
                    charge_by_team(charges, rate * in_window, weights)
                for team, gpus in active.items():
                    busy_usage[team] += gpus * elapsed
            was_busy = bool(active)
            active.update(changes[boundary])
            active = +active
            if was_busy and not active and busy_usage:
                last_usage, busy_usage = busy_usage, Counter()
            previous = boundary

        if stopped is not None and start <= stopped < end and stopped <= now:
            top_up = max(0, MINIMUM_BILLED_SECONDS - (stopped - launch))
            if top_up:
                charge_by_team(charges, rate * top_up, lifetime_charges or {node["sponsor"]: 1})

    # Allocate rounding remainders so the displayed team amounts add to the displayed total.
    micros = {team: value * MICRODOLLARS_PER_DOLLAR for team, value in charges.items()}
    rounded = {team: int(value) for team, value in micros.items()}
    total = round(sum(micros.values()))
    by_remainder = sorted(micros, key=lambda team: (-(micros[team] - rounded[team]), team))
    for team in by_remainder[:total - sum(rounded.values())]:
        rounded[team] += 1
    return {
        "from": start,
        "to": end,
        "as_of": now,
        "total_usd": total / MICRODOLLARS_PER_DOLLAR,
        "teams": [
            {"team": team, "gpu_hours": round(gpu_seconds[team] / 3600, 8),
             "cost_usd": rounded.get(team, 0) / MICRODOLLARS_PER_DOLLAR}
            for team in sorted(set(rounded) | {t for t in gpu_seconds if gpu_seconds[t]})
        ],
    }
