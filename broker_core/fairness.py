"""Share capacity between teams while keeping priority and wait time visible."""

PRIORITIES = {"low": 0, "normal": 1, "high": 2}
PROMOTION_SECONDS = 30 * 60
PROTECTION_SECONDS = 60 * 60


def rank_key(job, now, team_gpus, protected_job_id):
    promotions = (now - job["submitted_at"]) // PROMOTION_SECONDS
    priority = min(PRIORITIES["high"], PRIORITIES[job["priority"]] + promotions)
    return (job["id"] != protected_job_id, -priority, team_gpus.get(job["team"], 0), job["id"])


def protected_placement(queued, nodes, now, node_limit):
    """Select the oldest job without a reservation and, when possible, one node to drain."""
    long_waiting = (
        job for job in queued
        if job["node_id"] is None and now - job["submitted_at"] >= PROTECTION_SECONDS
    )
    job = min(long_waiting, key=lambda row: (row["submitted_at"], row["id"]), default=None)
    if job is None:
        return None, None

    fits = [node for node in nodes if node["gpus"] >= job["gpus"]]
    if fits:
        held = min(fits, key=lambda node: (
            node["gpus"] - node["used"] < job["gpus"],
            node["status"] != "ready",
            node["id"],
        ))
        return job["id"], held["id"]
    if len(nodes) >= node_limit:
        # Keep the same ready node as jobs finish, so incoming work cannot refill it.
        held = min(nodes, key=lambda node: (node["status"] != "ready", node["id"]))
        return job["id"], held["id"]
    return job["id"], None


def fifo_key(job):
    return -PRIORITIES[job["priority"]], job["id"]
