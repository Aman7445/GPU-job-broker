"""Validate the submission fields used by FastAPI and trace replay."""

from decimal import Decimal, InvalidOperation
from math import ceil


MAX_ESTIMATE_MINUTES = 5_256_000


def minutes_to_seconds(value, *, field="estimated_minutes", allow_zero=False):
    try:
        minutes = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"{field} must be a number") from None
    valid_minimum = minutes.is_finite() and (minutes >= 0 if allow_zero else minutes > 0)
    if not valid_minimum or minutes > MAX_ESTIMATE_MINUTES:
        minimum = "nonnegative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {minimum} and at most 5,256,000")
    return max(1, ceil(minutes * 60)) if minutes else 0


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

    seconds = minutes_to_seconds(body.get("estimated_minutes"))

    return team.strip(), client_key.strip(), gpus, seconds, priority
