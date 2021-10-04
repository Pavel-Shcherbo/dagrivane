"""Reviewed local release controls exposed through the loopback workbench."""

import sqlite3
import uuid

from .release_demo import run_demo, SCENARIOS
from .resources import state_directory


def evaluate_release(params):
    if set(params) - {"mode", "scenario"}:
        raise ValueError(
            "release workbench accepts only the reviewed scenario selector"
        )
    scenario = params.get("scenario", "healthy")
    if not isinstance(scenario, str) or scenario not in SCENARIOS:
        raise ValueError("unknown local release scenario")
    run_id = str(uuid.uuid4())
    directory = state_directory("dagrivane") / "release-runs" / run_id
    try:
        result = run_demo(directory, scenario)
    except sqlite3.Error as error:
        raise ValueError(
            "local artifact catalog could not complete the request"
        ) from error
    return {**result, "run_id": run_id}
