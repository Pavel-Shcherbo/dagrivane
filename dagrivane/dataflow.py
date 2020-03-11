"""Compile and run bounded local data plans, publishing only quality-passing output."""

import argparse
import copy
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .contracts import Contract, identifier
from .quality import compile_gates
from .tables import read_csv, local_path
from .transforms import compile_operation, fields

MAX_PLAN_BYTES = 1024 * 1024


def load_plan(payload):
    if (
        not isinstance(payload, (str, bytes))
        or len(payload if isinstance(payload, bytes) else payload.encode())
        > MAX_PLAN_BYTES
    ):
        raise ValueError("data plan must be bounded JSON text of at most 1 MiB")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON plan key")
            result[key] = value
        return result

    def reject(_):
        raise ValueError("plan numbers must be finite")

    try:
        return json.loads(payload, object_pairs_hook=unique, parse_constant=reject)
    except RecursionError as error:
        raise ValueError("plan nesting exceeds parser limit") from error


@dataclass(frozen=True)
class DataPlan:
    definition: dict
    sources: tuple
    steps: tuple
    output: str
    gates: tuple
    digest: str

