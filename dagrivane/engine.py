"""Execute validated command graphs and persist task outcomes in SQLite."""

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
from pathlib import Path

from .artifacts import verify_inputs


def validate(spec):
    if not isinstance(spec, dict):
        raise ValueError("specification must be an object")
    tasks = spec.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("tasks must be a non-empty list")
    by_id = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise ValueError("each task must be an object")
        name = task.get("id")
        if not isinstance(name, str) or not name or name in by_id:
            raise ValueError("task IDs must be unique non-empty strings")
        command = task.get("command")
        if (
            not isinstance(command, list)
            or not command
            or not all(isinstance(x, str) and x for x in command)
        ):
            raise ValueError(f"{name}: command must be a non-empty argument list")
        deps = task.get("needs", [])
        if not isinstance(deps, list) or not all(isinstance(x, str) for x in deps):
            raise ValueError(f"{name}: needs must be a list of IDs")
        retries = task.get("retries", 0)
        timeout = task.get("timeout", 30)
        if type(retries) is not int or not 0 <= retries <= 10:
            raise ValueError(f"{name}: retries must be between 0 and 10")
        if type(timeout) not in (int, float) or not 0 < timeout <= 3600:
            raise ValueError(f"{name}: timeout must be between 0 and 3600 seconds")
        by_id[name] = task
    ordered, visiting, visited = [], set(), set()

    def visit(name):
        if name not in by_id:
            raise ValueError(f"unknown dependency: {name}")
        if name in visiting:
            raise ValueError("dependency cycle")
        if name in visited:
            return
        visiting.add(name)
        for dep in by_id[name].get("needs", []):
            visit(dep)
        visiting.remove(name)
        visited.add(name)
        ordered.append(by_id[name])

    for name in by_id:
        visit(name)
    return ordered


def execute(command, cwd, timeout):
    with subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=(os.name == "posix"),
    ) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            return process.returncode == 0, stdout + stderr
        except subprocess.TimeoutExpired:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
            return False, (stdout + stderr + f"\nTask exceeded {timeout} seconds")


