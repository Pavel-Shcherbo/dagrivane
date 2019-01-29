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


def run(spec, state_path, run_id, cwd=None):
    tasks = validate(spec)
    cwd = str(Path(cwd or ".").resolve())
    verify_inputs(spec, cwd)
    fingerprint = hashlib.sha256(
        json.dumps({"spec": spec, "cwd": cwd}, sort_keys=True).encode()
    ).hexdigest()
    Path(state_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(state_path) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL)"
        )
        db.execute("""CREATE TABLE IF NOT EXISTS tasks (
            run_id TEXT, id TEXT, status TEXT NOT NULL, attempts INTEGER NOT NULL,
            output TEXT NOT NULL, PRIMARY KEY (run_id, id))""")
        previous = db.execute(
            "SELECT fingerprint FROM runs WHERE id=?", (run_id,)
        ).fetchone()
        if previous and previous[0] != fingerprint:
            raise ValueError(
                "run ID belongs to a different specification or working directory"
            )
        db.execute("INSERT OR IGNORE INTO runs VALUES (?, ?)", (run_id, fingerprint))
        db.commit()
        records = {
            row[0]: row[1]
            for row in db.execute(
                "SELECT id,status FROM tasks WHERE run_id=?", (run_id,)
            )
        }
        if "running" in records.values():
            raise ValueError(
                "interrupted task has an unknown outcome; inspect effects and use a new run ID"
            )
        for task in tasks:
            name = task["id"]
            if records.get(name) == "succeeded":
                continue
            if any(records.get(dep) != "succeeded" for dep in task.get("needs", [])):
                db.execute(
                    "INSERT OR REPLACE INTO tasks VALUES (?,?,?,0,?)",
                    (run_id, name, "blocked", "dependency failed"),
                )
                db.commit()
                records[name] = "blocked"
                continue
            for _ in range(task.get("retries", 0) + 1):
                attempts = db.execute(
                    "SELECT attempts FROM tasks WHERE run_id=? AND id=?", (run_id, name)
                ).fetchone()
                count = (attempts[0] if attempts else 0) + 1
                db.execute(
                    "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?)",
                    (run_id, name, "running", count, ""),
                )
                db.commit()
                try:
                    succeeded, output = execute(
                        task["command"], cwd, task.get("timeout", 30)
                    )
                    status = "succeeded" if succeeded else "failed"
                except OSError as error:
                    status, output = "failed", str(error)
                db.execute(
                    "UPDATE tasks SET status=?,output=? WHERE run_id=? AND id=?",
                    (status, output[-16384:], run_id, name),
                )
                db.commit()
                records[name] = status
                if status == "succeeded":
                    break
        saved = {
            row[0]: {
                "id": row[0],
                "status": row[1],
                "attempts": row[2],
                "output": row[3],
            }
            for row in db.execute(
                "SELECT id,status,attempts,output FROM tasks WHERE run_id=?", (run_id,)
            )
        }
        return [saved[task["id"]] for task in tasks]
