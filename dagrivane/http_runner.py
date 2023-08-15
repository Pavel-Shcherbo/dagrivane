"""Resume retained HTTP pages before fetching and keep connector ownership alive."""

import threading
import time
import uuid

from .http_client import FetchCancelled, HTTPSourceError, fetch_page
from .http_store import SourceLeaseLost


def poll_source(
    store,
    name,
    *,
    max_pages=25,
    lease_seconds=30,
    deadline_seconds=60,
    should_cancel=None,
    after_stage=None,
    after_apply=None,
):
    if type(max_pages) is not int or not 1 <= max_pages <= 100:
        raise ValueError("HTTP poll page limit must be 1–100")
    if (
        type(deadline_seconds) not in (int, float)
        or not 0.05 <= deadline_seconds <= 300
    ):
        raise ValueError("HTTP poll deadline must be 0.05–300 seconds")
    for callback in (should_cancel, after_stage, after_apply):
        if callback is not None and not callable(callback):
            raise ValueError("HTTP poll callbacks must be callable")
    claim = store.claim(name, "poll-" + uuid.uuid4().hex, lease_seconds=lease_seconds)
    if claim is None:
        status = store.status(name)
        reason = (
            "blocked"
            if status["pending"] and status["pending"]["state"] == "blocked"
            else (
                "cooldown"
                if status["not_before"] is not None
                and status["not_before"] > time.time()
                else "busy"
            )
        )
        return {"status": reason, "drained": False, "pages": [], "source": status}
    stop = threading.Event()
    lost = threading.Event()
    pages = []
    deadline = time.monotonic() + deadline_seconds

    def cancelled():
        return lost.is_set() or (should_cancel is not None and should_cancel())

    def heartbeat():
        while not stop.wait(lease_seconds / 3):
            try:
                store.heartbeat(claim)
            except Exception:
                lost.set()
                return

    thread = threading.Thread(
        target=heartbeat, name="http-source-heartbeat", daemon=True
    )
    thread.start()
    result_status = "limited"
    error = None
    retry_at = None
    try:
        for _ in range(max_pages):
            if cancelled():
                raise FetchCancelled()
            if time.monotonic() >= deadline:
                result_status = "deadline"
                break
            checkpoint = store.checkpoint(claim)
            page_id = checkpoint["pending"]
            if page_id is None:
                received = fetch_page(
                    checkpoint["configuration"],
                    checkpoint["cursor"],
                    should_cancel=cancelled,
                    deadline=deadline,
                )
                page_id = store.stage(claim, checkpoint, received)
                if after_stage:
                    after_stage(page_id)
            if cancelled():
                raise FetchCancelled()
            try:
                applied = store.apply(claim, page_id)
            except SourceLeaseLost:
                raise
            except ValueError as problem:
                store.block(claim, page_id, problem)
                error = problem
                result_status = "blocked"
                break
            ingestion = applied["ingestion"]
            summary = {
                key: applied[key]
                for key in (
                    "page_id",
                    "offset_before",
                    "offset_after",
                    "generation",
                    "items",
                    "more",
                    "reused",
                )
            }
            summary["ingestion"] = (
                {
                    key: ingestion[key]
                    for key in ("batch_id", "counts", "rows", "tombstones")
                }
                if ingestion is not None
                else None
            )
            pages.append(summary)
            if after_apply:
                after_apply(summary)
            if not applied["more"]:
                result_status = "drained"
                break
    except FetchCancelled as problem:
        result_status = "ownership_lost" if lost.is_set() else "cancelled"
        error = problem
    except SourceLeaseLost as problem:
        result_status = "ownership_lost"
        error = problem
        lost.set()
    except HTTPSourceError as problem:
        result_status = "deferred" if problem.retry_at is not None else "error"
        retry_at = problem.retry_at
        error = problem
    except ValueError as problem:
        result_status = "error"
        error = problem
    finally:
        stop.set()
        thread.join()
        if not lost.is_set():
            try:
                store.release(claim, error=error, retry_at=retry_at)
            except SourceLeaseLost:
                lost.set()
                result_status = "ownership_lost"
    result = {
        "status": result_status,
        "drained": result_status == "drained",
        "pages": pages,
        "source": store.status(name),
    }
    if error is not None:
        result["error"] = str(error)
    return result
