"""Synthetic loopback-only HTTP source for the documented ingestion control."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


def row(number, amount, day=1):
    return {
        "id": number,
        "amount": amount,
        "updated_at": f"2020-01-{day:02d}T12:00:00Z",
    }


def change(identifier, number, amount=None, day=1):
    return {
        "id": identifier,
        "operation": "delete" if amount is None else "upsert",
        "key": {"id": number},
        "at": f"2020-01-{day:02d}T12:00:00Z",
        "row": row(number, amount, day) if amount is not None else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8421)
    parser.add_argument(
        "--scenario",
        choices=["healthy", "over-budget", "retry-once"],
        default="healthy",
    )
    args = parser.parse_args()
    lock = threading.Lock()
    metrics = {"data_requests": 0, "retry_sent": False}

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, value=None, headers=None):
            raw = (
                json.dumps(value, separators=(",", ":")).encode()
                if value is not None
                else b""
            )
            self.send_response(status)
            for key, text in {
                "Content-Type": "application/json",
                "Content-Length": str(len(raw)),
                "Cache-Control": "no-store",
                **(headers or {}),
            }.items():
                self.send_header(key, text)
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query)
            if parsed.path == "/health":
                return self.respond(200, {"status": "ok", "data": "synthetic"})
            if parsed.path == "/metrics":
                with lock:
                    value = dict(metrics)
                return self.respond(200, value)
            if parsed.path not in ("/orders", "/changes"):
                return self.respond(404, {"error": "unknown synthetic endpoint"})
            scenario = query.get("scenario", [args.scenario])[0]
            if scenario not in ("healthy", "over-budget", "retry-once"):
                return self.respond(400, {"error": "unknown control scenario"})
            with lock:
                metrics["data_requests"] += 1
                retry = scenario == "retry-once" and not metrics["retry_sent"]
                if retry:
                    metrics["retry_sent"] = True
            if retry:
                return self.respond(
                    503, {"error": "synthetic transient failure"}, {"Retry-After": "0"}
                )
            cursor = query.get("cursor", [None])[0]
            if cursor == "tail":
                return self.respond(204)
            if cursor not in (None, "page2"):
                return self.respond(409, {"error": "unknown synthetic cursor"})
            first = cursor is None
            amount = "2000" if scenario == "over-budget" and first else "10"
            if parsed.path == "/changes":
                items = (
                    [change("change-1", 1, amount), change("change-2", 2, "20")]
                    if first
                    else [
                        change("change-3", 1, "15", 2),
                        change("change-4", 2, None, 2),
                        change("change-5", 3, "30", 2),
                    ]
                )
                value = {"events": items}
            else:
                items = (
                    [
                        {**row(1, amount), "revision": "row-1-v1"},
                        {**row(2, "20"), "revision": "row-2-v1"},
                    ]
                    if first
                    else [
                        {**row(1, "15", 2), "revision": "row-1-v2"},
                        {**row(3, "30", 2), "revision": "row-3-v1"},
                    ]
                )
                value = {"data": items}
            value.update(next_cursor="page2" if first else "tail", has_more=first)
            self.respond(200, value)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        print("HTTP source: http://127.0.0.1:" + str(server.server_port), flush=True)
        try:
            server.serve_forever(poll_interval=0.05)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
