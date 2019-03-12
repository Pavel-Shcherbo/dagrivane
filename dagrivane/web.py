import json
import sys
import threading
import uuid
from pathlib import Path

from .engine import run
from .httpapp import serve

ROOT = Path(__file__).resolve().parents[1]
RUN_LOCK = threading.Lock()


def example():
    return json.loads((ROOT / "examples/pipeline.json").read_text())


