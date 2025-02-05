"""Explicit foreground dispatch loop with bounded batches and restart-safe tick keys."""

import threading
import time

from .control import text
from .schedule_clock import instant


