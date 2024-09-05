"""Retained timezone clocks and bounded minute-resolution recurrence expansion."""

import base64
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
import hashlib
from importlib import resources
import io
from pathlib import Path
import re
import struct
from zoneinfo import TZPATH, ZoneInfo

UTC = timezone.utc
MIN_TIME = 0
MAX_TIME = 4133980800  # 2101-01-01T00:00:00Z, exclusive.


