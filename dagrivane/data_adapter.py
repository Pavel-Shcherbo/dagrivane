"""Run built-in dataflow tasks through the same process boundary as command tasks."""

import json
import sys
import tempfile
from pathlib import Path

from .dataflow import compile_plan


