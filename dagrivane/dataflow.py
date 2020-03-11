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


