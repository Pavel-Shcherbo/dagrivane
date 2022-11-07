"""Durable local order-workflow operations over reviewed installation templates."""

import copy
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .catalog_contracts import table_bundle
from .control import ControlStore, Conflict, canonical
from .quality import compile_gates
from .resources import examples_directory
from .worker import Worker

PROFILES = {
    "standard": "Order checks",
    "quality-failure": "Quality failure control",
    "cancellation": "Cancellation control",
}


class Operations:
    pass
