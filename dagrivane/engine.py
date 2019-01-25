"""Execute validated command graphs and persist task outcomes in SQLite."""

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
from pathlib import Path

from .artifacts import verify_inputs


