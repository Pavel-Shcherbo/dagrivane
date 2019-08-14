"""Locate read-only examples and a separate writable local state directory."""

import os
import argparse
import json
import re
from shutil import copy2
from pathlib import Path


