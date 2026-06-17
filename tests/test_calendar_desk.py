"""Calendar snapshots, receipt atomicity, input fencing and actual HTTP controls."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import copy
import csv
from decimal import Decimal
import http.client
import io
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from dagrivane.calendar_desk import CalendarDesk
from dagrivane.control import ControlStore, Conflict, ControlError
from dagrivane.operations import Operations
from dagrivane.schedule_evidence import verify_schedule_evidence
from dagrivane.schedule_model import digest
from dagrivane.resources import examples_directory
from tests.test_schedules import interval, cron


class CalendarDeskTests(unittest.TestCase):
    pass
