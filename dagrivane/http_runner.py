"""Resume retained HTTP pages before fetching and keep connector ownership alive."""

import threading
import time
import uuid

from .http_client import FetchCancelled, HTTPSourceError, fetch_page
from .http_store import SourceLeaseLost


