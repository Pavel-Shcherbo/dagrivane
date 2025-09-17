"""Explicit column projection with lossless casts and preserved stream identities."""

import copy
import json
from decimal import Decimal

from .catalog_contracts import fingerprint, json_bytes, token
from .contracts import Contract, compatibility, convert, encode
from .ingest_model import normalized_key, stream_definition, instant
from .quality import compile_gates
from .tables import Table


