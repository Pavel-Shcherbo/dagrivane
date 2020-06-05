"""Schema-aware relational operations with bounded cardinality and typed values."""

import copy
from dataclasses import dataclass
from decimal import Decimal, localcontext, ROUND_HALF_EVEN

from .contracts import Column, Contract, convert, identifier
from .tables import Table, MAX_ROWS, MAX_CELLS


