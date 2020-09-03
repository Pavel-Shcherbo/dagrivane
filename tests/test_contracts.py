import datetime as dt
import unittest
from decimal import Decimal

from dagrivane.contracts import Column, Contract, compatibility


def contract(columns, version=1):
    return Contract.compile({"name": "orders", "version": version, "columns": columns})


class ContractTests(unittest.TestCase):
    pass
