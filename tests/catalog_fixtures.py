import json
from pathlib import Path

from dagrivane.contracts import Contract
from dagrivane.tables import Table


def contract(version=1):
    return {
        "name": "orders",
        "version": version,
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "amount", "type": "decimal", "min": "0"},
        ],
    }


def gates():
    return [
        {"id": "ids", "kind": "unique", "columns": ["id"]},
        {
            "id": "total",
            "kind": "sum",
            "columns": ["amount"],
            "min": "0",
            "max": "1000",
        },
    ]


