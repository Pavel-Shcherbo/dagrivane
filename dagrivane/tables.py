"""Immutable bounded tables, canonical identities and safe local CSV ingestion."""

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from pathlib import Path

from .contracts import Contract, encode

MAX_ROWS = 100000
MAX_CELLS = 2000000
MAX_BYTES = 16 * 1024 * 1024


def local_path(root, relative):
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("data paths must be non-empty relative paths")
    base = Path(root).resolve()
    target = (base / relative).resolve()
    if not target.is_relative_to(base) or target == base:
        raise ValueError("data path escapes the workflow directory")
    return target


@dataclass(frozen=True)
class Table:
    contract: Contract
    records: tuple
    sources: tuple = ()

    def __post_init__(self):
        if not isinstance(self.contract, Contract):
            raise ValueError("table requires a compiled contract")
        if not isinstance(self.records, tuple) or len(self.records) > MAX_ROWS:
            raise ValueError("table exceeds row budget or records are not immutable")
        if len(self.records) * len(self.contract.columns) > MAX_CELLS:
            raise ValueError("table exceeds cell budget")
        parsed = []
        byte_count = 0
        for number, values in enumerate(self.records, 1):
            if not isinstance(values, tuple) or len(values) != len(
                self.contract.columns
            ):
                raise ValueError(f"row {number}: field count differs from contract")
            try:
                record = tuple(
                    column.parse(value)
                    for column, value in zip(self.contract.columns, values)
                )
                byte_count += len(
                    json.dumps([encode(value) for value in record]).encode()
                )
                if byte_count > MAX_BYTES:
                    raise ValueError("table exceeds encoded byte budget")
                parsed.append(record)
            except ValueError as error:
                raise ValueError(f"row {number}: {error}") from error
        if not isinstance(self.sources, tuple) or any(
            not isinstance(item, str) for item in self.sources
        ):
            raise ValueError("table sources must be immutable identity strings")
        object.__setattr__(self, "records", tuple(parsed))

    @classmethod
    def from_rows(cls, contract, rows, sources=()):
        records = []
        byte_count = 0
        for number, row in enumerate(rows, 1):
            if number > MAX_ROWS or number * len(contract.columns) > MAX_CELLS:
                raise ValueError("table exceeds row or cell budget")
            parsed = contract.parse_row(row)
            record = tuple(parsed[name] for name in contract.names)
            byte_count += len(json.dumps([encode(value) for value in record]).encode())
            if byte_count > MAX_BYTES:
                raise ValueError("table exceeds encoded byte budget")
            records.append(record)
        return cls(contract, tuple(records), tuple(sources))

    def rows(self):
        for record in self.records:
            yield dict(zip(self.contract.names, record))

    def json_rows(self):
        return [
            {name: encode(value) for name, value in row.items()} for row in self.rows()
        ]

    def digest(self):
        digest = hashlib.sha256()
        digest.update(
            json.dumps(
                self.contract.definition(), sort_keys=True, separators=(",", ":")
            ).encode()
        )
        digest.update(b"\n")
        for row in self.rows():
            digest.update(
                json.dumps(
                    {name: encode(value) for name, value in row.items()},
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
            digest.update(b"\n")
        return digest.hexdigest()

