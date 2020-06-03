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

    def summary(self):
        return {
            "rows": len(self.records),
            "columns": len(self.contract.columns),
            "contract": self.contract.definition(),
            "data_sha256": self.digest(),
            "source_sha256": list(self.sources),
        }


def read_csv(path, contract, *, null_token="\\N", expected_sha256=None):
    if (
        not isinstance(null_token, str)
        or not 1 <= len(null_token) <= 32
        or any(character in null_token for character in "\r\n\x00")
    ):
        raise ValueError("null_token must be a single-line string of 1–32 characters")
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
    ):
        raise ValueError("expected CSV hash must be lowercase SHA-256")
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("CSV exceeds 16 MiB byte budget")
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError("CSV content differs from its pinned hash")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("CSV must use UTF-8") from error
    if "\x00" in text:
        raise ValueError("CSV cannot contain NUL")
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    records = []
    try:
        header = next(reader, None)
        if header is None or tuple(header) != contract.names:
            raise ValueError("CSV header order must exactly match the contract")
        for number, values in enumerate(reader, 1):
            if number > MAX_ROWS or number * len(contract.columns) > MAX_CELLS:
                raise ValueError("CSV exceeds row or cell budget")
            if len(values) != len(header):
                raise ValueError(
                    f"CSV record {number}: field count differs from header"
                )
            try:
                parsed = contract.parse_row(
                    dict(zip(header, values)), from_csv=True, null_token=null_token
                )
            except ValueError as error:
                raise ValueError(f"CSV record {number}: {error}") from error
            records.append(tuple(parsed[name] for name in header))
    except csv.Error as error:
        raise ValueError("CSV syntax or field-size limit exceeded") from error
    return Table(contract, tuple(records), (digest,))
