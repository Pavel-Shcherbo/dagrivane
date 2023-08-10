"""Explicit HTTP pagination, typed row mapping and stable change identities."""

import copy
import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from .catalog_contracts import fingerprint, json_bytes
from .contracts import Contract, encode
from .ingest_model import MAX_BATCH_BYTES, instant, normalized_key, offset


def pointer(value):
    if (
        not isinstance(value, str)
        or len(value) > 512
        or value
        and not value.startswith("/")
    ):
        raise ValueError(
            "JSON pointer requires bounded text beginning with / or an empty root"
        )
    parts = []
    for part in value.split("/")[1:]:
        if re.search(r"~(?![01])", part):
            raise ValueError("invalid JSON pointer escape")
        parts.append(part.replace("~1", "/").replace("~0", "~"))
    return tuple(parts)


def extract(value, path):
    for part in pointer(path):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif (
            isinstance(value, list)
            and re.fullmatch(r"0|[1-9][0-9]{0,8}", part)
            and int(part) < len(value)
        ):
            value = value[int(part)]
        else:
            raise ValueError("required HTTP JSON pointer is missing")
    return value


def opaque(value):
    if value is None:
        return None
    if type(value) is int and 0 <= value < 2**63:
        return value
    if (
        isinstance(value, str)
        and 1 <= len(value.encode("utf-8")) <= 2048
        and not any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        return value
    raise ValueError(
        "pagination cursor must be null, bounded text or a nonnegative 64-bit integer"
    )


def endpoint(value):
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 4096
        or any(ord(c) <= 32 or ord(c) == 127 for c in value)
        or "\\" in value
    ):
        raise ValueError("HTTP endpoint requires a bounded absolute URL")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("endpoint must be HTTP(S), without credentials or a fragment")
    host = parsed.hostname.encode("idna").decode().lower()
    port = parsed.port
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("invalid HTTP endpoint port")
    authority = ("[" + host + "]" if ":" in host else host) + (
        ":" + str(port) if port is not None else ""
    )
    return urlunsplit(
        (
            parsed.scheme,
            authority,
            quote(parsed.path or "/", safe="/%:@!$&'()*+,;=-._~"),
            urlencode(parse_qsl(parsed.query, keep_blank_values=True)),
            "",
        )
    )


def origin(url):
    parsed = urlsplit(endpoint(url))
    return (
        parsed.scheme,
        parsed.hostname,
        parsed.port or (443 if parsed.scheme == "https" else 80),
    )


def definition(value, stream):
    fields = {
        "version",
        "url",
        "allow_http",
        "cursor_parameter",
        "initial_cursor",
        "response",
        "request",
        "authentication",
        "limits",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError(
            "HTTP source requires version 1 and explicit URL/cursor/response/request/authentication/limits"
        )
    result = copy.deepcopy(value)
    result["url"] = endpoint(value["url"])
    if (
        type(value["allow_http"]) is not bool
        or urlsplit(result["url"]).scheme == "http"
        and not value["allow_http"]
    ):
        raise ValueError("plain HTTP requires explicit allow_http=true")
    parameter = value["cursor_parameter"]
    if not isinstance(parameter, str) or not re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_.-]{0,63}", parameter
    ):
        raise ValueError("cursor parameter must be a bounded identifier")
    query = parse_qsl(urlsplit(result["url"]).query, keep_blank_values=True)
    if any(key == parameter for key, _ in query):
        raise ValueError("endpoint query already contains the cursor parameter")
    result["initial_cursor"] = opaque(value["initial_cursor"])
    response = value["response"]
    if (
        not isinstance(response, dict)
        or set(response)
        != {"items", "next_cursor", "more", "mode", "columns", "event_id"}
        or response["mode"] not in ("rows", "events")
    ):
        raise ValueError(
            "HTTP response requires item/cursor pointers and an explicit rows/events mode"
        )
    for key in ("items", "next_cursor"):
        pointer(response[key])
    for key in ("more", "event_id"):
        if response[key] is not None:
            pointer(response[key])
    if response["mode"] == "rows":
        columns = response["columns"]
        contract = Contract.compile(stream["contract"])
        if not isinstance(columns, dict) or set(columns) != set(contract.names):
            raise ValueError("HTTP row mapping must cover the typed contract exactly")
        for path in columns.values():
            pointer(path)
    elif response["columns"] is not None or response["event_id"] is not None:
        raise ValueError(
            "event envelopes provide their own keys, rows and revision IDs"
        )
    request = value["request"]
    if not isinstance(request, dict) or set(request) != {
        "timeout_seconds",
        "attempts",
        "retry_base_seconds",
        "max_inline_wait_seconds",
        "redirects",
    }:
        raise ValueError("HTTP request requires explicit timeout/retry/redirect bounds")
    for key, minimum, maximum in [
        ("timeout_seconds", 0.05, 30),
        ("retry_base_seconds", 0, 5),
        ("max_inline_wait_seconds", 0, 30),
    ]:
        number = request[key]
        if (
            type(number) not in (int, float)
            or not math.isfinite(number)
            or not minimum <= number <= maximum
        ):
            raise ValueError("invalid HTTP request timing limit: " + key)
    if (
        type(request["attempts"]) is not int
        or not 1 <= request["attempts"] <= 5
        or type(request["redirects"]) is not int
        or not 0 <= request["redirects"] <= 5
    ):
        raise ValueError("HTTP retries/redirects exceed their bounded limits")
    authentication = value["authentication"]
    if authentication is not None:
        if (
            not isinstance(authentication, dict)
            or set(authentication) != {"header", "environment", "prefix"}
            or authentication["header"] not in ("Authorization", "X-API-Key")
        ):
            raise ValueError(
                "authentication requires an approved header and environment reference"
            )
        if not isinstance(authentication["environment"], str) or not re.fullmatch(
            r"[A-Z][A-Z0-9_]{0,79}", authentication["environment"]
        ):
            raise ValueError(
                "authentication environment must be an uppercase identifier"
            )
        if (
            not isinstance(authentication["prefix"], str)
            or len(authentication["prefix"]) > 32
            or any(ord(c) < 32 or ord(c) > 126 for c in authentication["prefix"])
        ):
            raise ValueError("authentication prefix must be bounded printable ASCII")
    limits = value["limits"]
    if (
        not isinstance(limits, dict)
        or set(limits) != {"page_bytes", "journal_bytes"}
        or type(limits["page_bytes"]) is not int
        or not 1 <= limits["page_bytes"] <= MAX_BATCH_BYTES
        or type(limits["journal_bytes"]) is not int
        or not limits["page_bytes"] <= limits["journal_bytes"] <= 512 * 1024 * 1024
    ):
        raise ValueError("HTTP page/journal byte limits are invalid")
    json_bytes(result, 65536)
    return result


def request_url(config, cursor):
    parsed = urlsplit(config["url"])
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if cursor is not None:
        query.append((config["cursor_parameter"], str(opaque(cursor))))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))


def decode_page(raw):
    if not isinstance(raw, bytes) or len(raw) > MAX_BATCH_BYTES:
        raise ValueError("HTTP JSON entity exceeds 4 MiB")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate HTTP JSON key")
            result[key] = value
        return result

    def reject(_):
        raise ValueError("non-finite HTTP JSON number")

    try:
        value = json.loads(
            raw, parse_float=Decimal, parse_constant=reject, object_pairs_hook=unique
        )
    except (
        UnicodeError,
        json.JSONDecodeError,
        RecursionError,
        InvalidOperation,
    ) as error:
        raise ValueError("invalid or excessively nested HTTP JSON") from error
    pending = [(value, 0)]
    count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 32 or count > 250000:
            raise ValueError("HTTP JSON exceeds depth/node budget")
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    return value


