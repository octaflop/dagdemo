# coding: utf-8
"""URL-safe encodings for compacting a UUID7 string in a URL.

Run with:

    python uuid_playground.py          # interactive demo (random UUID)
    python uuid_playground.py --fixed  # sealed demo with a fixed UUID (for docstring tests)
    python -m doctest uuid_playground.py -v
"""

from __future__ import annotations

import base64
import sys
import uuid
from string import ascii_lowercase, ascii_uppercase, digits
from urllib.parse import quote

from uuid_extensions import uuid7, uuid_to_datetime

# -- alphabets ----------------------------------------------------------

BASE62 = digits + ascii_uppercase + ascii_lowercase

# Crockford base32 — avoids I L O U to prevent confusion with 1/0/V
CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# Base58 (Bitcoin-style) — avoids 0 O I l and + / =
BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

# -- fixed UUID for reproducible demos / doctests -----------------------

FIXED_UUID = uuid.UUID("06a0202e-5fb7-7dfa-8000-a837b47b8b97")


# -- general-purpose base-N codec ---------------------------------------

def _encode_int(n: int, alphabet: str) -> str:
    if n == 0:
        return alphabet[0]
    chars: list[str] = []
    base = len(alphabet)
    while n > 0:
        n, rem = divmod(n, base)
        chars.append(alphabet[rem])
    return "".join(reversed(chars))


def _decode_int(s: str, alphabet: str) -> int:
    base = len(alphabet)
    n = 0
    for ch in s:
        n = n * base + alphabet.index(ch)
    return n


# -- each encoding gets a from_uuid / to_uuid pair -----------------------

def b64std_from_uuid(u: uuid.UUID) -> str:
    """Standard base64 (+, /, =). Needs percent-encoding in URLs.

    >>> b64std_from_uuid(FIXED_UUID)
    'BqAgLl+3ffqAAKg3tHuLlw=='
    """
    return base64.b64encode(u.bytes).decode()


def b64std_to_uuid(s: str) -> uuid.UUID:
    """Reverse of b64std_from_uuid.

    >>> b64std_to_uuid('BqAgLl+3ffqAAKg3tHuLlw==') == FIXED_UUID
    True
    """
    return uuid.UUID(bytes=base64.b64decode(s))


def b64url_padded_from_uuid(u: uuid.UUID) -> str:
    """URL-safe base64 *with* = padding preserved.

    >>> b64url_padded_from_uuid(FIXED_UUID)
    'BqAgLl-3ffqAAKg3tHuLlw=='
    """
    return base64.urlsafe_b64encode(u.bytes).decode()


def b64url_padded_to_uuid(s: str) -> uuid.UUID:
    """Reverse of b64url_padded_from_uuid.

    >>> b64url_padded_to_uuid('BqAgLl-3ffqAAKg3tHuLlw==') == FIXED_UUID
    True
    """
    return uuid.UUID(bytes=base64.urlsafe_b64decode(s))


def b64url_nopad_from_uuid(u: uuid.UUID) -> str:
    """URL-safe base64, = padding stripped — standard modern approach.

    >>> b64url_nopad_from_uuid(FIXED_UUID)
    'BqAgLl-3ffqAAKg3tHuLlw'
    """
    return base64.urlsafe_b64encode(u.bytes).rstrip(b"=").decode()


def b64url_nopad_to_uuid(s: str) -> uuid.UUID:
    """Reverse of b64url_nopad_from_uuid.

    >>> b64url_nopad_to_uuid('BqAgLl-3ffqAAKg3tHuLlw') == FIXED_UUID
    True
    """
    padded = s + "=" * (-len(s) % 4)
    return uuid.UUID(bytes=base64.urlsafe_b64decode(padded))


def b62_from_uuid(u: uuid.UUID) -> str:
    """Base62 — pure alphanumeric, zero URL encoding needed.

    >>> b62_from_uuid(FIXED_UUID)
    'CV89vUs9ouko9WWCVNVYd'
    """
    return _encode_int(u.int, BASE62)


def b62_to_uuid(s: str) -> uuid.UUID:
    """Reverse of b62_from_uuid.

    >>> b62_to_uuid('CV89vUs9ouko9WWCVNVYd') == FIXED_UUID
    True
    """
    return uuid.UUID(int=_decode_int(s, BASE62))


def b58_from_uuid(u: uuid.UUID) -> str:
    """Base58 — avoids 0 O I l; also zero URL overhead.

    >>> b58_from_uuid(FIXED_UUID)
    'pTEgx1Y5mW1vL5q7fPBGz'
    """
    return _encode_int(u.int, BASE58)


def b58_to_uuid(s: str) -> uuid.UUID:
    """Reverse of b58_from_uuid.

    >>> b58_to_uuid('pTEgx1Y5mW1vL5q7fPBGz') == FIXED_UUID
    True
    """
    return uuid.UUID(int=_decode_int(s, BASE58))


def c32_from_uuid(u: uuid.UUID) -> str:
    """Crockford base32 — designed for identifiers, human-friendly.

    >>> c32_from_uuid(FIXED_UUID)
    '6M0G2WQXQFQX800586YT7Q2WQ'
    """
    return _encode_int(u.int, CROCKFORD_BASE32)


def c32_to_uuid(s: str) -> uuid.UUID:
    """Reverse of c32_from_uuid.

    >>> c32_to_uuid('6M0G2WQXQFQX800586YT7Q2WQ') == FIXED_UUID
    True
    """
    return uuid.UUID(int=_decode_int(s, CROCKFORD_BASE32))


# -- demo ---------------------------------------------------------------

def demo(uid: uuid.UUID | None = None) -> None:
    uid = uid or uuid7()

    encodings: list[tuple[str, str]] = [
        ("canonical", str(uid)),
        ("hex (no dashes)", uid.hex),
        ("base64 std", b64std_from_uuid(uid)),
        ("base64 url w/ pad", b64url_padded_from_uuid(uid)),
        ("base64 url no pad", b64url_nopad_from_uuid(uid)),
        ("base62", b62_from_uuid(uid)),
        ("base58", b58_from_uuid(uid)),
        ("crockford base32", c32_from_uuid(uid)),
    ]

    print(f"{'encoding':<22} {'raw':>4}  {'URL-quoted':>10}    example")
    print("-" * 78)
    for name, encoded in encodings:
        quoted = quote(encoded, safe="")
        print(f"{name:<22} {len(encoded):>4}  {len(quoted):>10}    {encoded}")

    print()
    dt = uuid_to_datetime(uid)
    print(f"embedded datetime       {dt}")


if __name__ == "__main__":
    uid = FIXED_UUID if "--fixed" in sys.argv else uuid7()
    demo(uid)
    print()
    if uid == FIXED_UUID:
        print("(seeded with FIXED_UUID for reproducible output)")
    print("verify with:  python -m doctest uuid_playground.py -v")
