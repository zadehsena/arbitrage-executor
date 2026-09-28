#!/usr/bin/env python3
"""Opt-in Novig account setup. Run each command explicitly; no orders or transfers."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import time

import requests
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
)


ROOT = Path(__file__).resolve().parent.parent
SECRETS = Path.home() / ".secrets"
TRADING_PEM = SECRETS / "novig-scanner-trading.pem"
TRADING_ID = SECRETS / "novig-scanner-trading.id"
READ_PEM = SECRETS / "novig-scanner-read.pem"
READ_ID = SECRETS / "novig-scanner-read.id"


def load_env():
    values = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip("\"'")
    return values


def config():
    values = load_env()
    key_id = values.get("NOVIG_MANAGEMENT_KEY_ID") or values.get("NOVIG_KEY_ID")
    pem_path = values.get("NOVIG_MANAGEMENT_PRIVATE_KEY_PATH") or values.get("NOVIG_PRIVATE_KEY_PATH")
    if not key_id or not pem_path:
        raise RuntimeError("Set the Novig management key ID and PEM path in .env")
    environment = values.get("NOVIG_ENV", "production")
    if environment not in ("production", "qa"):
        raise RuntimeError("NOVIG_ENV must be production or qa")
    host = "https://api.qa.novig.com" if environment == "qa" else "https://api.novig.com"
    private = load_pem_private_key(Path(pem_path).expanduser().read_bytes(), password=None)
    if not isinstance(private, Ed25519PrivateKey):
        raise RuntimeError("This setup helper requires an Ed25519 management key")
    return host, key_id, private


def signed_call(host, key_id, private, method, path, body=None):
    data = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
    timestamp = str(time.time_ns() // 1_000_000)
    canonical = "\n".join((
        "NOVIG-V3", timestamp, method, path, "", hashlib.sha256(data).hexdigest()
    )).encode()
    signature = base64.b64encode(private.sign(canonical)).decode()
    try:
        response = requests.request(
            method, host + path, data=data if body is not None else None,
            headers={
                "Novig-Key-Id": key_id,
                "Novig-Timestamp": timestamp,
                "Novig-Signature": signature,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except requests.RequestException as error:
        raise RuntimeError(
            "Request outcome is unknown. Check Novig's subaccount/key list before retrying."
        ) from error
    if not response.ok:
        try:
            code = response.json().get("code", "unknown")
        except (ValueError, AttributeError):
            code = "unknown"
        raise RuntimeError(f"Novig returned HTTP {response.status_code}, code {code}")
    return response.json()


def store_new(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(data)


def keypair(path):
    SECRETS.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        private = load_pem_private_key(path.read_bytes(), password=None)
        if not isinstance(private, Ed25519PrivateKey):
            raise RuntimeError(f"Existing key at {path} is not Ed25519")
    else:
        private = Ed25519PrivateKey.generate()
        store_new(path, private.private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        ))
    public = private.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
    return public.decode()


def open_subaccount(host, key_id, private):
    if TRADING_ID.exists():
        raise RuntimeError(f"Subaccount ID already saved at {TRADING_ID}; do not create a duplicate")
    existing = signed_call(host, key_id, private, "GET", "/v3/account/subaccounts")
    if not isinstance(existing, list):
        raise RuntimeError("Unexpected Novig subaccount-list response")
    if existing:
        raise RuntimeError("Novig already has a subaccount; inspect it before creating another")
    public = keypair(TRADING_PEM)
    opened = signed_call(host, key_id, private, "POST", "/v3/account/subaccounts", {
        "label": "arbitrage-scanner",
        "publicKey": public,
        "algorithm": "Ed25519",
    })
    new_id = opened.get("keyId")
    if not isinstance(new_id, str):
        raise RuntimeError("Subaccount opened, but response lacks keyId; inspect Novig before retrying")
    store_new(TRADING_ID, (new_id + "\n").encode())
    print(f"Subaccount opened. Trading key stored at {TRADING_PEM}.")
    print(f"Subaccount ID saved at {TRADING_ID}. The subaccount was not funded.")


def issue_read_key(host, key_id, private):
    if READ_ID.exists():
        raise RuntimeError(f"Read-only key ID already saved at {READ_ID}; do not create a duplicate")
    subaccount_id = TRADING_ID.read_text().strip()
    public = keypair(READ_PEM)
    issued = signed_call(host, key_id, private, "POST", f"/v3/account/subaccounts/{subaccount_id}/keys", {
        "name": "arbitrage-scanner-read",
        "publicKey": public,
        "algorithm": "Ed25519",
        "scope": "trading::read",
    })
    read_id = issued.get("keyId")
    if not isinstance(read_id, str):
        raise RuntimeError("Read-only key created, but response lacks keyId; inspect Novig before retrying")
    store_new(READ_ID, (read_id + "\n").encode())
    print("Read-only key created. Set these scanner variables in .env:")
    print(f"NOVIG_KEY_ID={read_id}")
    print(f"NOVIG_PRIVATE_KEY_PATH={READ_PEM}")
    print("Keep the original management key ID and PEM path separately.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "open-subaccount", "issue-read-key"))
    args = parser.parse_args()
    host, key_id, private = config()
    if args.action == "status":
        existing = signed_call(host, key_id, private, "GET", "/v3/account/subaccounts")
        if not isinstance(existing, list):
            raise RuntimeError("Unexpected Novig subaccount-list response")
        print(f"Novig subaccounts: {len(existing)}")
        print(f"Local subaccount ID saved: {TRADING_ID.exists()}")
        print(f"Local read-only key ID saved: {READ_ID.exists()}")
    elif args.action == "open-subaccount":
        open_subaccount(host, key_id, private)
    else:
        issue_read_key(host, key_id, private)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, requests.RequestException) as error:
        print(f"Setup stopped: {error}", file=sys.stderr)
        sys.exit(1)
