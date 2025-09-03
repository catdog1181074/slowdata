#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tracekrc20_kasplex_full.py — Kasplex-compatible KRC-20 tracer (wallet + token-wide)

token-wide mode pulls **ALL** ops for a ticker via Kasplex's /krc20/oplist?tick=... endpoint, suitable for
reconstructing the complete holder set and full transfer history for tokens
like SLOW.

Highlights
- API base defaults to Kasplex mainnet: https://api.kasplex.org/v1
- Two modes:
    1) wallet  — /krc20/oplist?address=...
    2) token   — /krc20/oplist?tick=...
- Pagination via `next` cursor (Kasplex style), limit=500 by default
- Robust retries/backoff, proxy bypass (trust_env=False)
- Normalizes fields to a clean schema:
    tx_id (hashRev preferred), timestamp_iso, token (tick), op_type, from, to, amount
- Writes narrow CSV (clean schema) and optional wide CSV with raw-prefixed keys
- Optional resume: dedupe by tx_id when appending to an existing CSV (token mode)
- Timestamp normalization tolerates ms/seconds/ISO

Usage
------
# Pull ALL SLOW ops across ALL addresses (token-wide)
python tracekrc20_kasplex_full.py \
  --mode token --token SLOW \
  --base-url https://api.kasplex.org/v1 \
  --out data/slow_token_ops.csv \
  --limit 500 --sleep 0.25 --verbose --wide --resume

# Pull KRC-20 ops for a single wallet (optionally filter a token)
python tracekrc20_kasplex_full.py \
  --mode wallet --address kaspa:qq5x... \
  --token SLOW \
  --base-url https://api.kasplex.org/v1 \
  --out data/wallet_ops.csv --verbose

"""

import argparse
import csv
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from datetime import datetime, timezone

# -----------------------------
# Defaults / Config
# -----------------------------

KASPLEX_API_DEFAULT = "https://api.kasplex.org/v1"  # mainnet
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "krc20-tracer/1.2 (+kasplex)"
}
VERIFY_SSL = True
RETRIES = 6
TIMEOUT = 30

# -----------------------------
# Utilities
# -----------------------------

def sanitize_for_fname(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-","_",".") else "_" for ch in s)

def to_iso(ts) -> str:
    """Accept ms, sec, or ISO strings; return ISO-8601 (UTC)."""
    if ts in (None, ""):
        return ""
    # numeric path
    try:
        v = float(ts)
        if v >= 1e12:  # ms → sec
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc).isoformat()
    except Exception:
        pass
    # ISO path
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return ""

def first_nonempty(d: Dict, keys: List[str], default="") -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", 0):
            return d[k]
    return default

def normalize_op(op: Dict) -> Dict:
    """Normalize a raw operation dict into the narrow schema + keep raw."""
    tx_id   = first_nonempty(op, ["txid","tx_id","hashRev","hash","txHash","transactionHash"])
    ts      = first_nonempty(op, ["mtsAdd","timestamp","ts","time","blockTime","block_ts","date"])
    token   = first_nonempty(op, ["tick","token","ticker","symbol","krc20","asset","name"])
    op_type = first_nonempty(op, ["op","opType","type","operation"])
    sender  = first_nonempty(op, ["from","sender","src","seller","maker","owner","from_address"])
    recip   = first_nonempty(op, ["to","recipient","dst","buyer","taker","to_address"])
    amount  = first_nonempty(op, ["amt","amount","qty","quantity","value","delta","amount_token","amountToken","transferAmount"], default="")
    if amount == "" and isinstance(op.get("meta"), dict):
        amount = first_nonempty(op["meta"], ["amount","qty","quantity","value","delta"], default="")
    try:
        amount_num = float(amount)
    except Exception:
        amount_num = None
    return {
        "tx_id": tx_id,
        "timestamp_raw": str(ts),
        "timestamp_iso": to_iso(ts),
        "token": token,
        "op_type": op_type,
        "from": sender,
        "to": recip,
        "amount": amount_num,
        "_raw": op,
    }

# -----------------------------
# Networking / Fetchers
# -----------------------------

def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False  # ignore system proxies
    s.proxies = {"http": None, "https": None}
    return s

def _get_json(session: requests.Session, url: str, params: Dict[str, Any], verbose: bool, save_raw_dir: Optional[Path], page: int) -> Dict:
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.get(url, params=params, headers=HEADERS, timeout=TIMEOUT, verify=VERIFY_SSL, allow_redirects=True)
            if r.status_code >= 400:
                if save_raw_dir:
                    save_raw_dir.mkdir(parents=True, exist_ok=True)
                    (save_raw_dir / f"error_{r.status_code}_page{page:05d}.raw").write_bytes(r.content)
                if r.status_code in (429, 500, 502, 503, 504):
                    sleep_for = min((2 ** attempt) * 0.25, 15.0)
                    if verbose:
                        print(f"[retry {attempt}/{RETRIES}] {r.status_code}, sleeping {sleep_for:.2f}s")
                    time.sleep(sleep_for)
                    continue
                r.raise_for_status()
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "application/json" not in ctype:
                if save_raw_dir:
                    (save_raw_dir / f"nonjson_page{page:05d}.raw").write_bytes(r.content)
                raise RuntimeError(f"Non-JSON response (Content-Type={ctype}) from {r.url}")
            try:
                data = r.json()
            except Exception as e:
                if save_raw_dir:
                    (save_raw_dir / f"badjson_page{page:05d}.raw").write_bytes(r.content)
                raise RuntimeError(f"JSON parse failed at {r.url}: {e}") from e
            if save_raw_dir:
                (save_raw_dir / f"okjson_page{page:05d}.json").write_bytes(r.content)
            return data
        except Exception as e:
            last_exc = e
            sleep_for = min((2 ** attempt) * 0.25, 15.0)
            if verbose:
                print(f"[retry {attempt}/{RETRIES}] Exception: {e}  sleeping {sleep_for:.2f}s")
            time.sleep(sleep_for)
            continue
    if last_exc:
        raise last_exc
    return {}

def fetch_oplist_by_address(address: str, base_url: str, token: Optional[str], limit: int, sleep_s: float, verbose: bool, save_raw_dir: Optional[Path]) -> List[Dict]:
    session = _session()
    out: List[Dict] = []
    cursor: Optional[str] = None
    page = 0
    url = f"{base_url.rstrip('/')}/krc20/oplist"
    while True:
        params = {"address": address, "limit": max(1, min(limit, 1000))}
        if token and token.upper() != "ALL":
            params["tick"] = token
        if cursor:
            params["next"] = cursor
        data = _get_json(session, url, params, verbose, save_raw_dir, page)
        rows = data.get("result") or []
        cursor = data.get("next")
        if not isinstance(rows, list) or not rows:
            if verbose:
                print(f"[done] total_pages={page} total_rows={len(out)}")
            break
        out.extend(rows)
        if verbose:
            print(f"[page {page}] fetched={len(rows)} cursor={cursor!r}")
        page += 1
        if not cursor:
            if verbose: print("[done] cursor empty; end")
            break
        time.sleep(sleep_s)
    return out

def fetch_oplist_by_tick(tick: str, base_url: str, limit: int, sleep_s: float, verbose: bool, save_raw_dir: Optional[Path]) -> List[Dict]:
    session = _session()
    out: List[Dict] = []
    cursor: Optional[str] = None
    page = 0
    url = f"{base_url.rstrip('/')}/krc20/oplist"
    while True:
        params = {"tick": tick, "limit": max(1, min(limit, 1000))}
        if cursor:
            params["next"] = cursor
        data = _get_json(session, url, params, verbose, save_raw_dir, page)
        rows = data.get("result") or []
        cursor = data.get("next")
        if not isinstance(rows, list) or not rows:
            if verbose:
                print(f"[done] total_pages={page} total_rows={len(out)}")
            break
        out.extend(rows)
        if verbose:
            print(f"[page {page}] fetched={len(rows)} cursor={cursor!r}")
        page += 1
        if not cursor:
            if verbose: print("[done] cursor empty; end")
            break
        time.sleep(sleep_s)
    return out

# -----------------------------
# CSV Writers / Dedupe
# -----------------------------

def write_csv(rows: List[Dict], out_csv: Path, wide: bool, resume: bool, verbose: bool) -> int:
    os.makedirs(out_csv.parent, exist_ok=True)
    # normalize first to derive columns
    norm = [normalize_op(r) for r in rows]
    df = pd.DataFrame(norm)
    # optional resume dedupe by tx_id (token-wide often uses hashRev)
    if resume and out_csv.exists():
        try:
            prev = pd.read_csv(out_csv)
            if "tx_id" in prev.columns:
                prev_ids = set(prev["tx_id"].dropna().astype(str))
                df = df[~df["tx_id"].astype(str).isin(prev_ids)]
                if verbose:
                    print(f"[resume] skipped {len(prev_ids)} existing tx_ids; new rows={len(df)}")
        except Exception as e:
            if verbose:
                print(f"[resume] couldn’t read prior CSV ({e}); continuing without resume")
    # stable narrow columns
    narrow_cols = ["tx_id","timestamp_raw","timestamp_iso","token","op_type","from","to","amount"]
    df_narrow = df[narrow_cols].copy()
    df_narrow.sort_values(["timestamp_iso","tx_id"], inplace=True)

    if not wide:
        wrote = _append_or_write(df_narrow, out_csv)
        return wrote

    # build wide with raw_* extras
    wide_rows = []
    for r in norm:
        w = {k: v for k, v in r.items() if k != "_raw"}
        raw = r.get("_raw", {})
        if isinstance(raw, dict):
            for k, v in raw.items():
                if k not in w:
                    w[f"raw_{k}"] = v
        wide_rows.append(w)
    df_wide = pd.DataFrame(wide_rows)
    df_wide.sort_values(["timestamp_iso","tx_id"], inplace=True)
    wrote = _append_or_write(df_wide, out_csv)
    return wrote

def _append_or_write(df: pd.DataFrame, out_csv: Path) -> int:
    if out_csv.exists() and out_csv.stat().st_size > 0:
        # append
        df.to_csv(out_csv, mode="a", header=False, index=False)
        return len(df)
    else:
        df.to_csv(out_csv, index=False)
        return len(df)

# -----------------------------
# CLI
# -----------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kasplex KRC-20 tracer (wallet + token-wide)")
    p.add_argument("--mode", choices=["wallet","token"], required=True, help="wallet=oplist by address, token=oplist by tick")
    p.add_argument("--address", help="Kaspa address (required if --mode wallet)")
    p.add_argument("--token", help="Ticker (tick) to filter or fetch (required in token mode; optional in wallet mode)")
    p.add_argument("--base-url", default=KASPLEX_API_DEFAULT, help="Kasplex API base (default: %(default)s)")
    p.add_argument("--out", required=True, type=Path, help="Output CSV path")
    p.add_argument("--limit", type=int, default=500, help="Page size (1..1000)")
    p.add_argument("--sleep", type=float, default=0.25, help="Delay between pages (seconds)")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    p.add_argument("--wide", action="store_true", help="Write wide CSV with raw_* extras")
    p.add_argument("--resume", action="store_true", help="Append mode: skip rows whose tx_id already exists in out CSV")
    p.add_argument("--save-raw", action="store_true", help="Save raw JSON pages (okjson/nonjson/badjson/error_*) next to output file")
    return p.parse_args(argv)

# -----------------------------
# Main
# -----------------------------

def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    base_url = args.base_url.rstrip("/")

    # Determine raw dir
    raw_dir = None
    if args.save_raw:
        stem = args.out if isinstance(args.out, Path) else Path(args.out)
        raw_dir = stem.with_suffix("")
        raw_dir = Path(str(raw_dir) + "_rawpages")

    if args.mode == "token":
        if not args.token:
            raise SystemExit("--token is required in --mode token")
        if args.verbose:
            print(f"[+] Fetching token-wide ops for tick={args.token}")
        rows = fetch_oplist_by_tick(
            tick=args.token,
            base_url=base_url,
            limit=args.limit,
            sleep_s=args.sleep,
            verbose=args.verbose,
            save_raw_dir=raw_dir,
        )
    else:  # wallet
        if not args.address:
            raise SystemExit("--address is required in --mode wallet")
        if args.verbose:
            print(f"[+] Fetching wallet ops for address={args.address} token={args.token or 'ALL'}")
        rows = fetch_oplist_by_address(
            address=args.address.strip(),
            base_url=base_url,
            token=args.token,
            limit=args.limit,
            sleep_s=args.sleep,
            verbose=args.verbose,
            save_raw_dir=raw_dir,
        )

    wrote = write_csv(rows, args.out, wide=args.wide, resume=args.resume, verbose=args.verbose)
    if args.verbose:
        print(f"[write] {args.out} rows_written={wrote}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
