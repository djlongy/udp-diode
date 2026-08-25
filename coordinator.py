#!/usr/bin/env python3
"""Same-side coordinator: copy allowlisted receipts between drop dirs.

No sockets. The data track and the validate track never share a directory
and never learn each other's addresses. This process is the only join:
catcher outbox → pitcher inbox, schema stripped to the receipt allowlist.

    high: data-catcher receipts/  →  validate-pitcher inbox/
    low:  validate-catcher receipts/ → data-pitcher inbox/

A host that both sends and receives UDP is the exfil anti-pattern; this
binary does not send or receive UDP at all.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from receipts import allowlist_receipt


def copy_once(src: Path, dst: Path) -> int:
    """Copy new/changed receipt JSON. Returns number of files written."""
    src.mkdir(parents=True, exist_ok=True)
    dst.mkdir(parents=True, exist_ok=True)
    written = 0
    for path in src.glob("*.json"):
        if path.name.endswith(".tmp"):
            continue
        try:
            data = json.loads(path.read_text())
            payload = allowlist_receipt(data)
        except (OSError, json.JSONDecodeError, ValueError, TypeError):
            continue
        dest = dst / path.name
        text = json.dumps(payload, indent=2) + "\n"
        if dest.is_file() and dest.read_text() == text:
            continue
        tmp = dest.with_suffix(".json.tmp")
        tmp.write_text(text)
        tmp.replace(dest)
        written += 1
    return written


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--from-dir", type=Path, required=True)
    p.add_argument("--to-dir", type=Path, required=True)
    p.add_argument("--interval", type=float, default=0.2)
    p.add_argument("--idle-exit", type=float, default=None)
    p.add_argument("--max-seconds", type=float, default=None)
    args = p.parse_args()

    start = time.time()
    last_write = start
    total = 0
    print(
        f"coordinator {args.from_dir} → {args.to_dir}",
        file=sys.stderr,
    )
    try:
        while True:
            now = time.time()
            if args.max_seconds is not None and now - start >= args.max_seconds:
                break
            if (
                args.idle_exit is not None
                and total
                and now - last_write >= args.idle_exit
            ):
                break
            n = copy_once(args.from_dir, args.to_dir)
            if n:
                total += n
                last_write = time.time()
            time.sleep(max(0.05, args.interval))
    except KeyboardInterrupt:
        pass
    print(f"copied={total}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
