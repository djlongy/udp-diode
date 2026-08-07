#!/usr/bin/env python3
"""Size-ladder benchmark for diode_udp_sim (local or orchestrated externally).

Emits JSON lines + summary JSON with throughput, frames, FEC, errors.
Used by ``bench_cross_site.sh`` and chart generation.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def _mb(n: int) -> float:
    return n / (1024 * 1024)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sizes-mb", default="1,4,16,64,128,256,512")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9400)
    p.add_argument("--chunk-size", type=int, default=1200)
    p.add_argument("--parity-group", type=int, default=4)
    p.add_argument("--passes", type=int, default=1)
    p.add_argument("--max-bitrate", type=float, default=0.0)
    p.add_argument("--loss", type=float, default=0.0, help="Artificial DATA loss on pitcher")
    p.add_argument("--hmac-secret", default="")
    p.add_argument("--workdir", type=Path, default=Path("/tmp/diode_bench"))
    p.add_argument("--label", default="bench")
    p.add_argument("--catcher-ttl", type=float, default=7200)
    p.add_argument("--results", type=Path, default=Path("/tmp/diode_bench_results.json"))
    args = p.parse_args()

    root = Path(__file__).resolve().parent
    py = sys.executable
    work = args.workdir
    work.mkdir(parents=True, exist_ok=True)
    out = work / "out"
    quar = work / "q"
    wdir = work / "work"
    sizes = [int(x.strip()) for x in args.sizes_mb.split(",") if x.strip()]

    rows: list[dict] = []

    for size_mb in sizes:
        size_b = size_mb * 1024 * 1024
        case = f"{args.label}_{size_mb}mb"
        src = work / f"src_{size_mb}mb.bin"
        print(f"\n=== {case} ({size_b} bytes) ===", flush=True)

        # Create source with sparse-ish random (fast enough)
        if not src.is_file() or src.stat().st_size != size_b:
            print(f"generating {src} ...", flush=True)
            # use dd via shell for speed
            subprocess.check_call(
                ["dd", "if=/dev/urandom", f"of={src}", "bs=1M", f"count={size_mb}", "status=none"]
            )

        # Clean dirs
        for d in (out, quar, wdir):
            if d.exists():
                subprocess.check_call(["rm", "-rf", str(d)])
            d.mkdir(parents=True, exist_ok=True)

        # Free port if needed
        subprocess.call(
            f"fuser -k {args.port}/udp 2>/dev/null || true",
            shell=True,
        )
        time.sleep(0.3)

        catcher_log = work / f"{case}.catcher.log"
        stats_path = work / f"{case}.stats.json"
        pitcher_log = work / f"{case}.pitcher.log"

        # Expected DATA chunks
        total_chunks = max(1, math.ceil(size_b / args.chunk_size))
        # timeout: size_mb / (bitrate/8) + slack; if no bitrate assume 25 MB/s
        if args.max_bitrate > 0:
            est_s = (size_b * 8 / (args.max_bitrate * 1_000_000)) * args.passes * 1.5 + 60
        else:
            est_s = (size_mb / 25.0) * args.passes * 2 + 90
        max_seconds = max(120.0, est_s)

        catcher_cmd = [
            py,
            str(root / "catcher.py"),
            "--bind",
            "0.0.0.0" if args.host not in ("127.0.0.1", "localhost") else "127.0.0.1",
            "--port",
            str(args.port),
            "--out",
            str(out),
            "--quarantine",
            str(quar),
            "--work",
            str(wdir),
            "--ttl",
            str(args.catcher_ttl),
            "--max-seconds",
            str(int(max_seconds)),
            "--idle-exit",
            "8",
        ]
        if args.hmac_secret:
            catcher_cmd.extend(["--hmac-secret", args.hmac_secret])

        # When host is remote, this script runs only on catcher side for stats —
        # full cross-site is driven by shell. Local mode runs both.
        local_both = args.host in ("127.0.0.1", "localhost")

        t0 = time.perf_counter()
        cproc = None
        if local_both:
            with catcher_log.open("w") as cl, stats_path.open("w") as _:
                cproc = subprocess.Popen(
                    catcher_cmd,
                    stdout=stats_path.open("w"),
                    stderr=cl,
                )
            time.sleep(0.5)

        pitcher_cmd = [
            py,
            str(root / "pitcher.py"),
            str(src),
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--chunk-size",
            str(args.chunk_size),
            "--parity-group",
            str(args.parity_group),
            "--passes",
            str(args.passes),
            "--progress-every-mb",
            "32",
        ]
        if args.max_bitrate > 0:
            pitcher_cmd.extend(["--max-bitrate", str(args.max_bitrate)])
        if args.loss > 0:
            pitcher_cmd.extend(["--loss", str(args.loss)])
        if args.hmac_secret:
            pitcher_cmd.extend(["--hmac-secret", args.hmac_secret])

        with pitcher_log.open("w") as pl:
            pproc = subprocess.run(pitcher_cmd, stdout=subprocess.PIPE, stderr=pl, text=True)
        t_pitch_end = time.perf_counter()

        if cproc is not None:
            try:
                cproc.wait(timeout=max(30, max_seconds - (t_pitch_end - t0)))
            except subprocess.TimeoutExpired:
                cproc.kill()
                cproc.wait()
        t1 = time.perf_counter()
        wall_s = t1 - t0
        pitch_s = t_pitch_end - t0

        # Parse catcher stats (last JSON object in stdout file)
        stats: dict = {}
        if stats_path.is_file():
            text = stats_path.read_text().strip()
            # catcher prints JSON at end to stdout
            try:
                # may have only JSON
                stats = json.loads(text) if text.startswith("{") else {}
            except json.JSONDecodeError:
                # find last {
                i = text.rfind("{")
                if i >= 0:
                    try:
                        stats = json.loads(text[i:])
                    except json.JSONDecodeError:
                        stats = {}

        # Parse pitcher log for sent/dropped
        plog = pitcher_log.read_text() if pitcher_log.is_file() else ""
        sent = dropped = 0
        mode = "unknown"
        for line in plog.splitlines():
            if "sent=" in line and "dropped_data=" in line:
                # pass=... sent=N dropped_data=M
                for part in line.split():
                    if part.startswith("sent="):
                        sent = int(part.split("=", 1)[1])
                    if part.startswith("dropped_data="):
                        dropped = int(part.split("=", 1)[1])
                    if part.startswith("mode="):
                        mode = part.split("=", 1)[1]

        published_files = list(out.glob("*.bin"))
        published = len(published_files) >= 1
        sha_match = False
        if published:
            import hashlib

            src_h = hashlib.sha256(src.read_bytes() if size_b <= 64 * 1024 * 1024 else b"")
            if size_b > 64 * 1024 * 1024:
                h = hashlib.sha256()
                with src.open("rb") as f:
                    while True:
                        b = f.read(1024 * 1024)
                        if not b:
                            break
                        h.update(b)
                src_digest = h.hexdigest()
            else:
                src_digest = src_h.hexdigest()
            out_path = published_files[0]
            h2 = hashlib.sha256()
            with out_path.open("rb") as f:
                while True:
                    b = f.read(1024 * 1024)
                    if not b:
                        break
                    h2.update(b)
            sha_match = h2.hexdigest() == src_digest

        data_frames = int(stats.get("data_frames", 0))
        # Unique DATA seqs needed = total_chunks; received data_frames may include dups from multipass
        # Natural loss estimate: if passes=1, max(0, 1 - unique/total). Without unique count,
        # use min(data_frames, total_chunks)/total_chunks for fill ratio after FEC.
        # Better: if published, loss recovered = 0 effective; report wire_loss approx from passes=1.
        fill_ratio = min(1.0, data_frames / total_chunks) if total_chunks else 0.0
        # intentional artificial loss from pitcher
        intentional_loss_pct = args.loss * 100.0
        # crude wire miss if single pass: not enough data_frames before FEC
        wire_coverage_pct = min(100.0, 100.0 * data_frames / max(1, total_chunks * args.passes))

        thr_mbit = (size_b * 8 / pitch_s / 1_000_000) if pitch_s > 0 else 0.0
        thr_wall = (size_b * 8 / wall_s / 1_000_000) if wall_s > 0 else 0.0

        row = {
            "label": args.label,
            "size_mb": size_mb,
            "size_bytes": size_b,
            "chunk_size": args.chunk_size,
            "total_chunks": total_chunks,
            "parity_group": args.parity_group,
            "passes": args.passes,
            "max_bitrate_mbit": args.max_bitrate,
            "artificial_loss": args.loss,
            "intentional_loss_pct": round(intentional_loss_pct, 2),
            "duration_pitch_s": round(pitch_s, 3),
            "duration_wall_s": round(wall_s, 3),
            "duration_catcher_s": stats.get("last_duration_s", 0),
            "throughput_pitch_mbit_s": round(thr_mbit, 3),
            "throughput_wall_mbit_s": round(thr_wall, 3),
            "throughput_mib_s": round(size_mb / pitch_s, 3) if pitch_s > 0 else 0.0,
            "published": published or int(stats.get("published", 0)) >= 1,
            "sha_match": sha_match,
            "pitcher_sent_frames": sent,
            "pitcher_dropped_data": dropped,
            "pitcher_mode": mode,
            "pitcher_rc": pproc.returncode,
            "catcher_datagrams": stats.get("datagrams", 0),
            "catcher_data_frames": data_frames,
            "catcher_parity_frames": stats.get("parity_frames", 0),
            "catcher_meta_frames": stats.get("meta_frames", 0),
            "catcher_eof_frames": stats.get("eof_frames", 0),
            "catcher_crc_fail": stats.get("crc_fail", 0),
            "catcher_hmac_fail": stats.get("hmac_fail", 0),
            "catcher_fec_recovered": stats.get("fec_recovered", 0),
            "catcher_duplicates": stats.get("duplicates", 0),
            "catcher_quarantined": stats.get("quarantined", 0),
            "catcher_published": stats.get("published", 0),
            "data_frame_fill_ratio": round(fill_ratio, 4),
            "wire_coverage_pct": round(wire_coverage_pct, 2),
            "errors": int(stats.get("crc_fail", 0))
            + int(stats.get("hmac_fail", 0))
            + (0 if (published or int(stats.get("published", 0))) else 1),
            "ok": bool(published and sha_match),
        }
        rows.append(row)
        print(json.dumps(row, indent=2), flush=True)

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": args.host,
        "port": args.port,
        "rows": rows,
    }
    args.results.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nWrote {args.results}", flush=True)
    return 0 if all(r.get("ok") for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
