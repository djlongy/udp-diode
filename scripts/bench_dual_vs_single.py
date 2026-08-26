#!/usr/bin/env python3
"""Loopback comparison: single-track multi-pass vs dual-track hole fill.

Measures wall time until the data pitcher exits and whether the catcher
published a sha256-matching file. Induced DATA loss is pitcher-side
(--loss). No WAN, no throttle — this isolates protocol overhead.

    python3 scripts/bench_dual_vs_single.py
"""
from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable
SIZE_MB = 8
REPS = 3
LOSSES = (0.0, 0.08, 0.15)
CHUNK = 1200
ACK_TIMEOUT = 0.25


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def kill_tree(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        if proc.poll() is not None:
            continue
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass
    time.sleep(0.15)
    for proc in procs:
        if proc.poll() is not None:
            continue
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass


def spawn(cmd: list[str], log: Path) -> subprocess.Popen:
    log.parent.mkdir(parents=True, exist_ok=True)
    fh = log.open("w")
    return subprocess.Popen(
        cmd,
        stdout=fh,
        stderr=fh,
        start_new_session=True,
    )


def published_ok(out: Path, want: str) -> bool:
    bins = [p for p in out.glob("*.bin") if not p.name.endswith(".part")]
    if len(bins) != 1:
        return False
    return sha256_file(bins[0]) == want


def parse_sent(log: Path) -> int | None:
    last = None
    try:
        text = log.read_text()
    except OSError:
        return None
    for line in text.splitlines():
        if "sent=" in line:
            for part in line.split():
                if part.startswith("sent="):
                    try:
                        last = int(part.split("=", 1)[1])
                    except ValueError:
                        pass
    return last


def run_single(
    work: Path, src: Path, want: str, port: int, *, passes: int, parity: int, loss: float
) -> dict:
    out = work / "out"
    quar = work / "q"
    for d in (out, quar):
        if d.exists():
            subprocess.check_call(["rm", "-rf", str(d)])
        d.mkdir(parents=True)
    catcher = spawn(
        [
            PY, str(ROOT / "catcher.py"),
            "--bind", "127.0.0.1", "--port", str(port),
            "--out", str(out), "--quarantine", str(quar),
            "--ttl", "20", "--idle-exit", "2", "--max-seconds", "60",
        ],
        work / "catcher.log",
    )
    time.sleep(0.25)
    t0 = time.perf_counter()
    pitcher = subprocess.run(
        [
            PY, str(ROOT / "pitcher.py"), str(src),
            "--host", "127.0.0.1", "--port", str(port),
            "--chunk-size", str(CHUNK),
            "--passes", str(passes),
            "--parity-group", str(parity),
            "--loss", str(loss),
            "--progress-every-mb", "0",
        ],
        stdout=subprocess.PIPE,
        stderr=(work / "pitcher.log").open("w"),
        text=True,
        check=False,
    )
    wall = time.perf_counter() - t0
    try:
        catcher.wait(timeout=8)
    except subprocess.TimeoutExpired:
        kill_tree([catcher])
    ok = published_ok(out, want)
    mbit = (SIZE_MB * 8) / wall if wall > 0 and ok else 0.0
    return {
        "ok": ok,
        "wall_s": round(wall, 3),
        "mbit_s": round(mbit, 2),
        "pitcher_rc": pitcher.returncode,
        "frames_sent": parse_sent(work / "pitcher.log"),
    }


def run_dual(
    work: Path, src: Path, want: str, data_port: int, val_port: int, *, parity: int, loss: float
) -> dict:
    high_out = work / "high_out"
    high_in = work / "high_in"
    low_out = work / "low_out"
    low_in = work / "low_in"
    sources = work / "sources"
    out = work / "out"
    quar = work / "q"
    val_out = work / "val_out"
    val_q = work / "val_q"
    for d in (high_out, high_in, low_out, low_in, sources, out, quar, val_out, val_q):
        if d.exists():
            subprocess.check_call(["rm", "-rf", str(d)])
        d.mkdir(parents=True)

    procs: list[subprocess.Popen] = []
    try:
        procs.append(spawn(
            [
                PY, str(ROOT / "catcher.py"),
                "--bind", "127.0.0.1", "--port", str(data_port),
                "--out", str(out), "--quarantine", str(quar),
                "--ttl", "20", "--idle-exit", "3", "--max-seconds", "60",
                "--receipts", str(high_out), "--receipt-every", "0",
            ],
            work / "data.catcher.log",
        ))
        procs.append(spawn(
            [
                PY, str(ROOT / "catcher.py"),
                "--bind", "127.0.0.1", "--port", str(val_port),
                "--out", str(val_out), "--quarantine", str(val_q),
                "--ttl", "20", "--idle-exit", "3", "--max-seconds", "60",
                "--receipts", str(low_out), "--status-only",
            ],
            work / "val.catcher.log",
        ))
        procs.append(spawn(
            [
                PY, str(ROOT / "coordinator.py"),
                "--from-dir", str(high_out), "--to-dir", str(high_in),
                "--interval", "0.05", "--max-seconds", "60",
            ],
            work / "high.coord.log",
        ))
        procs.append(spawn(
            [
                PY, str(ROOT / "coordinator.py"),
                "--from-dir", str(low_out), "--to-dir", str(low_in),
                "--interval", "0.05", "--max-seconds", "60",
            ],
            work / "low.coord.log",
        ))
        procs.append(spawn(
            [
                PY, str(ROOT / "pitcher.py"),
                "--send-receipts", str(high_in),
                "--host", "127.0.0.1", "--port", str(val_port),
                "--receipt-copies", "2", "--receipt-poll", "0.05",
                "--max-seconds", "60", "--idle-exit", "3",
            ],
            work / "val.pitcher.log",
        ))
        time.sleep(0.4)
        t0 = time.perf_counter()
        pitcher = subprocess.run(
            [
                PY, str(ROOT / "pitcher.py"), str(src),
                "--host", "127.0.0.1", "--port", str(data_port),
                "--chunk-size", str(CHUNK),
                "--passes", "1",
                "--parity-group", str(parity),
                "--loss", str(loss),
                "--watch-receipts", str(low_in),
                "--source-map", str(sources),
                "--ack-timeout-s", str(ACK_TIMEOUT),
                "--ack-rounds", "16",
                "--ack-backoff", "1.4",
                "--progress-every-mb", "0",
            ],
            stdout=subprocess.PIPE,
            stderr=(work / "pitcher.log").open("w"),
            text=True,
            check=False,
        )
        wall = time.perf_counter() - t0
    finally:
        kill_tree(procs)

    ok = published_ok(out, want)
    mbit = (SIZE_MB * 8) / wall if wall > 0 and ok else 0.0
    return {
        "ok": ok,
        "wall_s": round(wall, 3),
        "mbit_s": round(mbit, 2),
        "pitcher_rc": pitcher.returncode,
        "frames_sent": parse_sent(work / "pitcher.log"),
    }


def median(vals: list[float]) -> float:
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def main() -> int:
    work_root = Path("/tmp/udp_diode_dual_bench")
    work_root.mkdir(parents=True, exist_ok=True)
    src = work_root / f"src_{SIZE_MB}mb.bin"
    if not src.is_file() or src.stat().st_size != SIZE_MB * 1024 * 1024:
        subprocess.check_call(
            ["dd", "if=/dev/urandom", f"of={src}", "bs=1m", f"count={SIZE_MB}", "status=none"]
        )
    want = sha256_file(src)
    modes = (
        ("single_p1", "single", {"passes": 1, "parity": 0}),
        ("single_p2_fec4", "single", {"passes": 2, "parity": 4}),
        ("dual_p1", "dual", {"parity": 0}),
        ("dual_p1_fec4", "dual", {"parity": 4}),
    )
    rows: list[dict] = []
    port = 19100
    for loss in LOSSES:
        for name, kind, kw in modes:
            walls: list[float] = []
            mbits: list[float] = []
            oks = 0
            frames: list[int] = []
            for rep in range(REPS):
                case = work_root / f"{name}_loss{loss}_r{rep}"
                case.mkdir(parents=True, exist_ok=True)
                if kind == "single":
                    result = run_single(
                        case, src, want, port, loss=loss, **kw
                    )
                    port += 1
                else:
                    result = run_dual(
                        case, src, want, port, port + 1, loss=loss, **kw
                    )
                    port += 2
                print(
                    f"{name} loss={loss} rep={rep} ok={result['ok']} "
                    f"{result['wall_s']}s {result['mbit_s']} Mbit/s "
                    f"sent={result['frames_sent']}",
                    flush=True,
                )
                if result["ok"]:
                    oks += 1
                    walls.append(result["wall_s"])
                    mbits.append(result["mbit_s"])
                if result["frames_sent"] is not None:
                    frames.append(result["frames_sent"])
            rows.append({
                "mode": name,
                "loss": loss,
                "ok": f"{oks}/{REPS}",
                "ok_n": oks,
                "wall_s_med": round(median(walls), 3) if walls else None,
                "mbit_s_med": round(median(mbits), 2) if mbits else None,
                "frames_sent_med": int(median([float(x) for x in frames])) if frames else None,
            })
    out = {
        "size_mb": SIZE_MB,
        "reps": REPS,
        "chunk": CHUNK,
        "ack_timeout_s": ACK_TIMEOUT,
        "path": "loopback 127.0.0.1, induced pitcher --loss, no bitrate cap",
        "rows": rows,
    }
    dest = ROOT / "results" / "dual-vs-single.json"
    dest.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
