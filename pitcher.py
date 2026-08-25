#!/usr/bin/env python3
"""Send-only UDP pitcher: file → sequenced frames, or receipts → STATUS.

Never binds a receive socket. Retransmit is driven by local receipt files
that a same-side coordinator copied from the validate-track catcher.

Absorbs industry practice (godiode / UDPcast / svenseeberg):
  - stream large files (no full-file RAM)
  - compact META (chunk_size + last_chunk_len)
  - META/EOF redundancy (default ×3)
  - XOR FEC groups
  - send-side bitrate throttle + SO_SNDBUF
  - optional HMAC on META / STATUS
"""
from __future__ import annotations

import argparse
import random
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from protocol import (  # noqa: I001
    DEFAULT_CHUNK_SIZE,
    IN_MEMORY_MAX_BYTES,
    STATUS_FAIL,
    STATUS_HOLES,
    STATUS_OK,
    Frame,
    FrameType,
    build_meta_header,
    build_status_frame,
    build_transfer_frames,
    file_transfer_stats,
    iter_data_and_parity_from_file,
    iter_data_seqs_from_file,
)
from receipts import ReceiptStore, read_source_map, write_source_map


def _configure_send_buf(sock: socket.socket, packet_size: int) -> None:
    """godiode lesson: enlarge send buffer for burst pacing."""
    want = max(256 * 1024, 64 * packet_size)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, want)
    except OSError as exc:
        print(f"warn: SO_SNDBUF={want} failed: {exc}", file=sys.stderr)


def _send_frame(
    sock: socket.socket,
    dest: tuple[str, int],
    fr: Frame,
    gap_s: float,
    max_bps: float,
) -> int:
    wire = fr.encode()
    sock.sendto(wire, dest)
    sleep_s = gap_s
    if max_bps > 0:
        sleep_s = max(sleep_s, len(wire) / max_bps)
    if sleep_s > 0:
        time.sleep(sleep_s)
    return len(wire)


def run_validate_pitcher(args: argparse.Namespace) -> int:
    """Send-only: STATUS frames from a local receipt inbox (validate track)."""
    store = ReceiptStore(args.send_receipts)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _configure_send_buf(sock, 512)
    dest = (args.host, args.port)
    secret = args.hmac_secret.encode("utf-8") if args.hmac_secret else None
    gap_s = args.gap_ms / 1000.0
    last_sent: dict[str, str] = {}
    start = time.time()
    last_activity = start
    sent = 0
    print(
        f"validate pitcher receipts={args.send_receipts} dest={dest[0]}:{dest[1]}",
        file=sys.stderr,
    )
    try:
        while True:
            now = time.time()
            if args.max_seconds is not None and now - start >= args.max_seconds:
                break
            if (
                args.idle_exit is not None
                and sent
                and now - last_activity >= args.idle_exit
            ):
                break
            for _path, report in store.list_reports():
                token = f"{report.kind}:{report.have}:{report.missing}"
                if last_sent.get(report.transfer_id) == token:
                    continue
                copies = max(1, args.receipt_copies)
                frame = build_status_frame(report, hmac_secret=secret)
                for _ in range(copies):
                    _send_frame(sock, dest, frame, gap_s, 0.0)
                    sent += 1
                last_sent[report.transfer_id] = token
                last_activity = time.time()
                print(
                    f"status sent transfer_id={report.transfer_id} "
                    f"kind={report.kind} have={report.have} "
                    f"missing={list(report.missing)}",
                    file=sys.stderr,
                )
            time.sleep(max(0.05, args.receipt_poll))
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
    print(f"status_frames_sent={sent}", file=sys.stderr)
    return 0


@dataclass
class ArqJob:
    transfer_id: str
    source: Path
    chunk_size: int
    meta: Frame
    eof: Frame
    receipts: ReceiptStore
    timeout_s: float = 2.0
    rounds: int = 8
    backoff: float = 2.0
    max_timeout_s: float = 30.0
    gap_s: float = 0.0
    max_bps: float = 0.0
    meta_copies: int = 3
    eof_copies: int = 3


def watch_receipts_and_resend(
    sock: socket.socket, dest: tuple[str, int], job: ArqJob
) -> int:
    """Poll local receipts (validate catcher → coordinator). Send-only."""
    wait = job.timeout_s
    last_token = ""
    for round_i in range(max(1, job.rounds)):
        deadline = time.time() + wait
        report = None
        while time.time() < deadline:
            report = job.receipts.read(job.transfer_id)
            if report is not None:
                break
            time.sleep(0.05)
        if report is None:
            print(
                f"arq round={round_i + 1}/{job.rounds} no receipt; "
                f"nudge meta/eof wait={wait:.1f}s",
                file=sys.stderr,
            )
            for _ in range(job.meta_copies):
                _send_frame(sock, dest, job.meta, job.gap_s, job.max_bps)
            for _ in range(job.eof_copies):
                _send_frame(sock, dest, job.eof, job.gap_s, job.max_bps)
            wait = min(wait * job.backoff, job.max_timeout_s)
            continue
        if report.kind == STATUS_OK:
            print(f"arq ok transfer_id={job.transfer_id}", file=sys.stderr)
            return 0
        if report.kind == STATUS_FAIL:
            print(
                f"arq fail transfer_id={job.transfer_id} reason={report.reason}",
                file=sys.stderr,
            )
            return 2
        if report.kind != STATUS_HOLES:
            wait = min(wait * job.backoff, job.max_timeout_s)
            continue
        token = f"{report.kind}:{report.have}:{report.missing}"
        if token == last_token:
            time.sleep(wait)
            wait = min(wait * job.backoff, job.max_timeout_s)
            continue
        last_token = token
        missing = list(report.missing)
        print(
            f"arq round={round_i + 1}/{job.rounds} holes={missing} have={report.have}",
            file=sys.stderr,
        )
        for _ in range(job.meta_copies):
            _send_frame(sock, dest, job.meta, job.gap_s, job.max_bps)
        for fr in iter_data_seqs_from_file(
            job.source, job.transfer_id, missing, job.chunk_size
        ):
            _send_frame(sock, dest, fr, job.gap_s, job.max_bps)
        for _ in range(job.eof_copies):
            _send_frame(sock, dest, job.eof, job.gap_s, job.max_bps)
        time.sleep(wait)
        wait = job.timeout_s
    print(f"arq exhausted transfer_id={job.transfer_id}", file=sys.stderr)
    return 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "path",
        type=Path,
        nargs="?",
        default=None,
        help="File to send (data track). Omit with --send-receipts.",
    )
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9400)
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    p.add_argument("--meta-copies", type=int, default=3)
    p.add_argument("--eof-copies", type=int, default=3)
    p.add_argument(
        "--passes",
        type=int,
        default=1,
        help="Full retransmit passes (data track redundancy; validate-track ARQ is --watch-receipts)",
    )
    p.add_argument("--loss", type=float, default=0.0, help="Probability of dropping a DATA frame")
    p.add_argument(
        "--reorder",
        action="store_true",
        help="Shuffle DATA before send (in-memory path only; small files)",
    )
    p.add_argument(
        "--corrupt",
        action="store_true",
        help="Flip a byte in one DATA body (wire CRC still valid; whole-payload sha256 fails)",
    )
    p.add_argument(
        "--wire-corrupt",
        action="store_true",
        help="Corrupt one encoded datagram so catcher CRC rejects it",
    )
    p.add_argument(
        "--parity-group",
        type=int,
        default=0,
        help="XOR FEC group size (>1 enables PARITY frames; recover 1 loss per group)",
    )
    p.add_argument(
        "--drop-seq",
        type=int,
        default=-1,
        help="Deterministically drop DATA with this seq (for FEC demos)",
    )
    p.add_argument("--gap-ms", type=float, default=0.0, help="Minimum inter-datagram sleep (ms)")
    p.add_argument(
        "--max-bitrate",
        type=float,
        default=0.0,
        help=(
            "Throttle send rate to X Mbit/s (wire size). "
            "godiode/UDPcast: unthrottled blasts self-inflict loss on WAN. 0 = gap only."
        ),
    )
    p.add_argument(
        "--progress-every-mb",
        type=float,
        default=32.0,
        help="Log progress every N MiB of DATA body (0=off)",
    )
    p.add_argument("--transfer-id", default="")
    p.add_argument(
        "--hmac-secret",
        default="",
        help="If set, sign META/STATUS with HMAC-SHA256 (must match the catcher on this track)",
    )
    p.add_argument(
        "--force-memory",
        action="store_true",
        help="Force in-memory frame build (tests/reorder/corrupt only)",
    )
    p.add_argument(
        "--send-receipts",
        type=Path,
        default=None,
        help="Validate track: send STATUS from this inbox. Send-only; no file path.",
    )
    p.add_argument("--receipt-copies", type=int, default=3)
    p.add_argument("--receipt-poll", type=float, default=0.2)
    p.add_argument("--idle-exit", type=float, default=None)
    p.add_argument("--max-seconds", type=float, default=None)
    p.add_argument(
        "--watch-receipts",
        type=Path,
        default=None,
        help="Data track: after send, poll this inbox for holes/ok/fail and resend. Send-only.",
    )
    p.add_argument(
        "--source-map",
        type=Path,
        default=None,
        help="Directory for {transfer_id}.json mapping to the source file (data track).",
    )
    p.add_argument("--ack-timeout-s", type=float, default=2.0)
    p.add_argument("--ack-rounds", type=int, default=8)
    p.add_argument("--ack-backoff", type=float, default=2.0)
    p.add_argument("--ack-timeout-cap-s", type=float, default=30.0)
    args = p.parse_args()

    if args.send_receipts is not None:
        return run_validate_pitcher(args)

    path = args.path
    if path is None or not path.is_file():
        print("data track requires a file path", file=sys.stderr)
        return 2

    size = path.stat().st_size
    use_memory = args.force_memory or args.reorder or args.corrupt or size <= IN_MEMORY_MAX_BYTES
    if args.reorder and size > IN_MEMORY_MAX_BYTES and not args.force_memory:
        print(
            "error: --reorder requires in-memory path; use smaller file or --force-memory",
            file=sys.stderr,
        )
        return 2

    transfer_id = args.transfer_id or str(uuid.uuid4())
    secret = args.hmac_secret.encode("utf-8") if args.hmac_secret else None
    if args.source_map is not None:
        write_source_map(
            args.source_map,
            transfer_id,
            path,
            args.chunk_size,
            args.parity_group,
        )
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _configure_send_buf(sock, args.chunk_size + 256)
    dest = (args.host, args.port)
    sent = dropped = 0
    wire_corrupt_done = False
    max_bps = (args.max_bitrate * 1_000_000 / 8.0) if args.max_bitrate > 0 else 0.0
    progress_every = int(args.progress_every_mb * 1024 * 1024) if args.progress_every_mb > 0 else 0
    data_bytes_sent = 0
    next_progress = progress_every

    def _send(fr: Frame) -> None:
        nonlocal sent, wire_corrupt_done, data_bytes_sent, next_progress
        wire = fr.encode()
        if args.wire_corrupt and not wire_corrupt_done and fr.type == FrameType.DATA:
            wire = bytearray(wire)
            wire[-5] ^= 0xFF
            wire = bytes(wire)
            wire_corrupt_done = True
        sock.sendto(wire, dest)
        sent += 1
        if fr.type == FrameType.DATA:
            data_bytes_sent += len(fr.body)
            if progress_every and data_bytes_sent >= next_progress:
                print(
                    f"progress data_bytes={data_bytes_sent} sent_frames={sent}",
                    file=sys.stderr,
                )
                next_progress += progress_every
        sleep_s = args.gap_ms / 1000.0
        if max_bps > 0:
            sleep_s = max(sleep_s, len(wire) / max_bps)
        if sleep_s > 0:
            time.sleep(sleep_s)

    if use_memory:
        payload = path.read_bytes()
        frames = build_transfer_frames(
            transfer_id,
            payload,
            chunk_size=args.chunk_size,
            parity_group=args.parity_group,
            hmac_secret=secret,
        )
        meta = next(f for f in frames if f.type is FrameType.META)
        data = [f for f in frames if f.type is FrameType.DATA]
        parity = [f for f in frames if f.type is FrameType.PARITY]
        eof = next(f for f in frames if f.type is FrameType.EOF)

        if args.reorder:
            random.shuffle(data)
        if args.corrupt and data:
            victim = random.randrange(len(data))
            body = bytearray(data[victim].body)
            if body:
                body[0] ^= 0xFF
            data[victim] = Frame(
                type=FrameType.DATA, header=data[victim].header, body=bytes(body)
            )

        total_len = len(payload)
        total_chunks = len(data)
        for pass_i in range(args.passes):
            for _ in range(args.meta_copies):
                _send(meta)
            for fr in data:
                seq = int(fr.header["seq"])
                if args.drop_seq >= 0 and seq == args.drop_seq:
                    dropped += 1
                    continue
                if args.loss > 0 and random.random() < args.loss:
                    dropped += 1
                    continue
                _send(fr)
            for fr in parity:
                _send(fr)
            for _ in range(args.eof_copies):
                _send(eof)
            print(
                f"pass={pass_i + 1}/{args.passes} transfer_id={transfer_id} "
                f"bytes={total_len} chunks={total_chunks} parity={len(parity)} "
                f"sent={sent} dropped_data={dropped} mode=memory",
                file=sys.stderr,
            )
    else:
        # Streaming path (multi‑GB safe): hash pass, then send passes re-read file.
        digest, total_len, total_chunks, last_len = file_transfer_stats(
            path, args.chunk_size
        )
        meta = Frame(
            type=FrameType.META,
            header=build_meta_header(
                transfer_id,
                total_len,
                total_chunks,
                digest,
                args.chunk_size,
                last_len,
                parity_group=args.parity_group,
                hmac_secret=secret,
            ),
        )
        eof = Frame(
            type=FrameType.EOF,
            header={
                "transfer_id": transfer_id,
                "total_chunks": total_chunks,
                "sha256": digest,
                "total_len": total_len,
            },
        )
        print(
            f"stream prep transfer_id={transfer_id} bytes={total_len} "
            f"chunks={total_chunks} sha256={digest}",
            file=sys.stderr,
        )
        for pass_i in range(args.passes):
            data_bytes_sent = 0
            next_progress = progress_every
            for _ in range(args.meta_copies):
                _send(meta)
            parity_count = 0
            for fr in iter_data_and_parity_from_file(
                path, transfer_id, args.chunk_size, args.parity_group
            ):
                if fr.type is FrameType.DATA:
                    seq = int(fr.header["seq"])
                    if args.drop_seq >= 0 and seq == args.drop_seq:
                        dropped += 1
                        continue
                    if args.loss > 0 and random.random() < args.loss:
                        dropped += 1
                        continue
                    if args.wire_corrupt and not wire_corrupt_done:
                        # handled inside _send once
                        pass
                    _send(fr)
                else:
                    parity_count += 1
                    _send(fr)
            for _ in range(args.eof_copies):
                _send(eof)
            print(
                f"pass={pass_i + 1}/{args.passes} transfer_id={transfer_id} "
                f"bytes={total_len} chunks={total_chunks} parity≈{parity_count} "
                f"sent={sent} dropped_data={dropped} mode=stream",
                file=sys.stderr,
            )

    if args.watch_receipts is not None:
        src = path
        chunk_size = args.chunk_size
        mapped = None
        if args.source_map is not None:
            mapped = read_source_map(args.source_map, transfer_id)
        if mapped is not None:
            src = mapped["path"]
            chunk_size = int(mapped["chunk_size"])
        rc = watch_receipts_and_resend(
            sock,
            dest,
            ArqJob(
                transfer_id=transfer_id,
                source=src,
                chunk_size=chunk_size,
                meta=meta,
                eof=eof,
                receipts=ReceiptStore(args.watch_receipts),
                timeout_s=args.ack_timeout_s,
                rounds=args.ack_rounds,
                backoff=args.ack_backoff,
                max_timeout_s=args.ack_timeout_cap_s,
                gap_s=args.gap_ms / 1000.0,
                max_bps=max_bps,
                meta_copies=args.meta_copies,
                eof_copies=args.eof_copies,
            ),
        )
        sock.close()
        print(transfer_id)
        return rc

    sock.close()
    print(transfer_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
