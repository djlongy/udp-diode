#!/usr/bin/env python3
"""Low-side pitcher: file → sequenced UDP frames (one-way; no ACK).

Absorbs industry practice (godiode / UDPcast / svenseeberg):
  - stream large files (no full-file RAM)
  - compact META (chunk_size + last_chunk_len)
  - META/EOF redundancy (default ×3)
  - XOR FEC groups
  - send-side bitrate throttle + SO_SNDBUF
  - optional HMAC on META
"""
from __future__ import annotations

import argparse
import random
import socket
import sys
import time
import uuid
from pathlib import Path

from protocol import (  # noqa: I001
    DEFAULT_CHUNK_SIZE,
    IN_MEMORY_MAX_BYTES,
    Frame,
    FrameType,
    build_meta_header,
    build_transfer_frames,
    file_transfer_stats,
    iter_data_and_parity_from_file,
)


def _configure_send_buf(sock: socket.socket, packet_size: int) -> None:
    """godiode lesson: enlarge send buffer for burst pacing."""
    want = max(256 * 1024, 64 * packet_size)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, want)
    except OSError as exc:
        print(f"warn: SO_SNDBUF={want} failed: {exc}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("path", type=Path, help="File to send")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9400)
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    p.add_argument("--meta-copies", type=int, default=3)
    p.add_argument("--eof-copies", type=int, default=3)
    p.add_argument(
        "--passes",
        type=int,
        default=1,
        help="Full retransmit passes (no ACK path; redundancy only)",
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
        help="If set, sign META with HMAC-SHA256 (must match catcher)",
    )
    p.add_argument(
        "--force-memory",
        action="store_true",
        help="Force in-memory frame build (tests/reorder/corrupt only)",
    )
    args = p.parse_args()

    path = args.path
    if not path.is_file():
        print(f"not a file: {path}", file=sys.stderr)
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

    sock.close()
    print(transfer_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
