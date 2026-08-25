#!/usr/bin/env python3
"""High-side catcher: UDP frames → reassemble → output or quarantine.

Absorbs industry practice (godiode / svenseeberg / UDPcast):
  - single-file seek assembly (multi‑GB without 2× RAM or per-chunk inodes)
  - compact META (chunk_size + last_chunk_len)
  - large SO_RCVBUF
  - stream hash + atomic .part→final publish
  - fail closed: incomplete/TTL/integrity → quarantine
  - optional META HMAC
"""
from __future__ import annotations

import argparse
import hashlib
import json
import mmap
import os
import shutil
import socket
import sys
import time
from array import array
from dataclasses import dataclass, field
from pathlib import Path

from protocol import (
    DEFAULT_CHUNK_SIZE,
    STATUS_FAIL,
    STATUS_HOLES,
    STATUS_OK,
    STATUS_MISSING_CAP,
    FrameType,
    ProtocolError,
    decode_frame,
    expected_chunk_len,
    recover_missing_chunk,
    StatusReport,
    status_report_from_frame,
    verify_meta_hmac,
    verify_status_hmac,
)
from receipts import FAIL_REASONS, ReceiptStore

QUARANTINE_PAYLOAD_MAX = 64 * 1024 * 1024
# Prefer pure-RAM assembly below this size. Lab catchers have multi-GB free;
# staying off mmap/disk for ≤512 MiB avoids page-fault writeback that starves
# the UDP receive path (~20% loss observed at 128 MiB with seek/mmap spill).
RAM_MAX_BYTES = 640 * 1024 * 1024


@dataclass
class Assembly:
    transfer_id: str
    total_len: int | None = None
    total_chunks: int | None = None
    sha256: str | None = None
    chunk_size: int = DEFAULT_CHUNK_SIZE
    last_chunk_len: int = 0
    # RAM path
    chunks: dict[int, bytes] | None = field(default_factory=dict)
    # Disk path: one mmap'd file + presence bitmap (avoids seek-per-packet)
    work_dir: Path | None = None
    data_path: Path | None = None
    present: array | None = None  # 'B' 0/1 per seq
    have: int = 0
    # keyed by frozenset(group_seqs) or (seq_start, seq_end)
    parities: dict[tuple[int, int], dict] = field(default_factory=dict)
    first_seen: float = field(default_factory=time.time)
    complete: bool = False
    failed: bool = False
    disk: bool = False
    data_fh: object | None = None
    mm: mmap.mmap | None = None

    def has_seq(self, seq: int) -> bool:
        if self.disk and self.present is not None:
            return bool(self.present[seq])
        assert self.chunks is not None
        return seq in self.chunks

    def get_seq(self, seq: int) -> bytes | None:
        if self.disk:
            if self.present is None or not self.present[seq]:
                return None
            assert self.mm is not None and self.total_chunks is not None
            ln = expected_chunk_len(
                self.chunk_size, self.last_chunk_len, int(self.total_chunks), seq
            )
            off = seq * self.chunk_size
            return bytes(self.mm[off : off + ln])
        assert self.chunks is not None
        return self.chunks.get(seq)

    def put_seq(self, seq: int, body: bytes) -> None:
        if self.disk:
            assert self.mm is not None and self.present is not None
            if self.present[seq]:
                return
            off = seq * self.chunk_size
            end = off + len(body)
            self.mm[off:end] = body
            # pad remainder of fixed slot so stream-hash reads are length-correct
            if len(body) < self.chunk_size:
                pad = self.chunk_size - len(body)
                self.mm[end : end + pad] = b"\x00" * pad
            self.present[seq] = 1
            self.have += 1
            return
        assert self.chunks is not None
        if seq not in self.chunks:
            self.have += 1
        self.chunks[seq] = body

    def seq_count(self) -> int:
        return self.have

    def missing_prefix(self, cap: int = STATUS_MISSING_CAP) -> list[int]:
        if self.total_chunks is None:
            return []
        out: list[int] = []
        for i in range(int(self.total_chunks)):
            if not self.has_seq(i):
                out.append(i)
                if len(out) >= cap:
                    break
        return out

    def enable_disk(self, base: Path) -> None:
        if self.disk:
            return
        if self.total_chunks is None or self.total_chunks <= 0:
            return
        self.work_dir = base / self.transfer_id
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.data_path = self.work_dir / "slots.bin"
        n = int(self.total_chunks)
        slot_bytes = n * self.chunk_size
        # Pre-size + mmap: random put/get without per-packet lseek syscalls.
        self.data_fh = open(self.data_path, "w+b")  # noqa: SIM115
        self.data_fh.truncate(slot_bytes)
        self.data_fh.flush()
        self.mm = mmap.mmap(self.data_fh.fileno(), slot_bytes)
        self.present = array("B", [0]) * n
        # migrate RAM chunks
        if self.chunks:
            for seq, body in self.chunks.items():
                off = seq * self.chunk_size
                self.mm[off : off + len(body)] = body
                if len(body) < self.chunk_size:
                    pad = self.chunk_size - len(body)
                    self.mm[off + len(body) : off + len(body) + pad] = b"\x00" * pad
                self.present[seq] = 1
            self.have = sum(self.present)
        self.chunks = None
        self.disk = True

    def close(self) -> None:
        if self.mm is not None:
            try:
                self.mm.flush()
                self.mm.close()
            except (OSError, BufferError, ValueError):
                pass
            self.mm = None
        if self.data_fh is not None:
            try:
                self.data_fh.close()
            except OSError:
                pass
            self.data_fh = None


class Catcher:
    def __init__(
        self,
        out_dir: Path,
        quarantine_dir: Path,
        ttl_s: float,
        idle_exit_s: float | None,
        hmac_secret: bytes | None = None,
        work_dir: Path | None = None,
        disk_threshold_bytes: int = RAM_MAX_BYTES,
    ) -> None:
        self.out_dir = out_dir
        self.quarantine_dir = quarantine_dir
        self.ttl_s = ttl_s
        self.idle_exit_s = idle_exit_s
        self.hmac_secret = hmac_secret
        self.work_dir = work_dir or (out_dir.parent / "diode_work")
        self.disk_threshold_bytes = disk_threshold_bytes
        self.assemblies: dict[str, Assembly] = {}
        self.done_ids: set[str] = set()
        self.stats = {
            "datagrams": 0,
            "data_frames": 0,
            "parity_frames": 0,
            "meta_frames": 0,
            "eof_frames": 0,
            "crc_fail": 0,
            "hmac_fail": 0,
            "published": 0,
            "quarantined": 0,
            "duplicates": 0,
            "fec_recovered": 0,
            "bytes_published": 0,
            "last_transfer_id": "",
            "last_duration_s": 0.0,
            "receipts_written": 0,
            "status_frames": 0,
            "status_ignored": 0,
        }
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        # Recv-only: receipts are local files. Never send UDP from this process.
        self.receipts: ReceiptStore | None = None
        self.status_only = False
        self.receipt_every = 50
        self._data_since_receipt = 0

    def _asm(self, tid: str) -> Assembly:
        if tid not in self.assemblies:
            self.assemblies[tid] = Assembly(transfer_id=tid)
        return self.assemblies[tid]

    def _maybe_spill(self, asm: Assembly) -> None:
        if asm.disk or asm.total_len is None or asm.total_chunks is None:
            return
        if asm.total_len >= self.disk_threshold_bytes:
            asm.enable_disk(self.work_dir)

    def handle(self, data: bytes) -> None:
        self.stats["datagrams"] += 1
        try:
            frame = decode_frame(data)
        except ProtocolError as exc:
            self.stats["crc_fail"] += 1
            if self.stats["crc_fail"] <= 5 or self.stats["crc_fail"] % 500 == 0:
                print(f"drop: {exc}", file=sys.stderr)
            return

        tid = frame.header.get("transfer_id")
        if not tid:
            return
        if self.status_only:
            if frame.type is not FrameType.STATUS:
                self.stats["status_ignored"] += 1
                return
            self._handle_status_frame(frame)
            return
        if frame.type is FrameType.STATUS:
            self.stats["status_ignored"] += 1
            return
        if tid in self.done_ids:
            self.stats["duplicates"] += 1
            return

        asm = self._asm(str(tid))

        if frame.type in (FrameType.META, FrameType.EOF):
            if frame.type == FrameType.META:
                self.stats["meta_frames"] += 1
            else:
                self.stats["eof_frames"] += 1
            if (
                frame.type == FrameType.META
                and self.hmac_secret is not None
                and not verify_meta_hmac(
                    self.hmac_secret,
                    str(frame.header.get("transfer_id", "")),
                    int(frame.header.get("total_len", -1)),
                    int(frame.header.get("total_chunks", -1)),
                    str(frame.header.get("sha256", "")),
                    str(frame.header.get("hmac", "")),
                )
            ):
                self.stats["hmac_fail"] += 1
                print(f"drop: hmac fail transfer_id={tid}", file=sys.stderr)
                self._quarantine(
                    asm,
                    {
                        "transfer_id": tid,
                        "reason": "hmac",
                        "sha256_expected": frame.header.get("sha256"),
                    },
                    reason="hmac",
                    payload=b"",
                )
                asm.failed = True
                self.done_ids.add(str(tid))
                return
            asm.total_len = frame.header.get("total_len", asm.total_len)
            asm.total_chunks = frame.header.get("total_chunks", asm.total_chunks)
            asm.sha256 = frame.header.get("sha256", asm.sha256)
            if frame.type == FrameType.META:
                if "chunk_size" in frame.header:
                    asm.chunk_size = int(frame.header["chunk_size"])
                if "last_chunk_len" in frame.header:
                    asm.last_chunk_len = int(frame.header["last_chunk_len"])
                if "chunk_lens" in frame.header and frame.header["chunk_lens"]:
                    lenses = list(frame.header["chunk_lens"])
                    asm.last_chunk_len = int(lenses[-1])
                    if len(lenses) > 1:
                        asm.chunk_size = int(lenses[0])
            self._maybe_spill(asm)
        elif frame.type == FrameType.DATA:
            self.stats["data_frames"] += 1
            seq = int(frame.header["seq"])
            if not asm.has_seq(seq):
                asm.put_seq(seq, frame.body)
                self._note_data_receipt(asm)
            else:
                self.stats["duplicates"] += 1
            self._maybe_spill(asm)
        elif frame.type == FrameType.PARITY:
            self.stats["parity_frames"] += 1
            group = frame.header.get("group_seqs") or list(
                range(int(frame.header["seq_start"]), int(frame.header["seq_end"]) + 1)
            )
            group = [int(s) for s in group]
            key = (group[0], group[-1])
            asm.parities[key] = {"group_seqs": group, "body": frame.body}
            self._try_fec_group(asm, key)

        # FEC is intentional off the DATA hot path: every DATA used to scan
        # parity keys and often re-read group members — that starved recv and
        # caused multi‑% kernel drops on ≥128 MiB dual-pass transfers.
        # PARITY frames still try recovery; EOF / expire do a full sweep.
        if frame.type == FrameType.EOF and asm.parities:
            for key in list(asm.parities.keys()):
                self._try_fec_group(asm, key)

        self._try_complete(asm)
        if (
            frame.type == FrameType.EOF
            and not asm.complete
            and not asm.failed
        ):
            self._write_receipt(asm, STATUS_HOLES)

    def _handle_status_frame(self, frame) -> None:
        """Validate-track catcher: STATUS frames become local receipts only."""
        try:
            report = status_report_from_frame(frame)
        except ProtocolError as exc:
            self.stats["crc_fail"] += 1
            print(f"drop status: {exc}", file=sys.stderr)
            return
        self.stats["status_frames"] += 1
        if self.hmac_secret is not None:
            provided = str(frame.header.get("hmac", ""))
            if not verify_status_hmac(self.hmac_secret, report, provided):
                self.stats["hmac_fail"] += 1
                print(
                    f"drop: status hmac fail transfer_id={report.transfer_id}",
                    file=sys.stderr,
                )
                return
        if self.receipts is None:
            return
        self.receipts.write(report)
        self.stats["receipts_written"] += 1

    def _note_data_receipt(self, asm: Assembly) -> None:
        if self.receipts is None or asm.total_chunks is None:
            return
        self._data_since_receipt += 1
        if self.receipt_every <= 0 or self._data_since_receipt < self.receipt_every:
            return
        self._data_since_receipt = 0
        self._write_receipt(asm, STATUS_HOLES)

    def _write_receipt(self, asm: Assembly, kind: str, reason: str | None = None) -> None:
        if self.receipts is None:
            return
        missing = tuple(asm.missing_prefix()) if kind == STATUS_HOLES else ()
        if kind == STATUS_HOLES and asm.total_chunks is not None and not missing:
            return
        report = StatusReport(
            transfer_id=asm.transfer_id,
            kind=kind,
            have=asm.seq_count(),
            total_chunks=asm.total_chunks,
            missing=missing,
            reason=reason if kind == STATUS_FAIL else None,
        )
        self.receipts.write(report)
        self.stats["receipts_written"] += 1

    def _lens_map(self, asm: Assembly, group: list[int]) -> dict[int, int]:
        out: dict[int, int] = {}
        if asm.total_chunks is None:
            return out
        for s in group:
            out[s] = expected_chunk_len(
                asm.chunk_size, asm.last_chunk_len, int(asm.total_chunks), s
            )
        return out

    def _try_fec_group(self, asm: Assembly, key: tuple[int, int]) -> None:
        """Attempt XOR recovery for one parity group only (hot-path safe)."""
        if asm.total_chunks is None or key not in asm.parities:
            return
        entry = asm.parities[key]
        group = entry["group_seqs"]
        missing = [s for s in group if not asm.has_seq(s)]
        if len(missing) != 1:
            return
        miss = missing[0]
        mem: dict[int, bytes] = {}
        for s in group:
            if s == miss:
                continue
            body = asm.get_seq(s)
            if body is None:
                return
            mem[s] = body
        lens = self._lens_map(asm, group)
        if miss not in lens:
            return
        try:
            body = recover_missing_chunk(mem, miss, entry["body"], group, lens)
        except ValueError:
            return
        asm.put_seq(miss, body)
        self.stats["fec_recovered"] += 1
        if self.stats["fec_recovered"] <= 3 or self.stats["fec_recovered"] % 500 == 0:
            print(
                f"FEC recovered seq={miss} total_fec={self.stats['fec_recovered']}",
                file=sys.stderr,
            )

    def _stream_hash_and_publish(self, asm: Assembly) -> None:
        assert asm.total_chunks is not None
        assert asm.total_len is not None
        assert asm.sha256 is not None
        h = hashlib.sha256()
        total = 0
        final = self.out_dir / f"{asm.transfer_id}.bin"
        part = self.out_dir / f"{asm.transfer_id}.bin.part"
        with part.open("wb") as out:
            for i in range(asm.total_chunks):
                body = asm.get_seq(i)
                if body is None:
                    raise RuntimeError(f"missing seq {i}")
                out.write(body)
                h.update(body)
                total += len(body)
        digest = h.hexdigest()
        meta = {
            "transfer_id": asm.transfer_id,
            "total_len": asm.total_len,
            "sha256_expected": asm.sha256,
            "sha256_actual": digest,
            "chunks": asm.total_chunks,
            "fec_recovered": self.stats["fec_recovered"],
            "disk": asm.disk,
        }
        if total != asm.total_len or digest != asm.sha256:
            part.unlink(missing_ok=True)
            self._quarantine(asm, meta, reason="integrity", payload=b"")
            asm.failed = True
            self.done_ids.add(asm.transfer_id)
            return
        part.replace(final)
        (self.out_dir / f"{asm.transfer_id}.meta.json").write_text(
            json.dumps(meta, indent=2) + "\n"
        )
        self._cleanup_work(asm)
        asm.complete = True
        self.done_ids.add(asm.transfer_id)
        self.stats["published"] += 1
        self.stats["bytes_published"] = int(total)
        self.stats["last_transfer_id"] = asm.transfer_id
        self.stats["last_duration_s"] = round(time.time() - asm.first_seen, 3)
        self._write_receipt(asm, STATUS_OK)
        print(f"PUBLISHED {asm.transfer_id} bytes={total} sha256={digest}")
        try:
            (self.out_dir / f"{asm.transfer_id}.stats.json").write_text(
                json.dumps(self.stats, indent=2) + "\n"
            )
        except OSError:
            pass

    def _try_complete(self, asm: Assembly) -> None:
        if asm.complete or asm.failed:
            return
        if asm.total_chunks is None or asm.sha256 is None or asm.total_len is None:
            return
        if asm.seq_count() < asm.total_chunks:
            return
        try:
            self._stream_hash_and_publish(asm)
        except OSError as exc:
            print(f"publish error: {exc}", file=sys.stderr)
            self._quarantine(
                asm,
                {"transfer_id": asm.transfer_id, "error": str(exc)},
                reason="io",
                payload=b"",
            )
            asm.failed = True
            self.done_ids.add(asm.transfer_id)

    def _cleanup_work(self, asm: Assembly) -> None:
        asm.close()
        if asm.work_dir and asm.work_dir.is_dir():
            shutil.rmtree(asm.work_dir, ignore_errors=True)
        asm.chunks = {}
        asm.parities.clear()
        asm.present = None
        asm.have = 0

    def _quarantine(
        self,
        asm: Assembly,
        meta: dict,
        reason: str,
        payload: bytes | None = None,
    ) -> None:
        meta = {**meta, "reason": reason, "chunks_have": asm.seq_count()}
        qdir = self.quarantine_dir / asm.transfer_id
        qdir.mkdir(parents=True, exist_ok=True)
        (qdir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
        if payload is not None and len(payload) <= QUARANTINE_PAYLOAD_MAX:
            (qdir / "payload.bin").write_bytes(payload)
        fail_reason = reason if reason in FAIL_REASONS else "io"
        self._write_receipt(asm, STATUS_FAIL, fail_reason)
        self._cleanup_work(asm)
        self.stats["quarantined"] += 1
        print(f"QUARANTINE {asm.transfer_id} reason={reason}", file=sys.stderr)

    def expire(self, force: bool = False) -> None:
        now = time.time()
        for tid, asm in list(self.assemblies.items()):
            if asm.complete or asm.failed or tid in self.done_ids:
                continue
            if not force and now - asm.first_seen < self.ttl_s:
                continue
            # Last-chance FEC before fail (covers idle-timeout / graceful shutdown)
            if asm.parities and asm.total_chunks is not None:
                for key in list(asm.parities.keys()):
                    self._try_fec_group(asm, key)
                self._try_complete(asm)
                if asm.complete or asm.failed:
                    continue
            reason = "shutdown_incomplete" if force else "ttl"
            meta = {
                "transfer_id": tid,
                "total_len": asm.total_len,
                "total_chunks": asm.total_chunks,
                "sha256_expected": asm.sha256,
                "chunks_have_count": asm.seq_count(),
                "reason": reason,
            }
            self._quarantine(asm, meta, reason=reason, payload=b"")
            asm.failed = True
            self.done_ids.add(tid)


def _configure_recv_buf(sock: socket.socket, packet_size: int, want: int = 0) -> None:
    if want <= 0:
        want = max(32 * 1024 * 1024, 2000 * packet_size)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, want)
        actual = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        # Linux doubles the value for bookkeeping; report both.
        print(f"SO_RCVBUF want={want} actual={actual}", file=sys.stderr)
    except OSError as exc:
        print(f"warn: SO_RCVBUF failed: {exc}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bind", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9400)
    p.add_argument("--out", type=Path, default=Path("/tmp/diode_out"))
    p.add_argument("--quarantine", type=Path, default=Path("/tmp/diode_quarantine"))
    p.add_argument("--work", type=Path, default=None)
    p.add_argument("--ttl", type=float, default=300.0)
    p.add_argument("--idle-exit", type=float, default=None)
    p.add_argument("--max-seconds", type=float, default=None)
    p.add_argument("--hmac-secret", default="")
    p.add_argument("--rcvbuf", type=int, default=0)
    p.add_argument(
        "--receipts",
        type=Path,
        default=None,
        help="Local receipt drop-dir (recv-only). Coordinator copies this to the validate pitcher.",
    )
    p.add_argument(
        "--status-only",
        action="store_true",
        help="Validate-track catcher: accept STATUS frames only, write receipts, never assemble files.",
    )
    p.add_argument(
        "--receipt-every",
        type=int,
        default=50,
        help="Write a holes receipt every N new DATA frames (0=EOF/terminal only)",
    )
    args = p.parse_args()

    secret = args.hmac_secret.encode("utf-8") if args.hmac_secret else None
    idle_exit = args.idle_exit if args.idle_exit is not None and args.idle_exit > 0 else None
    catcher = Catcher(
        args.out, args.quarantine, args.ttl, idle_exit, secret, work_dir=args.work
    )
    if args.receipts is not None:
        catcher.receipts = ReceiptStore(args.receipts)
    catcher.status_only = bool(args.status_only)
    catcher.receipt_every = max(0, int(args.receipt_every))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _configure_recv_buf(sock, DEFAULT_CHUNK_SIZE + 256, want=args.rcvbuf)
    sock.bind((args.bind, args.port))
    sock.settimeout(0.2)
    print(f"catcher listening on {args.bind}:{args.port}", file=sys.stderr)

    start = time.time()
    last_pkt = start
    try:
        while True:
            now = time.time()
            if args.max_seconds is not None and now - start >= args.max_seconds:
                break
            if (
                idle_exit is not None
                and now - last_pkt >= idle_exit
                and catcher.stats["datagrams"]
            ):
                break
            catcher.expire()
            try:
                data, _addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            last_pkt = time.time()
            catcher.handle(data)
            # Drain kernel queue before next timeout: under dual-pass bursts the
            # socket fills faster than one-packet-per-loop handling.
            sock.setblocking(False)
            try:
                while True:
                    try:
                        data, _addr = sock.recvfrom(65535)
                    except BlockingIOError:
                        break
                    last_pkt = time.time()
                    catcher.handle(data)
            finally:
                sock.setblocking(True)
                sock.settimeout(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
        catcher.expire(force=True)
        print(json.dumps(catcher.stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
