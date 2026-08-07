"""UDP diode frame protocol — lab simulation only.

Wire format (big-endian):
  magic(4)=DIO1 | type(1) | flags(1) | header_len(2) | body_len(4) | header_json | body | crc32(4)

Types: META=1, DATA=2, EOF=3, PARITY=4
CRC covers everything before the CRC field.

META is intentionally compact for multi‑GB transfers: fixed ``chunk_size`` +
``last_chunk_len`` (not a full ``chunk_lens[]`` array — that blows the 16-bit
header length limit around ~tens of MB payloads).

High-assurance properties this encodes:
  - integrity: per-datagram CRC32 + whole-payload sha256
  - completeness: total_chunks + seq set
  - optional FEC: XOR parity over groups of DATA chunks (recover 1 loss/group)
"""
from __future__ import annotations

import hashlib
import json
import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any, BinaryIO, Iterator

MAGIC = b"DIO1"
HEADER_STRUCT = struct.Struct("!4sBBHI")  # magic, type, flags, header_len, body_len
CRC_STRUCT = struct.Struct("!I")
# UDP practical body; leave room for IP/UDP + JSON DATA header under ~1500 MTU / WG.
DEFAULT_CHUNK_SIZE = 1200
# Soft limit for in-memory build_transfer_frames (tests / small lab only).
IN_MEMORY_MAX_BYTES = 32 * 1024 * 1024


class FrameType(IntEnum):
    META = 1
    DATA = 2
    EOF = 3
    PARITY = 4


@dataclass(frozen=True)
class Frame:
    type: FrameType
    header: dict[str, Any]
    body: bytes = b""

    def encode(self) -> bytes:
        header_bytes = json.dumps(self.header, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        if len(header_bytes) > 0xFFFF:
            raise ProtocolError(
                f"header too large for wire ({len(header_bytes)} > 65535); "
                "META must stay compact (no full chunk_lens for large files)"
            )
        prefix = HEADER_STRUCT.pack(
            MAGIC, int(self.type), 0, len(header_bytes), len(self.body)
        )
        payload = prefix + header_bytes + self.body
        return payload + CRC_STRUCT.pack(zlib.crc32(payload) & 0xFFFFFFFF)


class ProtocolError(ValueError):
    pass


def decode_frame(datagram: bytes) -> Frame:
    if len(datagram) < HEADER_STRUCT.size + CRC_STRUCT.size:
        raise ProtocolError("datagram too short")
    magic, ftype, _flags, hlen, blen = HEADER_STRUCT.unpack_from(datagram, 0)
    if magic != MAGIC:
        raise ProtocolError(f"bad magic {magic!r}")
    total = HEADER_STRUCT.size + hlen + blen + CRC_STRUCT.size
    if len(datagram) != total:
        raise ProtocolError(f"length mismatch got={len(datagram)} want={total}")
    crc_off = HEADER_STRUCT.size + hlen + blen
    (want_crc,) = CRC_STRUCT.unpack_from(datagram, crc_off)
    got_crc = zlib.crc32(datagram[:crc_off]) & 0xFFFFFFFF
    if want_crc != got_crc:
        raise ProtocolError("crc mismatch")
    try:
        header = json.loads(datagram[HEADER_STRUCT.size : HEADER_STRUCT.size + hlen])
    except json.JSONDecodeError as exc:
        raise ProtocolError("bad header json") from exc
    body = datagram[HEADER_STRUCT.size + hlen : crc_off]
    try:
        t = FrameType(ftype)
    except ValueError as exc:
        raise ProtocolError(f"unknown type {ftype}") from exc
    return Frame(type=t, header=header, body=body)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    """Stream-hash a file; returns (hex_digest, size_bytes)."""
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            h.update(block)
            total += len(block)
    return h.hexdigest(), total


def meta_hmac(secret: bytes, transfer_id: str, total_len: int, total_chunks: int, digest: str) -> str:
    """HMAC-SHA256 over stable META fields (authenticity of sender intent)."""
    import hmac

    msg = f"{transfer_id}|{total_len}|{total_chunks}|{digest}".encode("utf-8")
    return hmac.new(secret, msg, hashlib.sha256).hexdigest()


def verify_meta_hmac(
    secret: bytes,
    transfer_id: str,
    total_len: int,
    total_chunks: int,
    digest: str,
    provided: str,
) -> bool:
    import hmac

    want = meta_hmac(secret, transfer_id, total_len, total_chunks, digest)
    return hmac.compare_digest(want, provided or "")


def chunk_payload(data: bytes, chunk_size: int) -> list[bytes]:
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)] or [b""]


def expected_chunk_len(chunk_size: int, last_chunk_len: int, total_chunks: int, seq: int) -> int:
    """Authoritative body length for DATA seq (compact META model)."""
    if total_chunks <= 0:
        return 0
    if seq == total_chunks - 1:
        return last_chunk_len if last_chunk_len > 0 else chunk_size
    return chunk_size


def xor_bytes(parts: list[bytes]) -> bytes:
    """XOR equal-width byte strings (pad shorter with zeros to max length)."""
    if not parts:
        return b""
    width = max(len(p) for p in parts)
    acc = bytearray(width)
    for part in parts:
        for i, b in enumerate(part):
            acc[i] ^= b
    return bytes(acc)


def recover_missing_chunk(
    chunks: dict[int, bytes],
    missing_seq: int,
    parity_body: bytes,
    group_seqs: list[int],
    original_lens: dict[int, int],
) -> bytes:
    """Recover one missing DATA body from XOR parity of its group."""
    others = []
    for seq in group_seqs:
        if seq == missing_seq:
            continue
        if seq not in chunks:
            raise ValueError("cannot recover: more than one missing in group")
        others.append(chunks[seq])
    others.append(parity_body)
    recovered = xor_bytes(others)
    want = original_lens.get(missing_seq)
    if want is None:
        return recovered.rstrip(b"\x00")  # weak fallback
    return recovered[:want]


def build_meta_header(
    transfer_id: str,
    total_len: int,
    total_chunks: int,
    digest: str,
    chunk_size: int,
    last_chunk_len: int,
    content_type: str = "application/octet-stream",
    parity_group: int = 0,
    hmac_secret: bytes | None = None,
) -> dict[str, Any]:
    header: dict[str, Any] = {
        "transfer_id": transfer_id,
        "total_len": total_len,
        "total_chunks": total_chunks,
        "sha256": digest,
        "content_type": content_type,
        "chunk_size": chunk_size,
        "last_chunk_len": last_chunk_len,
        "parity_group": parity_group if parity_group > 1 else 0,
    }
    if hmac_secret is not None:
        header["hmac"] = meta_hmac(
            hmac_secret, transfer_id, total_len, total_chunks, digest
        )
    return header


def build_transfer_frames(
    transfer_id: str,
    payload: bytes,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    content_type: str = "application/octet-stream",
    parity_group: int = 0,
    hmac_secret: bytes | None = None,
) -> list[Frame]:
    """Build META + DATA [+ PARITY] + EOF in memory (tests / small payloads).

    For large files use :func:`iter_transfer_frames_from_path` / pitcher streaming.
    """
    if len(payload) > IN_MEMORY_MAX_BYTES:
        raise ValueError(
            f"payload {len(payload)} exceeds in-memory limit {IN_MEMORY_MAX_BYTES}; "
            "use streaming pitcher for large files"
        )
    chunks = chunk_payload(payload, chunk_size)
    digest = sha256_hex(payload)
    last_len = len(chunks[-1]) if chunks else 0
    header = build_meta_header(
        transfer_id,
        len(payload),
        len(chunks),
        digest,
        chunk_size,
        last_len,
        content_type=content_type,
        parity_group=parity_group,
        hmac_secret=hmac_secret,
    )
    meta = Frame(type=FrameType.META, header=header)
    data_frames = [
        Frame(
            type=FrameType.DATA,
            header={
                "transfer_id": transfer_id,
                "seq": seq,
                "chunk_len": len(chunk),
            },
            body=chunk,
        )
        for seq, chunk in enumerate(chunks)
    ]
    out: list[Frame] = [meta, *data_frames]

    if parity_group > 1:
        for start in range(0, len(chunks), parity_group):
            group = list(range(start, min(start + parity_group, len(chunks))))
            bodies = [chunks[i] for i in group]
            out.append(
                Frame(
                    type=FrameType.PARITY,
                    header={
                        "transfer_id": transfer_id,
                        "seq_start": group[0],
                        "seq_end": group[-1],
                        "group_seqs": group,
                    },
                    body=xor_bytes(bodies),
                )
            )

    eof = Frame(
        type=FrameType.EOF,
        header={
            "transfer_id": transfer_id,
            "total_chunks": len(chunks),
            "sha256": digest,
            "total_len": len(payload),
        },
    )
    out.append(eof)
    return out


def file_transfer_stats(
    path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> tuple[str, int, int, int]:
    """Return (sha256_hex, total_len, total_chunks, last_chunk_len) without full load."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    h = hashlib.sha256()
    total = 0
    chunks = 0
    last_len = 0
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            h.update(block)
            total += len(block)
            last_len = len(block)
            chunks += 1
    if chunks == 0:
        # empty file: one empty chunk
        chunks = 1
        last_len = 0
        h.update(b"")
    return h.hexdigest(), total, chunks, last_len


def iter_data_and_parity_from_file(
    path: Path,
    transfer_id: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    parity_group: int = 0,
) -> Iterator[Frame]:
    """Yield DATA and optional PARITY frames by streaming the file (no full RAM)."""
    group_seqs: list[int] = []
    group_bodies: list[bytes] = []
    seq = 0
    with path.open("rb") as fh:
        empty = True
        while True:
            body = fh.read(chunk_size)
            if not body:
                break
            empty = False
            yield Frame(
                type=FrameType.DATA,
                header={
                    "transfer_id": transfer_id,
                    "seq": seq,
                    "chunk_len": len(body),
                },
                body=body,
            )
            if parity_group > 1:
                group_seqs.append(seq)
                group_bodies.append(body)
                if len(group_bodies) >= parity_group:
                    yield Frame(
                        type=FrameType.PARITY,
                        header={
                            "transfer_id": transfer_id,
                            "seq_start": group_seqs[0],
                            "seq_end": group_seqs[-1],
                            "group_seqs": list(group_seqs),
                        },
                        body=xor_bytes(group_bodies),
                    )
                    group_seqs.clear()
                    group_bodies.clear()
            seq += 1
        if empty:
            yield Frame(
                type=FrameType.DATA,
                header={"transfer_id": transfer_id, "seq": 0, "chunk_len": 0},
                body=b"",
            )
            seq = 1
        if parity_group > 1 and group_bodies:
            yield Frame(
                type=FrameType.PARITY,
                header={
                    "transfer_id": transfer_id,
                    "seq_start": group_seqs[0],
                    "seq_end": group_seqs[-1],
                    "group_seqs": list(group_seqs),
                },
                body=xor_bytes(group_bodies),
            )
