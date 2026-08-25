"""Executable assurance matrix for the DIO1 UDP diode protocol.

Imports modules from the repository root. Properties: integrity,
completeness, reorder, XOR FEC, quarantine on failure.
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

# Repo root (parent of tests/)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from catcher import Catcher  # noqa: E402
from protocol import (  # noqa: E402
    FrameType,
    ProtocolError,
    build_transfer_frames,
    decode_frame,
    recover_missing_chunk,
    sha256_hex,
    xor_bytes,
)


@pytest.fixture
def payload() -> bytes:
    rng = random.Random(42)
    return bytes(rng.getrandbits(8) for _ in range(3500))


def test_roundtrip_frames_encode_decode(payload: bytes) -> None:
    frames = build_transfer_frames("t1", payload, chunk_size=800)
    restored = [decode_frame(f.encode()) for f in frames]
    assert restored[0].type is FrameType.META
    data = [f for f in restored if f.type is FrameType.DATA]
    assert len(data) == restored[0].header["total_chunks"]
    body = b"".join(f.body for f in sorted(data, key=lambda x: x.header["seq"]))
    assert sha256_hex(body) == restored[0].header["sha256"]
    assert body == payload


def test_crc_mismatch_rejected(payload: bytes) -> None:
    fr = build_transfer_frames("t2", payload, chunk_size=500)[1]
    wire = bytearray(fr.encode())
    wire[-1] ^= 0x01
    with pytest.raises(ProtocolError, match="crc"):
        decode_frame(bytes(wire))


def test_reorder_still_assembles(tmp_path: Path, payload: bytes) -> None:
    frames = build_transfer_frames("reorder", payload, chunk_size=400)
    data = [f for f in frames if f.type is FrameType.DATA]
    meta = [f for f in frames if f.type is FrameType.META]
    eof = [f for f in frames if f.type is FrameType.EOF]
    order = meta + data + eof
    random.Random(7).shuffle(order)

    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    for f in order:
        c.handle(f.encode())
    assert c.stats["published"] == 1
    out = next((tmp_path / "out").glob("*.bin"))
    assert out.read_bytes() == payload


def test_integrity_failure_quarantines(tmp_path: Path, payload: bytes) -> None:
    frames = build_transfer_frames("bad", payload, chunk_size=400)
    # Flip a DATA body after build (CRC recomputed on encode → wire ok, sha256 fails)
    data_i = next(i for i, f in enumerate(frames) if f.type is FrameType.DATA)
    bad_body = bytearray(frames[data_i].body)
    bad_body[0] ^= 0xFF
    from protocol import Frame

    frames[data_i] = Frame(
        type=FrameType.DATA, header=frames[data_i].header, body=bytes(bad_body)
    )
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    for f in frames:
        c.handle(f.encode())
    assert c.stats["published"] == 0
    assert c.stats["quarantined"] == 1
    assert not list((tmp_path / "out").glob("*.bin"))


def test_duplicate_transfer_idempotent(tmp_path: Path, payload: bytes) -> None:
    frames = build_transfer_frames("dup", payload, chunk_size=600)
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    for f in frames:
        c.handle(f.encode())
    for f in frames:
        c.handle(f.encode())
    assert c.stats["published"] == 1
    assert c.stats["duplicates"] >= 1
    assert len(list((tmp_path / "out").glob("*.bin"))) == 1


def test_xor_parity_recovers_one_loss(tmp_path: Path, payload: bytes) -> None:
    frames = build_transfer_frames(
        "fec", payload, chunk_size=256, parity_group=4
    )
    # Drop DATA seq=1 only; keep META, other DATA, PARITY, EOF
    wire = []
    for f in frames:
        if f.type is FrameType.DATA and f.header["seq"] == 1:
            continue
        wire.append(f.encode())

    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    for d in wire:
        c.handle(d)
    assert c.stats["fec_recovered"] >= 1
    assert c.stats["published"] == 1
    assert next((tmp_path / "out").glob("*.bin")).read_bytes() == payload


def test_xor_parity_cannot_recover_two_losses(tmp_path: Path, payload: bytes) -> None:
    frames = build_transfer_frames(
        "fec2", payload, chunk_size=256, parity_group=4
    )
    wire = []
    for f in frames:
        if f.type is FrameType.DATA and f.header["seq"] in (0, 1):
            continue
        wire.append(f.encode())

    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=0.01, idle_exit_s=None)
    for d in wire:
        c.handle(d)
    c.expire(force=True)
    assert c.stats["published"] == 0
    assert c.stats["quarantined"] >= 1


def test_recover_missing_chunk_unit() -> None:
    a, b, c = b"aaaa", b"bbbb", b"cccc"
    parity = xor_bytes([a, b, c])
    chunks = {0: a, 2: c}
    got = recover_missing_chunk(chunks, 1, parity, [0, 1, 2], {1: 4})
    assert got == b


def test_meta_hmac_rejects_tampered_intent(tmp_path: Path, payload: bytes) -> None:
    secret = b"lab-shared-secret"
    frames = build_transfer_frames(
        "hmac1", payload, chunk_size=400, hmac_secret=secret
    )
    # Tamper META hmac after build
    meta = next(f for f in frames if f.type is FrameType.META)
    from protocol import Frame

    bad_header = dict(meta.header)
    bad_header["hmac"] = "0" * 64
    frames[0] = Frame(type=FrameType.META, header=bad_header, body=b"")
    c = Catcher(
        tmp_path / "out",
        tmp_path / "q",
        ttl_s=60,
        idle_exit_s=None,
        hmac_secret=secret,
    )
    for f in frames:
        c.handle(f.encode())
    assert c.stats["hmac_fail"] >= 1
    assert c.stats["published"] == 0
    assert c.stats["quarantined"] >= 1


def test_meta_hmac_accepts_valid(tmp_path: Path, payload: bytes) -> None:
    secret = b"lab-shared-secret"
    frames = build_transfer_frames(
        "hmac2", payload, chunk_size=400, hmac_secret=secret
    )
    c = Catcher(
        tmp_path / "out",
        tmp_path / "q",
        ttl_s=60,
        idle_exit_s=None,
        hmac_secret=secret,
    )
    for f in frames:
        c.handle(f.encode())
    assert c.stats["published"] == 1
    assert c.stats["hmac_fail"] == 0


def test_incomplete_ttl_quarantines(tmp_path: Path, payload: bytes) -> None:
    frames = build_transfer_frames("ttl", payload, chunk_size=500)
    # META only — never complete
    meta = next(f for f in frames if f.type is FrameType.META)
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=0.0, idle_exit_s=None)
    c.handle(meta.encode())
    c.expire(force=True)
    assert c.stats["quarantined"] == 1
    assert c.stats["published"] == 0


def test_compact_meta_fits_udp_header_for_large_logical_size() -> None:
    """META must not embed full chunk_lens (header_len is 16-bit)."""
    # Simulate 1 GiB @ 1200 B chunks → ~873k chunks — only via header fields.
    from protocol import Frame, build_meta_header

    header = build_meta_header(
        "big",
        total_len=1024**3,
        total_chunks=873814,
        digest="a" * 64,
        chunk_size=1200,
        last_chunk_len=400,
        parity_group=4,
    )
    wire = Frame(type=FrameType.META, header=header).encode()
    assert len(wire) < 1500
    assert "chunk_lens" not in header


def test_disk_backed_assembly_publishes(tmp_path: Path) -> None:
    """Catcher spills to disk when thresholds are low; still publishes correctly."""
    rng = random.Random(99)
    payload = bytes(rng.getrandbits(8) for _ in range(50_000))
    frames = build_transfer_frames("disk1", payload, chunk_size=800, parity_group=4)
    c = Catcher(
        tmp_path / "out",
        tmp_path / "q",
        ttl_s=60,
        idle_exit_s=None,
        work_dir=tmp_path / "work",
        disk_threshold_bytes=1000,
    )
    for f in frames:
        c.handle(f.encode())
    assert c.stats["published"] == 1
    assert next((tmp_path / "out").glob("*.bin")).read_bytes() == payload


def test_stream_stats_and_iter_match_in_memory(tmp_path: Path) -> None:
    from protocol import file_transfer_stats, iter_data_and_parity_from_file, sha256_hex

    path = tmp_path / "blob.bin"
    data = bytes(range(256)) * 40  # 10 KiB
    path.write_bytes(data)
    digest, total, nchunks, last = file_transfer_stats(path, chunk_size=500)
    assert digest == sha256_hex(data)
    assert total == len(data)
    frames = list(iter_data_and_parity_from_file(path, "s1", chunk_size=500, parity_group=4))
    data_n = sum(1 for f in frames if f.type is FrameType.DATA)
    assert data_n == nchunks
    assert last == len(data) % 500 or 500


def test_status_frame_roundtrip_and_hmac() -> None:
    from protocol import (
        STATUS_HOLES,
        StatusReport,
        build_status_frame,
        decode_frame,
        status_report_from_frame,
        verify_status_hmac,
    )

    report = StatusReport(
        transfer_id="t-status",
        kind=STATUS_HOLES,
        have=3,
        total_chunks=8,
        missing=(1, 4, 5),
    )
    secret = b"validate-track"
    frame = build_status_frame(report, hmac_secret=secret)
    restored = decode_frame(frame.encode())
    assert restored.type is FrameType.STATUS
    got = status_report_from_frame(restored)
    assert got.missing == (1, 4, 5)
    assert verify_status_hmac(secret, got, str(restored.header["hmac"]))
    assert not verify_status_hmac(secret, got, "0" * 64)


def test_data_catcher_writes_holes_then_ok(tmp_path: Path, payload: bytes) -> None:
    from protocol import STATUS_HOLES, STATUS_OK, build_transfer_frames
    from receipts import ReceiptStore

    frames = build_transfer_frames("holes1", payload, chunk_size=400)
    store = ReceiptStore(tmp_path / "receipts")
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    c.receipts = store
    c.receipt_every = 0
    dropped = None
    for f in frames:
        if f.type is FrameType.DATA and f.header["seq"] == 1:
            dropped = f
            continue
        c.handle(f.encode())
    assert c.stats["published"] == 0
    report = store.read("holes1")
    assert report is not None
    assert report.kind == STATUS_HOLES
    assert 1 in report.missing
    assert dropped is not None
    c.handle(dropped.encode())
    assert c.stats["published"] == 1
    done = store.read("holes1")
    assert done is not None
    assert done.kind == STATUS_OK
    assert done.missing == ()


def test_data_catcher_ignores_status_frames(tmp_path: Path, payload: bytes) -> None:
    from protocol import STATUS_HOLES, StatusReport, build_status_frame, build_transfer_frames

    frames = build_transfer_frames("ign", payload, chunk_size=400)
    status = build_status_frame(
        StatusReport("ign", STATUS_HOLES, have=0, total_chunks=2, missing=(0,))
    )
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    c.handle(status.encode())
    for f in frames:
        c.handle(f.encode())
    assert c.stats["status_ignored"] >= 1
    assert c.stats["published"] == 1


def test_status_only_catcher_writes_receipt_ignores_data(
    tmp_path: Path, payload: bytes
) -> None:
    from protocol import STATUS_OK, StatusReport, build_status_frame, build_transfer_frames
    from receipts import ReceiptStore

    store = ReceiptStore(tmp_path / "receipts")
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    c.receipts = store
    c.status_only = True
    data = build_transfer_frames("sx", payload, chunk_size=400)
    for f in data:
        c.handle(f.encode())
    assert c.stats["published"] == 0
    assert c.stats["status_ignored"] >= 1
    frame = build_status_frame(
        StatusReport("sx", STATUS_OK, have=4, total_chunks=4, missing=())
    )
    c.handle(frame.encode())
    got = store.read("sx")
    assert got is not None
    assert got.kind == STATUS_OK


def test_coordinator_strips_unknown_keys(tmp_path: Path) -> None:
    import json as json_mod

    from coordinator import copy_once

    src = tmp_path / "from"
    dst = tmp_path / "to"
    src.mkdir()
    (src / "t1.json").write_text(
        '{"transfer_id":"t1","kind":"holes","have":1,'
        '"total_chunks":3,"missing":[2],'
        '"hostname":"high-side-secret","error":"traceback"}\n'
    )
    n = copy_once(src, dst)
    assert n == 1
    body = json_mod.loads((dst / "t1.json").read_text())
    assert body["missing"] == [2]
    assert "hostname" not in body
    assert "error" not in body


def test_integrity_fail_writes_fail_receipt(tmp_path: Path, payload: bytes) -> None:
    from protocol import Frame, STATUS_FAIL, build_transfer_frames
    from receipts import ReceiptStore

    frames = build_transfer_frames("bad2", payload, chunk_size=400)
    data_i = next(i for i, f in enumerate(frames) if f.type is FrameType.DATA)
    bad_body = bytearray(frames[data_i].body)
    bad_body[0] ^= 0xFF
    frames[data_i] = Frame(
        type=FrameType.DATA, header=frames[data_i].header, body=bytes(bad_body)
    )
    store = ReceiptStore(tmp_path / "receipts")
    c = Catcher(tmp_path / "out", tmp_path / "q", ttl_s=60, idle_exit_s=None)
    c.receipts = store
    for f in frames:
        c.handle(f.encode())
    report = store.read("bad2")
    assert report is not None
    assert report.kind == STATUS_FAIL
    assert report.reason == "integrity"
