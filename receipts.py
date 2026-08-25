"""Local receipt drop-dir — the coordinator's language.

Catchers write JSON here. Pitchers read JSON here. Neither opens a socket
in the opposite direction. The two UDP tracks never share a directory;
a same-side coordinator copies an allowlisted schema from catcher outbox
to pitcher inbox.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from protocol import (
    STATUS_FAIL,
    STATUS_KINDS,
    STATUS_MISSING_CAP,
    StatusReport,
)

RECEIPT_KEYS = (
    "transfer_id",
    "kind",
    "have",
    "total_chunks",
    "missing",
    "reason",
)
SOURCE_KEYS = ("transfer_id", "path", "chunk_size", "parity_group")
FAIL_REASONS = frozenset(
    {"integrity", "ttl", "hmac", "io", "shutdown_incomplete"}
)


def report_to_dict(report: StatusReport) -> dict:
    return {
        "transfer_id": report.transfer_id,
        "kind": report.kind,
        "have": report.have,
        "total_chunks": report.total_chunks,
        "missing": list(report.missing)[:STATUS_MISSING_CAP],
        "reason": report.reason if report.kind == STATUS_FAIL else None,
    }


def report_from_dict(data: dict) -> StatusReport:
    kind = str(data.get("kind", ""))
    if kind not in STATUS_KINDS:
        raise ValueError(f"unknown receipt kind {kind!r}")
    reason = data.get("reason")
    if reason is not None:
        reason = str(reason)
        if reason not in FAIL_REASONS:
            reason = None
    raw_missing = data.get("missing") or []
    missing = tuple(int(s) for s in raw_missing)[:STATUS_MISSING_CAP]
    total = data.get("total_chunks")
    return StatusReport(
        transfer_id=str(data.get("transfer_id", "")),
        kind=kind,
        have=int(data.get("have", 0)),
        total_chunks=None if total is None else int(total),
        missing=missing,
        reason=reason if kind == STATUS_FAIL else None,
    )


def allowlist_receipt(data: dict) -> dict:
    """Drop any key the reverse track must not carry."""
    report = report_from_dict(data)
    return report_to_dict(report)


@dataclass
class ReceiptStore:
    directory: Path

    def __post_init__(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, transfer_id: str) -> Path:
        return self.directory / f"{transfer_id}.json"

    def write(self, report: StatusReport) -> Path:
        path = self.path_for(report.transfer_id)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(report_to_dict(report), indent=2) + "\n")
        tmp.replace(path)
        return path

    def read(self, transfer_id: str) -> StatusReport | None:
        path = self.path_for(transfer_id)
        if not path.is_file():
            return None
        data = json.loads(path.read_text())
        return report_from_dict(data)

    def list_reports(self) -> list[tuple[Path, StatusReport]]:
        out: list[tuple[Path, StatusReport]] = []
        for path in sorted(self.directory.glob("*.json")):
            if path.name.endswith(".tmp"):
                continue
            try:
                report = report_from_dict(json.loads(path.read_text()))
            except (OSError, json.JSONDecodeError, ValueError, TypeError):
                continue
            if not report.transfer_id:
                continue
            out.append((path, report))
        return out


def write_source_map(
    directory: Path,
    transfer_id: str,
    path: Path,
    chunk_size: int,
    parity_group: int = 0,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    dest = directory / f"{transfer_id}.json"
    tmp = dest.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(
            {
                "transfer_id": transfer_id,
                "path": str(path),
                "chunk_size": chunk_size,
                "parity_group": parity_group,
            },
            indent=2,
        )
        + "\n"
    )
    tmp.replace(dest)
    return dest


def read_source_map(directory: Path, transfer_id: str) -> dict | None:
    path = directory / f"{transfer_id}.json"
    if not path.is_file():
        return None
    data = json.loads(path.read_text())
    if "path" not in data or "chunk_size" not in data:
        return None
    return {
        "transfer_id": str(data.get("transfer_id", transfer_id)),
        "path": Path(str(data["path"])),
        "chunk_size": int(data["chunk_size"]),
        "parity_group": int(data.get("parity_group") or 0),
    }
