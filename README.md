# udp-diode

**DIO1** — a lab-grade one-way UDP file transfer protocol (pitcher → catcher).

> **Not a security boundary.** This models sequenced UDP + checksums so you can reason about reorder, loss, FEC, quarantine, and multi‑GB transfers. A true data diode is a *physics* one-way link; applications still own integrity (at-least-once assembly). A second, independent one-way stream may carry receipts. A host that both sends and receives is the exfil anti-pattern.

## What you get

| Piece | Role |
|---|---|
| `pitcher.py` | Low-side sender (stream multi‑GB, throttle, multi-pass, XOR FEC) |
| `catcher.py` | High-side reassembler (RAM/mmap, HMAC META, atomic publish / quarantine) |
| `protocol.py` | Wire format `DIO1` — META / DATA / PARITY / EOF / STATUS |
| `receipts.py` | Local receipt JSON (holes / ok / fail). Coordinator language. |
| `coordinator.py` | Same-side copy of allowlisted receipts. No sockets. |
| `soft_diode_relay.py` | Optional software one-way hop for local demos |
| `run_demo.sh` | Loopback scenario matrix (publish / quarantine / dual-track ARQ) |

### Research absorbed

| Practice | In this tool |
|---|---|
| Compact META (no full lens array) | `chunk_size` + `last_chunk_len` |
| Stream large files | Pitcher stream path (>32 MiB) |
| Disk-backed reassembly | Catcher mmap spill above 640 MiB (RAM first) |
| META/EOF ×N redundancy | `--meta-copies` / `--eof-copies` (default 3) |
| XOR FEC (1 loss / group) | `--parity-group N` (PARITY + EOF path) |
| Send bitrate throttle | `--max-bitrate` Mbit/s |
| Large SO_RCVBUF + drain | Default ~32 MiB + non-blocking queue drain |
| META HMAC | `--hmac-secret` |
| Atomic publish + hash gate | `.part` → final + SHA-256 |

**Not included (candidates):** Reed-Solomon multi-loss FEC, jumbo frames E2E, product integrations.

## Quick start

```bash
# demo matrix (loopback)
./run_demo.sh

# unit tests
python3 -m pytest -q

# manual
python3 catcher.py --bind 127.0.0.1 --port 9400 \
  --out /tmp/diode_out --quarantine /tmp/diode_q --ttl 600

dd if=/dev/urandom of=/tmp/blob bs=1m count=64
python3 pitcher.py /tmp/blob --host 127.0.0.1 --port 9400 \
  --parity-group 4 --max-bitrate 80 --passes 2
```

## Multi‑GB

- Pitcher **streams** (hash pass, then send) — does not load the whole file into RAM.
- Catcher keeps assemblies in RAM up to 640 MiB, then mmap-backed slots.
- Prefer `--max-bitrate` on WAN; raise catcher `--ttl` for long transfers.

```bash
# high side
sudo sysctl -w net.core.rmem_max=67108864 net.core.rmem_default=16777216
python3 catcher.py --bind 0.0.0.0 --port 9400 \
  --out /tmp/diode_out --quarantine /tmp/diode_q --work /tmp/diode_work \
  --ttl 7200 --rcvbuf 33554432 --hmac-secret "$SECRET"

# low side (WAN-balanced example ≤128 MiB)
python3 pitcher.py /path/to/file.bin --host HIGH_IP --port 9400 \
  --hmac-secret "$SECRET" --parity-group 4 \
  --max-bitrate 18 --passes 2 --gap-ms 0.5
```

## Soft diode (local one-way hop)

```bash
python3 catcher.py --bind 127.0.0.1 --port 9400 --out /tmp/out --quarantine /tmp/q
python3 soft_diode_relay.py --listen-port 9401 --forward-port 9400
python3 pitcher.py /tmp/blob --host 127.0.0.1 --port 9401
```

## WAN recommended profile (speed vs assurance)

Measured on a ~10 ms RTT bidirectional site VPN (not optical diode). Full table: [`results/WAN-BALANCE.md`](results/WAN-BALANCE.md).

| Goal | Pitcher flags | Observed goodput |
|---|---|---|
| **Balanced ≤128 MiB** | `--max-bitrate 18 --passes 2 --parity-group 4 --gap-ms 0.5` | **~4.6–5.1 Mbit/s** |
| **256 MiB** | `--max-bitrate 15 --passes 2 …` | **~4.1 Mbit/s** |
| **512 MiB** | `--max-bitrate 12 --passes 2 …` | **~3.4 Mbit/s** |
| Speed best-effort | `--max-bitrate 18 --passes 1 …` | ~9.4 when lucky — **flaky integrity** |

**Takeaway:** dual-pass is the knee. Single-pass is faster but not for custody. Third pass is usually diminishing returns once dual-pass + FEC + catcher buffering hold. Unthrottled blasts self-inflict severe loss.

## Pitcher flags

| Flag | Effect |
|---|---|
| `--loss 0.1` | Drop 10% of DATA frames (lab) |
| `--reorder` | Shuffle DATA (memory path / small files) |
| `--corrupt` / `--wire-corrupt` | Integrity / CRC demos |
| `--parity-group N` | XOR FEC every N DATA chunks |
| `--passes N` | Full envelope retransmit N times (data-track redundancy) |
| `--send-receipts DIR` | Validate track: send STATUS from a receipt inbox (send-only) |
| `--watch-receipts DIR` | Data track: poll local inbox for holes/ok/fail and resend |
| `--meta-copies` / `--eof-copies` | Control-plane redundancy |
| `--hmac-secret S` | Sign META HMAC-SHA256 |
| `--max-bitrate X` | Throttle to X Mbit/s |
| `--gap-ms N` | Minimum inter-datagram sleep |
| `--progress-every-mb N` | Progress logs while streaming |

## Catcher flags

| Flag | Effect |
|---|---|
| `--ttl` | Incomplete assembly TTL (default 300s) |
| `--work DIR` | Spill root for large assemblies |
| `--rcvbuf N` | Explicit SO_RCVBUF (0 = auto ~32 MiB) |
| `--hmac-secret S` | Require META HMAC |
| `--idle-exit N` | Exit after idle (**positive only**; ≤0 disables) |
| `--max-seconds N` | Hard run limit |
| `--receipts DIR` | Local receipt drop-dir (recv-only; coordinator copies this) |
| `--status-only` | Validate-track catcher: STATUS frames only |

## Dual track (data ↑, validate ↓)

Two one-way streams that never share a socket, a directory, or a host role.
A process is send-only **or** receive-only. Production is four hosts
(`low-tx`, `high-rx`, `high-tx`, `low-rx`). Lab loopback uses four processes
plus two coordinators.

```text
LOW                              HIGH
data-pitcher ──data UDP──► data-catcher      recv-only, writes receipts/
     send-only                      │ coordinator (files only)
                                    ▼
validate-catcher ◄──STATUS── validate-pitcher  send-only
     recv-only, receipts/          reads inbox/
          │ coordinator
          ▼
data-pitcher watches inbox/ and resends missing DATA only
```

```bash
# high-rx
python3 catcher.py --bind 0.0.0.0 --port 9400 --out /tmp/out \
  --quarantine /tmp/q --receipts /high/data/out --receipt-every 0

python3 coordinator.py --from-dir /high/data/out --to-dir /high/validate/in

# high-tx
python3 pitcher.py --send-receipts /high/validate/in \
  --host LOW_RX_IP --port 9401 --receipt-copies 3

# low-rx
python3 catcher.py --bind 0.0.0.0 --port 9401 --status-only \
  --receipts /low/validate/out --out /tmp/val-out --quarantine /tmp/val-q

python3 coordinator.py --from-dir /low/validate/out --to-dir /low/data/in

# low-tx
python3 pitcher.py /path/to/file.bin --host HIGH_RX_IP --port 9400 \
  --watch-receipts /low/data/in --source-map /low/data/sources
```

Packet retry lives in the send-only data pitcher (`--watch-receipts`,
`--ack-timeout-s`, `--ack-rounds`, `--ack-backoff`). It never binds a
receive socket. An orchestrator is optional and only drops files next to
that pitcher.

### Loopback bench (8 MiB, induced DATA loss, n=3)

Not a WAN figure. No bitrate cap. Full table:
[`results/DUAL-VS-SINGLE.md`](results/DUAL-VS-SINGLE.md).

| Loss | single p=1 | single p=2+FEC | dual p=1 | dual p=1+FEC |
|---:|---|---|---|---|
| 0% | 3/3, **430 Mbit/s** | 3/3, 116 Mbit/s | 3/3, 205 Mbit/s | 3/3, 118 Mbit/s |
| 8% | 0/3 | 3/3, **119 Mbit/s** | 3/3, 25 Mbit/s | 3/3, 46 Mbit/s |
| 15% | 0/3 | 0/3 | 0/3 | 3/3, **26 Mbit/s** |

Dual-track is **not** a line-rate boost. On a clean path, single-pass is
fastest; dual-track adds receipt lag. At 8% loss it recovers where
single-pass cannot, but dual-pass+FEC is still faster on loopback
(second copy is cheaper than many 64-seq hole rounds). At 15%
independent loss, dual-pass+FEC failed and dual-track+FEC was the only
mode that published. Re-run: `python3 scripts/bench_dual_vs_single.py`.

## Guarantee modelled

At-least-once assembly with atomic publish and quarantine on integrity/TTL
failure. Without the validate track, retransmit is time/redundancy based only.
With it, the low-side coordinator resends only the missing seqs.

## Cross-site helpers

```bash
# deploy tools to two SSH hosts (you supply hosts — no estate defaults)
export PITCHER_HOST=low.example.net CATCHER_HOST=high.example.net
export SSH_USER=ops SSH_KEY=~/.ssh/id_ed25519
./scripts/bench_cross_site.sh

# local size ladder (both ends on this machine or tunnel)
python3 scripts/bench_size_ladder.py --help
```

## License

MIT — see [LICENSE](LICENSE).
