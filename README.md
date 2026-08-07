# udp-diode

**DIO1** — a lab-grade one-way UDP file transfer protocol (pitcher → catcher).

> **Not a security boundary.** This models sequenced UDP + checksums so you can reason about reorder, loss, FEC, quarantine, and multi‑GB transfers. A true data diode is a *physics* one-way link; applications still own integrity (at-least-once, no reverse ACK).

## What you get

| Piece | Role |
|---|---|
| `pitcher.py` | Low-side sender (stream multi‑GB, throttle, multi-pass, XOR FEC) |
| `catcher.py` | High-side reassembler (RAM/mmap, HMAC META, atomic publish / quarantine) |
| `protocol.py` | Wire format `DIO1` — META / DATA / PARITY / EOF |
| `soft_diode_relay.py` | Optional software one-way hop for local demos |
| `run_demo.sh` | Loopback scenario matrix (publish / quarantine cases) |

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
| `--passes N` | Full envelope retransmit N times (no ACK path) |
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

## Guarantee modelled

At-least-once assembly with atomic publish and quarantine on integrity/TTL failure. **No high→low ACK** — retransmit is time/redundancy based only.

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
