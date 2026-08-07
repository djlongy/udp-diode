# WAN balance ladder (representative)

Measured on a bidirectional site VPN path (~10 ms RTT), not a physical diode.
Validates the **application protocol** under geo latency and loss.

Catcher configuration for these runs:
- pure-RAM assembly for payloads ≤640 MiB
- FEC recovery on PARITY/EOF only (not per-DATA hot path)
- non-blocking socket drain + SO_RCVBUF 32 MiB (`rmem_max` ≥ 64 MiB)

## Size ladder (best OK config)

| MB | ok | Mbit/s | sec | fec | bitrate | passes | gap_ms |
|---:|:---:|------:|----:|----:|--------:|-------:|-------:|
| 1 | true | 4.84 | 1.7 | 0 | 18 | 2 | 0.5 |
| 4 | true | 5.04 | 6.7 | 0 | 18 | 2 | 0.5 |
| 16 | true | 5.08 | 26.4 | 106 | 18 | 2 | 0.5 |
| 64 | true | 4.62 | 116.1 | 13 | 18 | 2 | 0.5 |
| 128 | true | 4.75 | 226.1 | 3 | 18 | 2 | 0.5 |
| 256 | true | 4.08 | 526.9 | 5 | 15 | 2 | 0.5 |
| 512 | true | 3.43 | 1253.8 | 65 | 12 | 2 | 0.5 |

## 64 MiB profile bake-off

| profile | ok | Mbit/s | bitrate | passes |
|---|:---:|------:|--------:|-------:|
| fast (p=1) | false | 0 | 18 | 1 |
| balanced | true | 4.74 | 18 | 2 |
| mid | true | 4.12 | 15 | 2 |
| solid | true | 3.47 | 12 | 2 |
| fast15 (p=1) | false | 0 | 15 | 1 |

## Takeaways

1. **Dual-pass is the assurance knee** on this path — single-pass can peak ~9.4 Mbit/s but fails integrity under clustered residual loss.
2. **Lower bitrate as size grows** (18 → 15 → 12) rather than adding a third pass once dual+FEC works.
3. Unthrottled blasts self-inflict severe loss; always use `--max-bitrate` on WAN.
