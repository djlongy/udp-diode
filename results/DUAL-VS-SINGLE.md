# Dual-track vs single-track (loopback)

**Not a WAN number.** Loopback `127.0.0.1`, 8 MiB payload, chunk 1200,
no `--max-bitrate`. Loss is pitcher `--loss` on the *initial* DATA pass
only (ARQ fills are not coin-flipped). n=3, median of successful reps.
Ack timeout 0.25 s. Script: `scripts/bench_dual_vs_single.py`.
Raw: [`dual-vs-single.json`](dual-vs-single.json).

Wall time is pitcher start→exit. Mbit/s is `8 MiB * 8 / wall` when the
catcher published a matching sha256.

| Induced loss | Mode | Publish | Median wall | Median Mbit/s | Frames logged |
|---:|---|:---:|---:|---:|---:|
| 0% | single p=1 | 3/3 | 0.149 s | 430 | 6997 |
| 0% | single p=2 + FEC4 | 3/3 | 0.553 s | 116 | 17490 |
| 0% | dual p=1 | 3/3 | 0.312 s | 205 | 6997 |
| 0% | dual p=1 + FEC4 | 3/3 | 0.543 s | 118 | 8745 |
| 8% | single p=1 | 0/3 | — | — | 6436 |
| 8% | single p=2 + FEC4 | 3/3 | 0.536 s | 119 | 16379 |
| 8% | dual p=1 | 3/3 | 2.548 s | 25 | 6453 |
| 8% | dual p=1 + FEC4 | 3/3 | 1.390 s | 46 | 8219 |
| 15% | single p=1 | 0/3 | — | — | 5935 |
| 15% | single p=2 + FEC4 | 0/3 | — | — | 15377 |
| 15% | dual p=1 | 0/3 | — | — | 5920 |
| 15% | dual p=1 + FEC4 | 3/3 | 2.425 s | 26 | 7681 |

`frames_sent` is the last `sent=` line from the *initial* pass. ARQ
fill frames are not in that counter.

## What this shows

1. **Dual-track does not raise peak goodput.** On a clean loopback,
   single-pass is fastest. Dual-track pays receipt/coordinator lag
   (~0.16 s here). Dual-pass+FEC is slowest because it sends the
   envelope twice plus parity.
2. **At 8% loss, dual-track recovers where single-pass cannot**, but
   it is *slower* than dual-pass+FEC on loopback. Hole fill is windowed
   (64 seqs per STATUS) and waits a timeout per window, so many holes
   become round-trip bound. Dual-pass just blasts a second copy.
3. **At 15% independent loss, dual-pass+FEC failed 3/3; dual-track+FEC
   published 3/3.** That is the case the validate track exists for:
   remaining holes after FEC, without paying a third full pass.
4. Dual-track without FEC failed at 15% in this window/timeout budget
   (16 rounds). FEC is still required for clustered loss.

## What this does *not* show

WAN dual-pass pays **2× wire time** at `--max-bitrate`. Dual-track
sends about one pass plus holes. If RTT ≪ transfer duration, that
should beat dual-pass on wall clock. **Not measured on a site path
in this run.** The WAN table in the README is still dual-pass only.
