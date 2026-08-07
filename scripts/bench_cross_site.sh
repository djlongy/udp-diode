#!/usr/bin/env bash
# Cross-site size ladder over SSH. No estate defaults — set env explicitly.
#
# Required:
#   PITCHER_HOST  SSH host for pitcher (low side)
#   CATCHER_HOST  SSH host for catcher (high side)
# Optional:
#   SSH_USER (default: $USER), SSH_KEY, PORT (9400), SIZES_MB, PASSES, MAX_BITRATE,
#   PARITY, HMAC_SECRET, REMOTE_DIR
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

: "${PITCHER_HOST:?set PITCHER_HOST}"
: "${CATCHER_HOST:?set CATCHER_HOST}"
SSH_USER="${SSH_USER:-$USER}"
SSH_KEY="${SSH_KEY:-}"
PORT="${PORT:-9400}"
SIZES_MB="${SIZES_MB:-1,4,16,64,128}"
PASSES="${PASSES:-2}"
MAX_BITRATE="${MAX_BITRATE:-18}"
PARITY="${PARITY:-4}"
HMAC_SECRET="${HMAC_SECRET:-bench-hmac-change-me}"
REMOTE_DIR="${REMOTE_DIR:-/tmp/udp_diode}"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new)
[[ -n "$SSH_KEY" ]] && SSH_OPTS+=(-i "$SSH_KEY")

ssh_h() { ssh "${SSH_OPTS[@]}" "${SSH_USER}@$1" "bash -lc $(printf '%q' "$2")"; }
scp_to() { scp "${SSH_OPTS[@]}" "$2" "${SSH_USER}@$1:$3"; }

for h in "$PITCHER_HOST" "$CATCHER_HOST"; do
  ssh_h "$h" "mkdir -p $REMOTE_DIR"
  scp_to "$h" "$ROOT/protocol.py" "$REMOTE_DIR/"
  scp_to "$h" "$ROOT/pitcher.py" "$REMOTE_DIR/"
  scp_to "$h" "$ROOT/catcher.py" "$REMOTE_DIR/"
done

echo "Deployed to $PITCHER_HOST (pitcher) and $CATCHER_HOST (catcher)"
echo "Example catcher:"
echo "  ssh $SSH_USER@$CATCHER_HOST 'cd $REMOTE_DIR && python3 catcher.py --bind 0.0.0.0 --port $PORT --out /tmp/out --quarantine /tmp/q --rcvbuf 33554432 --hmac-secret $HMAC_SECRET'"
echo "Example pitcher:"
echo "  ssh $SSH_USER@$PITCHER_HOST 'cd $REMOTE_DIR && python3 pitcher.py /path/file --host $CATCHER_HOST --port $PORT --max-bitrate $MAX_BITRATE --passes $PASSES --parity-group $PARITY --gap-ms 0.5 --hmac-secret $HMAC_SECRET'"
echo
echo "This helper only deploys. Drive size ladders with scripts/bench_size_ladder.py locally,"
echo "or your own SSH orchestration. Defaults intentionally omit private network topology."
