#!/usr/bin/env bash
# Local loopback diode demo matrix. Exit non-zero if a scenario fails its expect.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
PY="${PYTHON:-python3}"
WORKDIR="${TMPDIR:-/tmp}/diode_udp_demo_$$"
mkdir -p "$WORKDIR"
trap 'rm -rf "$WORKDIR"' EXIT

SRC="$WORKDIR/source.bin"
dd if=/dev/urandom of="$SRC" bs=4096 count=8 status=none
SRC_SHA="$($PY -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$SRC")"
PORT=$((19400 + RANDOM % 1000))

pass=0
fail=0

run_case() {
  local name="$1"
  local expect="$2" # publish | quarantine
  shift 2
  local out="$WORKDIR/$name/out"
  local quar="$WORKDIR/$name/quar"
  mkdir -p "$out" "$quar"

  $PY catcher.py --bind 127.0.0.1 --port "$PORT" --out "$out" --quarantine "$quar" \
    --ttl 3 --idle-exit 1.5 --max-seconds 12 >"$WORKDIR/$name.stats.json" 2>"$WORKDIR/$name.catcher.log" &
  local cpid=$!
  sleep 0.3
  # remaining args go to pitcher
  $PY pitcher.py "$SRC" --host 127.0.0.1 --port "$PORT" "$@" >"$WORKDIR/$name.tid" 2>"$WORKDIR/$name.pitcher.log"
  wait "$cpid" || true

  local published quar_n
  published=$(find "$out" -name '*.bin' ! -name '*.part' 2>/dev/null | wc -l | tr -d ' ')
  quar_n=$(find "$quar" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | tr -d ' ')

  if [[ "$expect" == "publish" ]]; then
    if [[ "$published" -ge 1 ]]; then
      local got
      got="$($PY -c "import hashlib,glob,sys; p=glob.glob(sys.argv[1]+'/*.bin'); print(hashlib.sha256(open(p[0],'rb').read()).hexdigest())" "$out")"
      if [[ "$got" == "$SRC_SHA" ]]; then
        echo "OK  $name (published, sha256 match)"
        pass=$((pass + 1))
        return
      fi
      echo "FAIL $name published but sha mismatch got=$got want=$SRC_SHA"
    else
      echo "FAIL $name expected publish, published=$published quarantined=$quar_n"
      tail -5 "$WORKDIR/$name.catcher.log" || true
    fi
  else
    if [[ "$quar_n" -ge 1 && "$published" -eq 0 ]]; then
      echo "OK  $name (quarantined as expected)"
      pass=$((pass + 1))
      return
    fi
    echo "FAIL $name expected quarantine, published=$published quarantined=$quar_n"
  fi
  fail=$((fail + 1))
  # bump port so next case is clean
  PORT=$((PORT + 1))
}

echo "source sha256=$SRC_SHA workdir=$WORKDIR"

run_case clean publish --passes 1
PORT=$((PORT + 1))
run_case reorder publish --reorder --passes 1
PORT=$((PORT + 1))
run_case loss_with_retransmit publish --loss 0.15 --passes 3
PORT=$((PORT + 1))
run_case corrupt_payload quarantine --corrupt --passes 1 --loss 0
PORT=$((PORT + 1))
# Drop all DATA by loss=1.0 with no retransmit → TTL quarantine
run_case total_loss quarantine --loss 1.0 --passes 1 --meta-copies 1 --eof-copies 1
PORT=$((PORT + 1))
# Single DATA loss recovered by XOR parity (no retransmit pass)
run_case fec_single_loss publish --parity-group 4 --drop-seq 1 --passes 1 --chunk-size 512
PORT=$((PORT + 1))
# Wire CRC reject path (should still complete if only one frame CRC-failed and FEC/other covers —
# here we corrupt one DATA without FEC → may quarantine if that chunk is lost to CRC)
run_case wire_crc_drop quarantine --wire-corrupt --passes 1 --parity-group 0 --chunk-size 512
PORT=$((PORT + 1))

# Soft diode: pitcher → relay (one-way) → catcher. No reverse path on the relay.
{
  name="soft_diode"
  expect="publish"
  out="$WORKDIR/$name/out"
  quar="$WORKDIR/$name/quar"
  mkdir -p "$out" "$quar"
  RELAY_PORT=$((PORT + 50))
  $PY catcher.py --bind 127.0.0.1 --port "$PORT" --out "$out" --quarantine "$quar" \
    --ttl 3 --idle-exit 1.5 --max-seconds 12 >"$WORKDIR/$name.stats.json" 2>"$WORKDIR/$name.catcher.log" &
  cpid=$!
  $PY soft_diode_relay.py --listen-host 127.0.0.1 --listen-port "$RELAY_PORT" \
    --forward-host 127.0.0.1 --forward-port "$PORT" --idle-exit 1.5 --max-seconds 12 \
    >"$WORKDIR/$name.relay.log" 2>&1 &
  rpid=$!
  sleep 0.3
  $PY pitcher.py "$SRC" --host 127.0.0.1 --port "$RELAY_PORT" --passes 1 \
    >"$WORKDIR/$name.tid" 2>"$WORKDIR/$name.pitcher.log"
  wait "$cpid" || true
  wait "$rpid" || true
  published=$(find "$out" -name '*.bin' ! -name '*.part' 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$published" -ge 1 ]]; then
    got="$($PY -c "import hashlib,glob,sys; p=glob.glob(sys.argv[1]+'/*.bin'); print(hashlib.sha256(open(p[0],'rb').read()).hexdigest())" "$out")"
    if [[ "$got" == "$SRC_SHA" ]]; then
      echo "OK  $name (published via one-way relay, sha256 match)"
      pass=$((pass + 1))
    else
      echo "FAIL $name published but sha mismatch"
      fail=$((fail + 1))
    fi
  else
    echo "FAIL $name expected publish through soft diode, published=$published"
    tail -5 "$WORKDIR/$name.catcher.log" "$WORKDIR/$name.relay.log" 2>/dev/null || true
    fail=$((fail + 1))
  fi
}

echo "----"
echo "passed=$pass failed=$fail"
[[ "$fail" -eq 0 ]]
