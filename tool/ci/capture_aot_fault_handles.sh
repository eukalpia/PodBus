#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update >/dev/null
sudo apt-get install -y strace lsof >/dev/null
mkdir -p build test-results/aot-handles

dart compile exe tool/fault_suite.dart -o build/podbus-fault-suite
bash tool/ci/start_integration_services.sh nats rabbitmq toxiproxy
bash tool/ci/wait_for_nats.sh
bash tool/ci/wait_for_rabbitmq.sh
bash tool/ci/wait_for_toxiproxy.sh

report=test-results/aot-handles/nats.json
trace_prefix=test-results/aot-handles/lifecycle
strace -ff -tt -T \
  -e trace=socket,socketpair,connect,accept,accept4,bind,listen,shutdown,close,epoll_ctl,timerfd_create,timerfd_settime \
  -o "$trace_prefix" \
  -- build/podbus-fault-suite \
    --profile=smoke \
    --scenario=nats-tcp-partition \
    --report="$report" \
  > test-results/aot-handles/stdout.log 2>&1 &
tracer_pid=$!
echo "$tracer_pid" > test-results/aot-handles/tracer-pid.txt

for _ in $(seq 1 120); do
  if [[ -s "$report" ]]; then
    break
  fi
  if ! kill -0 "$tracer_pid" 2>/dev/null; then
    wait "$tracer_pid"
    exit $?
  fi
  sleep 1
done

test -s "$report"
python3 - "$report" <<'PY'
import json
import pathlib
import sys

data = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert data['success'] is True, data
PY

pid="$(pgrep -P "$tracer_pid" -f 'podbus-fault-suite' | head -n 1 || true)"
if [[ -z "$pid" ]]; then
  pid="$(pgrep -n -f "$PWD/build/podbus-fault-suite" || true)"
fi
test -n "$pid"
echo "$pid" > test-results/aot-handles/pid.txt

sleep 3
if ! kill -0 "$pid" 2>/dev/null; then
  wait "$tracer_pid"
  exit $?
fi

{
  echo '=== status ==='
  cat "/proc/$pid/status"
  echo '=== fd ==='
  ls -la "/proc/$pid/fd"
  echo '=== fd targets ==='
  for fd in /proc/$pid/fd/*; do
    printf '%s -> ' "$fd"
    readlink "$fd" || true
  done
  echo '=== fdinfo ==='
  for info in /proc/$pid/fdinfo/*; do
    echo "--- $info ---"
    cat "$info" || true
  done
  echo '=== process tcp ==='
  cat "/proc/$pid/net/tcp" || true
  echo '=== process tcp6 ==='
  cat "/proc/$pid/net/tcp6" || true
  echo '=== process unix ==='
  cat "/proc/$pid/net/unix" || true
  echo '=== process timers ==='
  cat "/proc/$pid/timers" || true
  echo '=== lsof ==='
  sudo lsof -nP -p "$pid" || true
  echo '=== lsof internet ==='
  sudo lsof -nP -a -p "$pid" -i || true
  echo '=== sockets ==='
  sudo ss -anpte || true
  echo '=== task stacks ==='
  for task in /proc/$pid/task/*; do
    echo "--- $task ---"
    sudo cat "$task/stack" || true
  done
} > test-results/aot-handles/handles.txt 2>&1

python3 - "$pid" > test-results/aot-handles/socket-map.txt <<'PY'
import ipaddress
import pathlib
import re
import socket
import sys

pid = sys.argv[1]
fd_dir = pathlib.Path(f'/proc/{pid}/fd')
inodes = {}
for fd in fd_dir.iterdir():
    try:
        target = fd.readlink().as_posix()
    except OSError:
        continue
    match = re.fullmatch(r'socket:\[(\d+)\]', target)
    if match:
        inodes[match.group(1)] = fd.name

def decode_ipv4(value):
    return socket.inet_ntoa(bytes.fromhex(value)[::-1])

def decode_ipv6(value):
    raw = bytes.fromhex(value)
    words = [raw[index:index + 4][::-1] for index in range(0, 16, 4)]
    return str(ipaddress.IPv6Address(b''.join(words)))

states = {
    '01': 'ESTABLISHED', '02': 'SYN_SENT', '03': 'SYN_RECV',
    '04': 'FIN_WAIT1', '05': 'FIN_WAIT2', '06': 'TIME_WAIT',
    '07': 'CLOSE', '08': 'CLOSE_WAIT', '09': 'LAST_ACK',
    '0A': 'LISTEN', '0B': 'CLOSING',
}
for name, decoder in [('tcp', decode_ipv4), ('tcp6', decode_ipv6)]:
    table = pathlib.Path(f'/proc/{pid}/net/{name}')
    if not table.exists():
        continue
    for line in table.read_text().splitlines()[1:]:
        fields = line.split()
        inode = fields[9]
        if inode not in inodes:
            continue
        local_address, local_port = fields[1].split(':')
        remote_address, remote_port = fields[2].split(':')
        print(
            f'fd={inodes[inode]} inode={inode} family={name} '
            f'local={decoder(local_address)}:{int(local_port, 16)} '
            f'remote={decoder(remote_address)}:{int(remote_port, 16)} '
            f'state={states.get(fields[3], fields[3])}'
        )
PY

kill -TERM "$pid" || true
sleep 2
kill -KILL "$pid" 2>/dev/null || true
kill -TERM "$tracer_pid" 2>/dev/null || true
wait "$tracer_pid" || true

echo 'AOT process remained alive after a successful report.' >&2
exit 1
