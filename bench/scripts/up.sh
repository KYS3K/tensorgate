#!/usr/bin/env bash
# Brings up the two bench VMs (server and load generator) on a private Hetzner network, applies the kernel
# settings from BENCHMARKS.md, installs mpstat and copies the binaries from bin/.
# Prints the variables the other scripts expect (SRV, CLI, SRV_PRIV, IF) and writes them to bench/env.sh.
# With UPS=1 a third VM is added for the load balancer runs: it hosts the mock upstreams (UPS, UPS_PRIV).
#
# Needs: the hcloud CLI logged in (HCLOUD_TOKEN or `hcloud context create`), an SSH key already uploaded to
# the Hetzner project, and `make bench-build` done. The VMs are billed by the hour until down.sh deletes them.
#
#   SSH_KEY=my-key bench/scripts/up.sh && source bench/env.sh
#   UPS=1 SSH_KEY=my-key bench/scripts/up.sh && source bench/env.sh     # plus the upstream VM
set -euo pipefail

PREFIX=${PREFIX:-tgbench}
SSH_KEY=${SSH_KEY:?name of an SSH key already in the Hetzner project, see: hcloud ssh-key list}
LOCATION=${LOCATION:-nbg1}
ZONE=${ZONE:-eu-central}      # network zone that LOCATION belongs to
SRV_TYPE=${SRV_TYPE:-cpx22}   # BENCHMARKS.md: 2 vCPU, 4 GB, shared AMD. Check the name: hcloud server-type list
CLI_TYPE=${CLI_TYPE:-cpx32}   # 4 vCPU, 8 GB
UPS=${UPS:-}                  # set to 1 to also create the upstream VM
UPS_TYPE=${UPS_TYPE:-cpx22}   # 2 vCPU: three mockupstream processes need little
IMAGE=${IMAGE:-ubuntu-26.04}  # BENCHMARKS.md ran Ubuntu 26.04. Check the name: hcloud image list --type system
SRV_PRIV=10.77.1.10
CLI_PRIV=10.77.1.11
UPS_PRIV=10.77.1.12
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5)

command -v hcloud >/dev/null || { echo "hcloud not found: https://github.com/hetznercloud/cli" >&2; exit 1; }
for f in bin/tensorgate-linux bin/loadgen-linux ${UPS:+bin/mockupstream-linux}; do
  [ -f "$f" ] || { echo "$f is missing: run 'make bench-build' from the repo root" >&2; exit 1; }
done
if hcloud server describe "$PREFIX-srv" >/dev/null 2>&1 || hcloud server describe "$PREFIX-cli" >/dev/null 2>&1 ||
  hcloud server describe "$PREFIX-ups" >/dev/null 2>&1; then
  echo "$PREFIX-srv, $PREFIX-cli or $PREFIX-ups already exists: run bench/scripts/down.sh first" >&2
  exit 1
fi
trap 'echo "up.sh failed: whatever was created is still billed, run bench/scripts/down.sh" >&2' ERR

echo "=== network"
hcloud network create --name "$PREFIX" --ip-range 10.77.0.0/16 --label bench="$PREFIX" >/dev/null
hcloud network add-subnet "$PREFIX" --type cloud --network-zone "$ZONE" --ip-range 10.77.1.0/24

create() { # name type private-ip
  echo "=== server $1 ($2)" >&2
  hcloud server create --name "$1" --type "$2" --image "$IMAGE" --location "$LOCATION" \
    --ssh-key "$SSH_KEY" --label bench="$PREFIX" >/dev/null
  hcloud server attach-to-network "$1" --network "$PREFIX" --ip "$3" >/dev/null
  hcloud server ip "$1"
}
SRV_IP=$(create "$PREFIX-srv" "$SRV_TYPE" "$SRV_PRIV")
CLI_IP=$(create "$PREFIX-cli" "$CLI_TYPE" "$CLI_PRIV")
[ -n "$UPS" ] && UPS_IP=$(create "$PREFIX-ups" "$UPS_TYPE" "$UPS_PRIV")

wait_ssh() {
  for _ in $(seq 60); do
    ssh "${SSH_OPTS[@]}" "root@$1" true 2>/dev/null && return 0
    sleep 3
  done
  echo "root@$1 did not accept ssh in 3 minutes" >&2
  return 1
}
wait_ssh "$SRV_IP"
wait_ssh "$CLI_IP"
[ -n "$UPS" ] && wait_ssh "$UPS_IP"

# $1 = "srv" or "cli", $2 = the private address of this VM. Prints the private interface name
tune() {
  ssh "${SSH_OPTS[@]}" "root@$3" bash -s -- "$1" "$2" <<'REMOTE'
set -e
role=$1; priv=$2
cat > /etc/sysctl.d/99-bench.conf <<'SYS'
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
fs.file-max = 2097152
net.ipv4.tcp_rmem = 4096 87380 4194304
net.ipv4.tcp_wmem = 4096 65536 4194304
SYS
# '*' does not cover root in limits.conf, and the benchmarks run as root
printf 'root soft nofile 65535\nroot hard nofile 65535\n* soft nofile 65535\n* hard nofile 65535\n' >> /etc/security/limits.conf
if [ "$role" = srv ]; then echo 'net.core.default_qdisc = fq' >> /etc/sysctl.d/99-bench.conf; fi
sysctl --system >/dev/null
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null && apt-get install -y -qq sysstat >/dev/null
iface=$(ip -o -4 addr show | awk -v p="$priv/" 'index($4, p) == 1 {print $2}')
if [ "$role" = srv ]; then tc qdisc replace dev "$iface" root fq; fi
echo "$iface"
REMOTE
}
echo "=== tuning"
IF=$(tune srv "$SRV_PRIV" "$SRV_IP" | tail -n 1)
tune cli "$CLI_PRIV" "$CLI_IP" >/dev/null
[ -n "$UPS" ] && tune cli "$UPS_PRIV" "$UPS_IP" >/dev/null

echo "=== binaries"
scp -q "${SSH_OPTS[@]}" bin/tensorgate-linux "root@$SRV_IP:/root/"
scp -q "${SSH_OPTS[@]}" bin/loadgen-linux "root@$CLI_IP:/root/"
[ -n "$UPS" ] && scp -q "${SSH_OPTS[@]}" bin/mockupstream-linux "root@$UPS_IP:/root/"

cat > bench/env.sh <<EOF
export SRV=root@$SRV_IP
export CLI=root@$CLI_IP
export SRV_PRIV=$SRV_PRIV
export IF=$IF
EOF
if [ -n "$UPS" ]; then
  printf 'export UPS=root@%s\nexport UPS_PRIV=%s\n' "$UPS_IP" "$UPS_PRIV" >> bench/env.sh
fi
echo
echo "ready. Next: source bench/env.sh"
echo "when finished: bench/scripts/down.sh"
cat bench/env.sh
