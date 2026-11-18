#!/usr/bin/env bash
# Deletes everything up.sh created (the servers and the network labelled bench=$PREFIX) so it stops costing money.
# The disks go with the VMs: copy the results out first (admission.sh and the other scripts scp them already).
#
#   bench/scripts/down.sh       asks before deleting
#   bench/scripts/down.sh -y    does not ask
set -euo pipefail

PREFIX=${PREFIX:-tgbench}
command -v hcloud >/dev/null || { echo "hcloud not found: https://github.com/hetznercloud/cli" >&2; exit 1; }

echo "will delete:"
hcloud server list -l "bench=$PREFIX"
hcloud network list -l "bench=$PREFIX"
if [ "${1:-}" != "-y" ]; then
  read -r -p "delete these? [y/N] " answer
  [ "$answer" = y ] || { echo "nothing deleted"; exit 1; }
fi

hcloud server list -l "bench=$PREFIX" -o noheader -o columns=name | while read -r name; do
  hcloud server delete "$name"
done
hcloud network list -l "bench=$PREFIX" -o noheader -o columns=name | while read -r name; do
  hcloud network delete "$name"
done

# public IPs are reused by Hetzner: a stale host key would make the next up.sh fail with a key mismatch
if [ -f bench/env.sh ]; then
  # shellcheck disable=SC1091
  source bench/env.sh
  ssh-keygen -R "${SRV#root@}" >/dev/null 2>&1 || true
  ssh-keygen -R "${CLI#root@}" >/dev/null 2>&1 || true
  rm -f bench/env.sh
fi

echo "left in the project (should be empty):"
hcloud server list -l "bench=$PREFIX"
hcloud network list -l "bench=$PREFIX"
