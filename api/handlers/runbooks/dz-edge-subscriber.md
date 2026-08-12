---
runbook: dz-edge-subscriber
title: DoubleZero Edge — Market-Data Subscriber Onboarding
updated: 2026-07
summary: Take a user from no DoubleZero connection to a live Hyperliquid market-data feed over DoubleZero Edge.
applies_to: { arch: x86_64, os: [ubuntu-22.04+, debian-11+, rhel-9+, rocky-9+] }
modes:
  - id: direct-multicast
    title: Direct multicast subscribe (decode the feed yourself)
    status: available
  - id: edge-connect
    title: edge-connect container (pre-decoded JSON over WebSocket)
    status: coming-soon
uses_tools: [check_edge_access]
steps:
  - { id: prereqs,   locus: local,  blocking: true }
  - { id: install,   locus: local,  branch: os }
  - { id: identity,  locus: local,  produces: [pubkey] }
  - { id: firewall,  locus: local,  branch: os }
  - { id: feed,      locus: local,  mode: direct-multicast }
  - { id: access,    locus: verify, tool: check_edge_access, inputs: [pubkey, public_ip], blocking: true }
  - { id: subscribe, locus: local,  mode: direct-multicast }
  - { id: confirm,   locus: local,  mode: direct-multicast, verify: packets_flowing }
---

# DoubleZero Edge — Market-Data Subscriber Onboarding

Walks a user from no DoubleZero connection to a live Hyperliquid market-data feed
(published as UDP multicast on the DoubleZero group **`tiredsolid`**) delivered over
DoubleZero Edge. This runbook targets **mainnet-beta** subscribers.

> Run every `local` step **on the target host** — the machine that will receive the
> feed. That may be the user's own machine, or a remote host reached over SSH. Never
> assume localhost.

## mode-select

Ask the user how they want to connect:

- **`direct-multicast`** — they subscribe to the multicast group and decode the binary
  feed themselves (or with a reference parser). Fully supported below.
- **`edge-connect`** — they run the `doublezero-edge-connect` container, which bundles
  the client and re-serves the feed as normalized JSON over a WebSocket. **Coming soon
  in this runbook** — for now, guide `direct-multicast`.

## prereqs

**Do** — confirm on the target host:

- `uname -m` → must be `x86_64`.
- `. /etc/os-release; echo "$ID $VERSION_ID"` → Ubuntu 22.04+, Debian 11+, or Rocky/RHEL 9+.
- The DoubleZero client will run **directly on the host, not in a container**.
- The host has a **public IP with no NAT**. Compare the local interface address with
  `curl -s ifconfig.me`; if they differ, the host is behind NAT.
- The user has **root or sudo**.
- If on **AWS**: the instance ENI must have **source/destination check disabled**.

**Verify:** all of the above hold. **If NAT is detected → STOP.** A public IP with no
NAT is required; do not work around it.

## install

**Do** — install the DoubleZero packages for the host OS (`branch: os`):

Ubuntu / Debian:

```bash
curl -1sLf https://dl.cloudsmith.io/public/malbeclabs/doublezero/setup.deb.sh | sudo -E bash
sudo apt-get install doublezero
```

Rocky / RHEL:

```bash
curl -1sLf https://dl.cloudsmith.io/public/malbeclabs/doublezero/setup.rpm.sh | sudo -E bash
sudo yum install doublezero
```

**Verify:** the `doublezerod` systemd unit is active:

```bash
sudo systemctl status doublezerod   # should be active (running)
sudo journalctl -u doublezerod      # logs, if troubleshooting
```

## identity

**Do:** create the DoubleZero identity (the pubkey that authorizes this machine):

```bash
doublezero keygen
doublezero address    # prints the DoubleZero ID pubkey
```

**Produces:** `pubkey` — the output of `doublezero address`. **Carry this value into the
`access` step.**

**Verify:** `doublezero address` prints a pubkey.

## firewall

**Do** — allow the DoubleZero connection (GRE + BGP + PIM) and the Hyperliquid multicast
feed ports. Use `iptables` or `ufw` per the host (`branch: os`).

iptables:

```bash
sudo iptables -A OUTPUT -p gre -j ACCEPT
sudo iptables -A INPUT  -i doublezero1 -s 169.254.0.0/16 -d 169.254.0.0/16 -p tcp --dport 179 -j ACCEPT
sudo iptables -A OUTPUT -o doublezero1 -s 169.254.0.0/16 -d 169.254.0.0/16 -p tcp --dport 179 -j ACCEPT
sudo iptables -A OUTPUT -o doublezero1 -p pim -j ACCEPT
# Top-of-Book & Trades (mktdata + refdata, port sets A–D)
sudo iptables -A INPUT -i doublezero1 -p udp -m multiport --dports 9101,9102,9201,9202,9401,9402,9601,9602 -j ACCEPT
# Market-by-Order (mktdata + refdata + snapshot, port sets A–D)
sudo iptables -A INPUT -i doublezero1 -p udp -m multiport --dports 10101,10102,10103,10201,10202,10203,10401,10402,10403,10601,10602,10603 -j ACCEPT
sudo iptables -A INPUT -i doublezero0 -p udp --dport 44880 -j ACCEPT
```

ufw:

```bash
sudo ufw allow proto gre from any to any
sudo ufw allow in  on doublezero1 from 169.254.0.0/16 to 169.254.0.0/16 port 179 proto tcp
sudo ufw allow out on doublezero1 from 169.254.0.0/16 to 169.254.0.0/16 port 179 proto tcp
sudo ufw allow out on doublezero1 proto pim from any to any
sudo ufw allow in  on doublezero1 to any port 9101,9102,9201,9202,9401,9402,9601,9602 proto udp
sudo ufw allow in  on doublezero1 to any port 10101,10102,10103,10201,10202,10203,10401,10402,10403,10601,10602,10603 proto udp
sudo ufw allow in  on doublezero0 to any port 44880 proto udp
```

The client must also reach the DoubleZero RPC at `doublezero-mainnet-beta.rpcpool.com`
(outbound) and allow ICMP to/from the DoubleZero device IPs (those listed by
`doublezero latency`).

**Verify:** rules are present (`sudo iptables -L -n` or `sudo ufw status`).

## feed

**Do** — the Hyperliquid feed on `tiredsolid` (mainnet-beta):

| Property | Value |
|---|---|
| Multicast group (`tiredsolid`) | `233.84.178.15` (mainnet-beta; testnet is `233.84.178.6`) |
| Source ID | `1` |
| Market-by-Order channel ID | `1` |

Streams share the group address but use distinct ports — you select a stream by the
ports you bind. Port sets on mainnet-beta:

| Set | TOB mktdata | TOB refdata | MBO mktdata | MBO refdata | MBO snapshot |
|---|---|---|---|---|---|
| A | 9101 | 9102 | 10101 | 10102 | 10103 |
| B | 9201 | 9202 | 10201 | 10202 | 10203 |
| C | 9401 | 9402 | 10401 | 10402 | 10403 |
| D | 9601 | 9602 | 10601 | 10602 | 10603 |

The live source of truth for group IPs/names is `doublezero multicast group list`.

**Verify:** the user has chosen a feed type (Top-of-Book vs Market-by-Order) and a port set.

## access

**Do** — multicast access is gated by an access pass tied to the user's identity + a
specific receiving IP, plus membership on the `tiredsolid` subscriber allow list. Check
it using the pubkey from the `identity` step and the public IPv4 the user will receive on:

Call the MCP tool **`check_edge_access`** with `pubkey` and `public_ip`.

**Verify:**

- Status `active` → the IP is approved; continue to `subscribe`.
- Status `pending`/`none` → **blocking.** Hand the user the exact payload to send the
  DoubleZero Foundation to be added to the allow list:
  - The public IPv4 address they will receive on (`public_ip`).
  - Their DoubleZero identity pubkey (`pubkey`, from `doublezero address`).

  Then re-check with `check_edge_access` until it returns `active`. (One access pass is
  needed per receiving IP.)

## subscribe

**Do:**

```bash
doublezero connect multicast subscriber tiredsolid
```

**Verify:** the command succeeds and the tunnel interface `doublezero1` exists:

```bash
ip a s doublezero1
```

## confirm

**Do** — confirm the feed is arriving on the tunnel (example: Top-of-Book mktdata,
port set A):

```bash
sudo tcpdump -ni doublezero1 host 233.84.178.15 and udp port 9101
```

**Verify (`packets_flowing`):** packets are visible for the chosen group/port. If yes,
onboarding is complete — the user is receiving the live Hyperliquid feed over
DoubleZero Edge. To turn the binary frames into usable data, point them at the
reference parsers (`topofbook-parser` / `marketbyorder-parser`) or the
`edge-feed-spec` for a custom decoder.

## help

For access passes, allow-list changes, or feed issues, the user should contact the
DoubleZero Foundation with their identity pubkey and the IP(s) involved.
