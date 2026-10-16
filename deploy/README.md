# Production deployment kit: one always-on antimony server

A single-host setup for a server that a fleet of agents (and you) use around
the clock: TLS on a domain, durable Postgres, an S3 store, nightly backups, a
plain-git mirror and basic monitoring. It is the "one replica" shape of the
[operator guide](../docs/operator-guide.md); read its sections 1 to 3 once.

```
agents / you                          Internet
  ant (gRPC, HTTPS)  git clone (HTTPS)     |  443 (80 for certificates)
        \                 /                v
         +------- Caddy: TLS, h2 / gRPC passthrough ------+
                           |  h2c :7420 (gRPC)   http :7421 (/o /t /repo.git /readyz)
                      ant serve  (policy, rate limits)  -- :9100 /metrics, loopback only
                       /                 \
              PostgreSQL 17           SeaweedFS (S3)
          (refs, commits, small objects)   (objects > 4 KiB, packs)

host cron:  backup.sh (nightly)   git-mirror.sh (every 10 min)   ant admin gc (weekly)
```

| file | what |
|---|---|
| `compose.yaml` | the stack: `ant`, `postgres`, `seaweedfs`, `caddy`; healthchecks, restart policies, volumes, resource limits |
| `Caddyfile` | TLS and routing: gRPC to the h2c port, everything else to the HTTP port |
| `postgres.conf` | tuned for 16 GB, durability settings left on |
| `.env.example` | domain, secrets, image, backup and mirror settings |
| `policy.toml.example` | access policy: admin, per-agent principals, read/land rules |
| `backup.sh`, `restore.md` | nightly Postgres dump + bucket mirror with retention, and the restore procedure |
| `git-mirror.sh` | keeps plain-git copies of every ref, optionally pushed to a remote |
| `alerts.yml` | Prometheus alert rules |

## Choices, and why

- **SeaweedFS for S3.** It is what antimony's conformance, e2e, soak and
  benchmark suites run against (`docker-compose.yml`, `scripts/dev-services.sh`),
  so it is the only store here known to work with `ant` end to end. MinIO's
  community edition has lost most of its maintenance and admin features and is
  not a good bet for a new long-lived install; Garage is a lean, good S3 for
  a single node and a reasonable alternative, but nobody has run antimony
  against it (point `ANT_S3_*` at it and run `ant-bench soak` first if you want
  to try). SeaweedFS needs its concurrency limits raised, as in
  `scripts/dev-services.sh`; `compose.yaml` does that. Its gotcha: deleting
  objects (what `ant admin gc` does) does not shrink the data volume at once
  (volume vacuum reclaims it over time), and only deleting a whole bucket frees
  space immediately; plan disk for the high-water mark.
- **Caddy** because certificates are automatic (Let's Encrypt, renewal
  included) and it proxies gRPC to an h2c upstream. One hostname serves both
  protocols: gRPC requests are recognised by `Content-Type: application/grpc`
  and go to `ant:7420`, everything else to `ant:7421`.
- **One `ant serve` replica** with `--pg-lock-connections 0`: it is enough for
  a handful of agents by a wide margin (the scorecard's single process
  sustained thousands of lands per second) and removes cross-replica
  coordination. Replicas can be added later; Postgres `max_connections` and the
  S3 store are shared.
- **No published ports except 80/443.** Postgres and S3 are reachable only on
  the compose network. `/metrics` is served on a separate listener bound to
  `127.0.0.1:9100` on the host.

## Sizing

For a handful of agents (up to a few dozen working continuously) on one
host: **4 to 8 vCPU, 16 GB RAM, NVMe/SSD**.

| resource | guidance |
|---|---|
| RAM | `ant serve` ~0.7 GB plus the git export during clones (a full large clone peaks at ~1 GB); Postgres 6 GB (2 GB shared_buffers, rest page cache); SeaweedFS 3 GB. The limits in `compose.yaml` add up to ~12.3 GB. On 8 GB: halve the Postgres/SeaweedFS limits and `shared_buffers` to 1 GB. |
| CPU | landing is bounded by Postgres fsync latency, not CPU; 4 vCPU suffice for light fleets, 8 for many agents cloning and checking out at once |
| disk | objects up to 4 KiB and all metadata live in Postgres, everything else in SeaweedFS. Start at 200 GB of fast local storage for a monorepo of moderate size; the full Linux kernel history is ~160 GB of objects plus ~17 GB of Postgres. Growth driver for agents: lands x average change size (see operator guide, "Storage growth"). |
| backup disk | a **second** volume or storage box at least the size of the data (it holds the Postgres dumps and a copy of the bucket, plus the objects `gc` deleted for the retention period) |
| network | agents talk to the domain over TLS; checkout traffic is bursty. Put the VM near the agents. |

Check your disk first: `pg_test_fsync` (inside the postgres container)
should show a few hundred microseconds per fsync on NVMe; a land is one
durable commit, so landing latency is that number plus a little.

## Step by step: a fresh Ubuntu 24.04 VM (Hetzner, Scaleway, ...)

### 1. Create the machine

- Ubuntu 24.04 LTS, 4 to 8 vCPU / 16 GB (Hetzner: a CCX or CPX 8 vCPU /
  16 GB class; Scaleway: a POP2/PRO2 4 to 8 vCPU / 16 GB class; check the
  current catalogue and prefer dedicated vCPUs for steady latency).
- An SSH key, IPv4 (and IPv6 if you want).
- A second block volume or storage box for backups, mounted at
  `/mnt/backup` (format it `ext4`, add it to `/etc/fstab`).
- **DNS:** an `A` (and `AAAA`) record for e.g. `ant.example.com` pointing at
  the VM's address. Do this before first start so certificate issuance works.

### 2. Firewall

At the provider (Hetzner Cloud Firewall / Scaleway security group): allow
inbound TCP 22 (ideally only from your IP), TCP 80, TCP 443 and UDP 443;
deny everything else. On the host as well:

```sh
sudo ufw default deny incoming && sudo ufw default allow outgoing
sudo ufw allow 22/tcp && sudo ufw allow 80/tcp && sudo ufw allow 443/tcp && sudo ufw allow 443/udp
sudo ufw enable
```

Docker writes its own iptables rules for published ports and bypasses `ufw`;
that is fine here because only 80/443 (and `127.0.0.1:9100`) are published.
Do not add other `ports:` to `compose.yaml` without thinking about it.

### 3. Docker and basics

```sh
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get -y install ca-certificates curl git unattended-upgrades
curl -fsSL https://get.docker.com | sudo sh         # Docker Engine + compose plugin
sudo systemctl enable --now docker
```

### 4. Get the kit and configure it

```sh
sudo mkdir -p /opt/antimony && sudo chown "$USER" /opt/antimony
git clone --depth 1 https://github.com/mapika/antimony.git /opt/antimony/src
cd /opt/antimony/src/deploy

cp .env.example .env && chmod 600 .env
# edit .env: ANT_DOMAIN, ACME_EMAIL, and fresh secrets:
#   for v in POSTGRES_PASSWORD S3_ACCESS_KEY S3_SECRET_KEY; do echo "$v=$(openssl rand -hex 24)"; done
$EDITOR .env

# policy: one principal per agent, hashed tokens (see the file for the format)
cp policy.toml.example policy.toml && chmod 600 policy.toml
TOKEN=$(openssl rand -hex 16); echo "agent-1 token: $TOKEN"
printf %s "$TOKEN" | sha256sum | cut -d' ' -f1      # -> token_sha256 of that principal
$EDITOR policy.toml
```

Create the admin token the same way and keep it somewhere safe. Without a
policy the server is in open mode: `compose.yaml` mounts `policy.toml`, so a
missing file stops the start instead of silently opening the server.

### 5. Start

```sh
docker compose config -q            # validates the file and that every variable is set
docker compose pull
docker compose up -d
docker compose ps                   # postgres, seaweedfs, ant, caddy: all "healthy"
curl -s https://ant.example.com/readyz     # {"status":"ok","postgres":"ok","s3":"ok"}
```

The first start migrates the (empty) schema and creates the bucket; Caddy
fetches the certificate on the first request. If it does not come up, read
`docker compose logs caddy` (DNS or port 80/443 problems are the usual cause).

### 6. Put a repository in

On any machine with the `ant` client (version 0.2.1 or later, see below):

```sh
export ANT_SERVER=https://ant.example.com ANT_HTTP=https://ant.example.com
export ANT_TOKEN=<admin token>
ant ref init main            # an empty first commit, then agents fill it from workspaces
```

or import an existing git repository. Imports run next to the stores, so use
the server's configuration (the `ant` service already carries it in `ANT_*`
variables):

```sh
git clone --bare https://github.com/your-org/monorepo.git /tmp/monorepo.git
docker compose run --rm --no-deps -v /tmp/monorepo.git:/src:ro ant \
  import /src --git-ref refs/heads/main --ref main --policy /etc/antimony/policy.toml
```

`--policy` tags the imported objects with the policy's zones, as
[docs/operations.md](../docs/operations.md#importing-an-existing-repository)
describes. Large imports: mind the disk rules in the contributor guide and
start with a medium repository.

### 7. Schedule the housekeeping

As root, or a user in the `docker` group, with `BACKUP_DIR` (and optionally
`MIRROR_*`) set in `.env`:

```sh
sudo tee /etc/cron.d/antimony >/dev/null <<'EOF'
15 3 * * *   root  /opt/antimony/src/deploy/backup.sh      >> /var/log/antimony-backup.log 2>&1
*/10 * * * * root  /opt/antimony/src/deploy/git-mirror.sh  >> /var/log/antimony-mirror.log 2>&1
# garbage collection: weekly, off-peak (takes at least the 30 min quiesce period)
30 4 * * 0   root  cd /opt/antimony/src/deploy && docker compose run --rm --no-deps -T ant admin gc >> /var/log/antimony-gc.log 2>&1
EOF
sudo ./backup.sh && ls /mnt/backup/antimony        # run the first backup by hand
```

Do a restore drill (see [restore.md](restore.md)) before you rely on the
backups. Rotate the logs (`/etc/logrotate.d`) or switch the jobs to
systemd timers if you prefer; the scripts exit non-zero on failure and write
`$BACKUP_DIR/last-success` / `$MIRROR_DIR/last-success` for monitoring.
`ant admin repack` (monthly, see the operator guide) shrinks history stored
by many small lands; schedule it the same way once the store has some age.

## The plain-git mirror

`git-mirror.sh` fetches antimony's read-only git export
(`https://<domain>/repo.git`, authenticated with a token that may read
everything, e.g. the `mirror` principal of `policy.toml.example`) into a bare
repository and pushes all branches to `MIRROR_PUSH_URL`. Every antimony ref
becomes a branch of the same name (`agents/agent-1/try1`), so the repository
exists as ordinary git even if antimony does not.

antimony stores **SHA-256** git objects. The mirror therefore has two modes:

- `MIRROR_FORMAT=sha1` (default): converts incrementally into a SHA-1
  repository (`git fast-export | git fast-import` with marks files), for
  hosts that only know SHA-1 such as GitHub. Commit ids differ from antimony's;
  trees, authors, dates and messages are identical. It cannot convert a
  history that contains submodule links (the script says so and stops; use
  `sha256`).
- `MIRROR_FORMAT=sha256`: pushes as is, identical ids. The remote must support
  SHA-256 repositories (a bare repository over SSH does; as of this writing
  GitHub does not; check your host).

Without `MIRROR_PUSH_URL` it keeps `$MIRROR_DIR/source.git` (and `sha1.git`),
which is already a `git clone`-able backup. The push is a mirror
(`--prune --force`): deleting a ref in antimony deletes the branch remotely;
it is not an archive.

## How agents connect

Each agent host needs the `ant` client (**0.2.1 or later**: 0.2.0 clients
speak cleartext gRPC and cannot use an `https://` server URL) and:

```sh
export ANT_SERVER=https://ant.example.com   # gRPC, through Caddy
export ANT_HTTP=https://ant.example.com     # objects and tree bundles: faster checkouts
export ANT_TOKEN=<that agent's token>       # one principal per agent
export ANT_AUTHOR_NAME=agent-1 ANT_AUTHOR_EMAIL=agent-1@example.com
export ANT_CACHE=/var/cache/ant             # one shared cache per host
ant ws new /work/agent-1 --sparse services/billing
```

The per-agent loop, exit codes and retry rules are in the
[operator guide](../docs/operator-guide.md#4-agent-workflow-at-fleet-scale):
`ant land` exits 2 on conflict, retries on `aborted` / `unavailable` are safe,
sparse workspaces by default. A private CA is trusted through `SSL_CERT_FILE`.
Humans can `git clone https://x:<token>@ant.example.com/repo.git`.

Many agent hosts in one place, or a slow link: run `ant cache-serve` near them
(`ant cache-serve --cache /var/cache/ant-tier --upstream https://ant.example.com
--listen 0.0.0.0:7422 --auth forward`) and give those agents
`ANT_HTTP=http://<tier>:7422`; gRPC still goes to the origin. See the operator
guide's "Cache tier".

## Monitoring

- **Metrics:** `ant serve` exposes Prometheus metrics on `ANT_METRICS_LISTEN`
  (`0.0.0.0:9100` inside the container), published only on the host's
  loopback. Scrape from a Prometheus on the same host, or over a private
  network / SSH tunnel:

  ```yaml
  scrape_configs:
    - job_name: antimony
      static_configs: [{targets: ["127.0.0.1:9100"]}]
  rule_files: ["/opt/antimony/src/deploy/alerts.yml"]
  ```

  Metric families and dashboards: operator guide section 5.
- **Minimal alert list** (`alerts.yml`): `up == 0`; store errors; aborted
  lands; land p99 above 250 ms; Postgres pool above 90 percent; load
  shedding; RPC server errors; history index lag. Plus the things only the host
  knows (not in the compose stack): free disk on the Docker volumes and on
  `BACKUP_DIR` (below 15 percent), age of `$BACKUP_DIR/last-success` (older than
  26 hours), an external probe of `https://<domain>/readyz` (catches DNS,
  certificate and firewall breakage), and containers restarting
  (`docker compose ps`, `RestartCount`).
- **Health:** `/healthz` (process up) and `/readyz` (Postgres and S3 reachable)
  on the HTTP port; compose uses `/readyz` for the `ant` healthcheck.
- **Logs:** JSON from `ant`, one line per slow request with the principal;
  `docker compose logs -f ant`. Docker's json-file driver keeps 5 x 20 MB per
  service.

## Upgrading

1. Read the [changelog](../CHANGELOG.md) and
   [docs/operations.md, "Upgrading"](../docs/operations.md#upgrading) (schema
   versions, rolling-restart constraints, `ant admin schema`).
2. Run `./backup.sh`. A schema migration cannot be undone by downgrading the
   binary; the way back is [restore.md](restore.md).
3. Set `ANT_IMAGE` in `.env` to the new tag, then
   `docker compose pull ant && docker compose up -d ant`. The new server
   migrates the schema on start; Caddy holds and retries requests for up to 30
   seconds meanwhile, and clients retry idempotent lands.
4. Check `docker compose ps`, `/readyz`, `ant log main -n 3`.

Postgres and SeaweedFS images are pinned to a major/minor tag; move them
deliberately (Postgres major upgrades need `pg_upgrade` or a dump/restore: use
the `backup.sh` dump and `restore.md` into a new volume). Caddy
(`caddy:2-alpine`) can be pulled whenever.

## Operating notes

- **Edit the policy:** change `policy.toml`, `docker compose restart ant`.
  Path-rule changes need `ant admin reindex-acl main --history`
  ([operations.md](../docs/operations.md#access-policy)).
- **Run an admin command** with the server's own configuration:
  `docker compose run --rm --no-deps ant admin schema` (also `gc`, `repack`,
  `reindex-acl`).
- **A shell in Postgres:** `docker compose exec postgres psql -U ant ant`
  (schema `ant`).
- **Rotate a token:** hash the new one into `policy.toml`, restart `ant`, give
  the agent the new token.
- **Disk full:** Postgres stops accepting writes and `ant` goes unready.
  Free space (`docker system prune` removes only unused images; do not delete
  volumes), or grow the disk. SeaweedFS reserves 5 percent
  (`-volume.minFreeSpacePercent`).

## What was validated, and what was not

Validated on the development machine with real binaries:

- `docker compose config` accepts `compose.yaml` (Docker 29 client); the YAML
  parses; `shellcheck` is clean on the scripts; the Caddyfile passes
  `caddy validate` (Caddy 2.8.4).
- Caddy in front of a live `ant serve` with a policy: `ant` gRPC over TLS and
  HTTP/2 (`ref list`, `log`), a `ref watch` stream held open for 10 s,
  `git ls-remote` and `git clone` through the proxy, 404 for `/metrics` and
  `/debug/*`, HTTP to HTTPS redirect, wrong tokens rejected.
- The policy example: parsed by the server; admin-only `main` moves, agents
  landing on `main` and on their own `agents/<id>/*` ref but not on someone
  else's, a reader unable to write, and the mirror principal reading the git
  export.
- SeaweedFS started with exactly the flags and identity file of `compose.yaml`:
  `ant import` works with the configured credentials and is rejected with
  wrong ones; the master health endpoint used by the healthcheck answers 200.
- `postgres.conf` is accepted by PostgreSQL 17.11 (`postgres -C` on every
  tuned setting).
- `backup.sh` and the restore procedure end to end (see `restore.md`).
- `git-mirror.sh` against a policy-protected server in both modes, including
  incremental runs, a new branch, a deleted branch, content equality of the
  converted copy, and rejection of a bad token.
- `ant` over `https://`: this needed a client fix (the 0.2.0 client could not
  speak TLS); it is in the same branch.

**Not validated** (no Docker daemon on the development machine): starting the
compose stack itself, so the container healthchecks (`bash` /dev/tcp probe in
the `ant` image, `wget`/`nc` in the SeaweedFS image), the `user: 65534`
read-only `ant` container, the Postgres container reading `postgres.conf`,
and real certificate issuance have not been run; neither has a Hetzner or
Scaleway VM. `postgres.conf` is tuned from the operator guide's numbers and
standard guidance, not benchmarked on a deployment. On the first start, watch
`docker compose ps` and the logs, and expect to adjust something small.
