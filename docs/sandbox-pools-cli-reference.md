# Sandbox Pools CLI Reference

Manage warm-container fleets backing fast sandbox claims using the
`tl sbx pool` subcommand group.

---

## Authentication

All `tl sbx pool` commands require authentication. Either:

- Run `tl login` once — credentials are saved to `~/.config/tensorlake/credentials.toml`
- Or set `TENSORLAKE_API_KEY=<your-key>` in your environment

---

## Concepts

A **sandbox pool** keeps a configurable number of pre-built containers warm
so that calling `tl sbx pool claim` returns a running sandbox in
sub-second time on the warm path. Network and proxy access settings are
applied at the pool level and inherited by every claimed sandbox.

| Concept | What it does |
|---|---|
| `warm_containers` | How many idle containers to keep ready at all times. The scheduler refills this count after each claim. |
| `max_containers` | Hard cap on total containers (warm + claimed). Once reached, claims wait until a slot opens. |
| `min_containers` | Server-derived (read-only). Reflects the floor the scheduler maintains internally. |
| Pool deletion with `--force` | Terminates every claimed sandbox before removing the pool. Without `--force`, the API returns 409 if the pool still has active sandboxes. |

---

## Commands

### `tl sbx pool create`

Create a new sandbox pool.

```
tl sbx pool create [OPTIONS]
```

**Options**

| Option | Description |
|---|---|
| `-i, --image <IMAGE>` | Sandbox image name (e.g. `tensorlake/ubuntu-minimal` or a registered image) |
| `-c, --cpus <N>` | CPUs per container (default: `1.0`) |
| `-m, --memory <MB>` | Memory in MB per container (default: `1024`) |
| `--disk_mb <MB>` | Ephemeral disk in MB per container (default: `1024`) |
| `-t, --timeout <SECS>` | Per-sandbox timeout (`0` or omitted = no timeout) |
| `-e, --entrypoint <PART>` | Entrypoint command parts (repeatable) |
| `--max-containers <N>` | Total-container cap |
| `--warm-containers <N>` | Number of warm containers to maintain |
| `-x, --expose <PORT>` | Expose a TCP port (>9501) on every claimed sandbox (repeatable) |
| `-N, --no-internet` | Block all outbound internet access |
| `-A, --network-allow <CIDR>` | Allow outbound traffic to this IP/CIDR (repeatable) |
| `-D, --network-deny <CIDR>` | Deny outbound traffic to this IP/CIDR (repeatable) |

> Setting any `--expose` port implicitly enables unauthenticated proxy access
> for those ports — there's no way to expose a port and still require auth.

**Example**

```bash
tl sbx pool create \
  --image tensorlake/ubuntu-minimal \
  --warm-containers 3 \
  --max-containers 10 \
  --expose 8080 \
  --no-internet \
  --network-allow 10.0.0.0/8
```

Prints the new `pool_id` on success.

---

### `tl sbx pool ls`

List pools in the current namespace.

```
tl sbx pool ls [--quiet]
```

| Option | Description |
|---|---|
| `-q, --quiet` | Print only pool IDs, one per line (no table formatting) |

The default table shows pool ID, image, CPUs, memory, warm/max counts,
timeout, and creation time.

---

### `tl sbx pool get`

Show full pool detail and the current container list (warm + claimed).

```
tl sbx pool get <POOL_ID>
```

Containers are rendered in a table with their state (`Idle` or `Running`),
the claiming sandbox ID (if any), and the executor ID hosting them.

---

### `tl sbx pool update`

Patch-like update — only fields you pass are changed. The CLI fetches the
current pool state, merges your overrides, and `PUT`s the full body, so
you don't need to repeat unchanged fields.

```
tl sbx pool update <POOL_ID> [OPTIONS]
```

| Option | Description |
|---|---|
| `-i, --image <IMAGE>` | Replace the pool image |
| `-c, --cpus <N>` | Change CPUs per container |
| `-m, --memory <MB>` | Change memory per container |
| `--disk_mb <MB>` | Change ephemeral disk per container |
| `-t, --timeout <SECS>` | Change per-sandbox timeout |
| `-e, --entrypoint <PART>` | Replace entrypoint (only when at least one part is passed) |
| `--max-containers <N>` | Change the total-container cap |
| `--warm-containers <N>` | Change the warm-container target |

> Network and proxy access settings (`--no-internet`, `--allow-unauthenticated-access`,
> `--expose`, `--network-allow`/`-deny`) are not exposed on `update` in v1
> because clap bare-flag args can only flip to true. To change those, recreate
> the pool with the new settings, or use the Python SDK's `update_pool()`
> kwargs.

**Example**

```bash
tl sbx pool update <pool-id> --warm-containers 5
```

---

### `tl sbx pool rm`

Delete a sandbox pool.

```
tl sbx pool rm <POOL_ID> [--force]
```

| Option | Description |
|---|---|
| `-f, --force` | Skip the confirmation prompt and terminate any sandboxes still claimed from this pool |

Behavior:

- **Empty pool**: deletes immediately, no prompt.
- **Active sandboxes, TTY**: prompts before deletion. Choose `y` to terminate them along with the pool.
- **Active sandboxes, non-TTY** (CI, scripts): exits with an error if `--force` is missing, instead of hanging on a prompt.
- **`--force`**: terminates active sandboxes and deletes the pool unconditionally.

---

### `tl sbx pool claim`

Claim a sandbox from the pool. The warm path is sub-second; if no warm
container is available, the scheduler creates one on demand.

```
tl sbx pool claim <POOL_ID> [--no-wait]
```

| Option | Description |
|---|---|
| `--no-wait` | Return immediately with the sandbox ID rather than blocking until it transitions to `running` |

By default `claim` waits for the sandbox to reach `running` before
returning, so the printed ID is always usable for follow-up commands.
This differs from `tl sbx create`, where `--wait` is opt-in — pools
exist for fast claims, so blocking briefly is the expected mode.

**Example**

```bash
sandbox_id=$(tl sbx pool claim <pool-id>)
tl sbx ssh "$sandbox_id"
```

---

## See also

- [`tl sbx create`](../crates/cli/src/commands/sbx/create.rs) — create individual sandboxes (no pool)
- [Python SDK](../src/tensorlake/sandbox/client.py) — `client.create_pool()`, `client.claim()`, etc.
