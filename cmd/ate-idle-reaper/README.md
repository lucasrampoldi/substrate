# ate-idle-reaper

`ate-idle-reaper` is an automated scale-to-zero background service and CLI tool for **Agent Substrate**. It continuously monitors actors running in worker sandboxes (gVisor) and suspends any actor that has exceeded a configurable inactivity window (`--idle-timeout`).

When suspended, an actor releases its worker memory and CPU resources while gVisor checkpoints its state to Google Cloud Storage. When traffic arrives through `atenet-router`, Envoy's parking lot transparently resumes the actor.

---

## Architecture

```mermaid
flowchart TD
    A["ate-idle-reaper"] -->|1. ListActors| B["ate-api-server (gRPC)"]
    A -->|2. Optional: statusz| C["atenet-router (Traffic Activity)"]
    A -->|3. Evaluate Inactivity| D{"Idle > Timeout?"}
    D -- "No" --> E["Keep Actor RUNNING"]
    D -- "Yes" --> F["SuspendActor()"]
    F --> G["Actor moves to ACTOR_STATE_SUSPENDED<br/>(Snapshot saved to GCS)"]
    H["New Ingress HTTP Request"] --> C
    C -->|Wake-on-Request| B
    B -->|ResumeActor()| I["Actor restored into Worker Pod"]
```

---

## Features

* **Dual Deployment Modes:**
  * **Daemon Mode:** Continuous reconciliation ticker loop (`--check-interval=1m`).
  * **CLI Tool / CronJob:** Single-pass execution via `kubectl ate reap actor`.
* **Safe Dry-Run Mode:** Test what would be reaped before making changes (`--dry-run`).
* **Router Traffic Cross-Check:** Query `atenet-router` `/statusz?format=json` to ensure in-flight or recent requests reset the idle counter.
* **Granular Filtering:** Target all atespaces or scope to a single atespace (`-a <atespace>`).

---

## Usage

### 1. As a `kubectl ate` command:

```bash
# Dry-run: see what would be reaped (default 10m idle timeout)
kubectl ate reap actor --dry-run -a openclaw

# Execute immediate reap for actors idle > 5 minutes
kubectl ate reap actor --idle-timeout 5m -a openclaw

# Run continuously as a background loop
kubectl ate reap actor --daemon --interval 1m --idle-timeout 10m -a openclaw
```

### 2. As a standalone daemon:

```bash
ate-idle-reaper \
  --idle-timeout=10m \
  --check-interval=1m \
  --atespace=openclaw \
  --router-status-url=http://atenet-router.ate-system.svc:4040/statusz?format=json
```

---

## Flags Reference

| Flag | Default | Description |
|---|---|---|
| `--idle-timeout` | `10m` | Duration of inactivity before an actor is considered idle. |
| `--check-interval` / `--interval` | `1m` | Frequency of reaper evaluation cycles. |
| `-a, --atespace` | `""` | Filter by atespace (empty = all atespaces). |
| `--dry-run` | `false` | Log intended actions without invoking `SuspendActor`. |
| `--daemon` | `false` | Run continuously in a loop instead of a single pass. |
| `--router-status-url` | `""` | Optional URL to router `/statusz` for traffic verification. |
