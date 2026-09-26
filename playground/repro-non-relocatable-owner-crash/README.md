# Reproducer: non-relocatable actor ownership survives owner crash

This sample reproduces a stale cluster name claim left by a named actor created
with `WithRelocationDisabled()` when the process hosting that actor dies without
running graceful `ActorSystem.Stop()`.

The sample deliberately uses two OS processes. The survivor runs in the parent
process and the original actor owner runs in a child process. Once the actor is
visible remotely, the parent terminates the owner with `Process.Kill()`, waits
until cluster membership reports that the peer is gone, and then tries to reuse
the same stable actor name.

`WithReplicaCount(2)` is intentional: the registry record must survive the
physical loss of its original node so the sample exercises crash cleanup rather
than partition loss.

## Run

```bash
go run ./playground/repro-non-relocatable-owner-crash
```

On the broken behavior the program exits with status 1 and prints:

```text
REPRO (broken): the dead non-relocatable actor still blocks clean reuse of its stable name
```

Status 0 means the dead incarnation stopped owning the name and a new
non-relocatable actor could claim it. Status 2 means the two-node setup itself
failed.
