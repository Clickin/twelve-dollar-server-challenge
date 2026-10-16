# Java 25 / Micronaut

A feed API built with Micronaut's HTTP routing and serialization, JDBC and SQLite,
tuned for one vCPU **inside Micronaut**: no custom HTTP parser, no custom socket
transport, no replacement of the SQLite JDBC driver.

| Component | Version |
| --- | --- |
| JDK | Eclipse Temurin 25.0.2+10 |
| Micronaut platform / core | 5.2.2 / 5.2.15 |
| Netty transport | portable NIO, 4.2.19.Final |
| SQLite driver | `org.xerial:sqlite-jdbc` 3.53.4.0 (JDBC) |
| Gradle | 9.5.0, checksum-verified wrapper |
| Shadow packaging plugin | 9.6.1 |

Dependencies are pinned in `gradle.lockfile`. **Direct, no Nginx**: the service listens
on `HOST`/`PORT` itself. `start.sh` runs the JAR in the foreground and reads
`SQLITE_PATH`, `JWT_SECRET`, `HOST` and `PORT` (defaults: `127.0.0.1:3000`, secret
`twelve-dollar-challenge`). Startup fails if the database file is missing; the app never
creates or changes the schema.

## Run

```sh
sudo bash submissions/java-micronaut-Clickin/install.sh   # JDK, as root
bash seed/make-seed.sh
bash test/run.sh submissions/java-micronaut-Clickin       # build.sh runs the unit tests
```

`build.sh` builds one self-contained `build/libs/twelve-dollar-micronaut.jar` that
`start.sh` runs. Nothing prebuilt is shipped.

## What it does

- **One event loop, one DB owner.** A single Netty NIO event loop does Micronaut routing,
  authentication and validation. One SQLite connection, with every statement prepared at
  startup, is owned by a FIFO worker thread (`db-owner`). JDBC never blocks the event loop.
- **Group commit, response after commit.** The worker drains up to 256 already-queued
  operations (no timer, no minimum batch). A batch that contains a write runs in one
  `BEGIN IMMEDIATE` ... `COMMIT`; every response in it, including reads that saw those
  writes, is published only after the commit. An unexpected SQL or commit error rolls back
  and fails the whole batch; writes are never retried. A full 4,096-operation queue
  returns 503.
- **SQLite builds the JSON.** Feed and post reads return one UTF-8 BLOB produced by SQLite
  (`json_object`, ordered `group_concat`), written to the response without Java row
  objects or re-serialization. Likes use `INSERT ... SELECT` on the existing unique
  constraint.
- **JWT.** Every authenticated request verifies HS256 with a thread-local keyed `Mac`
  and a constant-time comparison, and re-checks algorithm, expiry and claims. Nothing
  about a verified token is cached.
- **SQLite settings.** WAL, `synchronous=NORMAL`, foreign keys, 8 MiB page cache, 256 MiB
  mmap, automatic checkpoints left on.
- **No response or query-result caching**, and the schema is untouched.

### GC and heap (`start.sh`)

```
-XX:ActiveProcessorCount=1 -XX:+UseSerialGC
-Xms768m -Xmx768m -Xmn384m -XX:MaxTenuringThreshold=3
-XX:MetaspaceSize=64m -XX:MaxMetaspaceSize=64m -XX:+AlwaysPreTouch
```

With about 56k open connections the live heap is about 250 MB (roughly 4 KB per
connection). The settings aim for steady state with minor collections only:

- A fixed 384 MB young generation, because the default young generation stays near its
  initial 38 MB and promotes survivors early. The old generation then fills and triggers
  a Full GC at the end of the ramp.
- `MaxTenuringThreshold=3` limits how long objects are copied between survivor spaces. Together
  with the 384 MB young generation it gave the shortest pauses measured at 56k connections
  (longest about 85-100 ms, versus about 157 ms for a 640 MB young generation).
- Metaspace is sized up front (steady state is about 31 MB), so growing it never needs a
  Full GC.
- Serial rather than G1/ZGC/Shenandoah: on one vCPU there is no spare core for concurrent
  work. G1 with the same young size measured roughly 30-70% more total pause time at the same
  CPU per request.

Settings and flags only affect the JVM process; nothing changes the kernel or host.

## Measurements

Measured on DigitalOcean droplets in `syd1`: the server is `s-1vcpu-2gb` (the challenge
box), and the load generator ran on a separate 4 vCPU droplet in the same VPC. The generator
is a Rust re-implementation of `bench/load.js` (same endpoints, request mix, think time,
ramp and a five-minute hold) rather than k6, to avoid k6's memory use at tens of thousands
of VUs and the source-port limits of one client IP. The pass criteria are the challenge's:
p95 < 500 ms, p99 < 1000 ms, failure rate < 1%.

| VUs | Result | p99 during hold | Server CPU | CPU per request |
| ---: | --- | ---: | ---: | ---: |
| 58,000 | pass | 143 ms | 83% | 135 µs |
| 61,000 | pass | 358 ms | 88% | 136 µs |
| 62,500 | pass | 304 ms | 89% | 135 µs |
| 64,000 | fail | 2,489 ms | 89% | 131 µs |

**Highest passing step: 62,500 VUs; first failure: 64,000** (searched at 2k resolution, one
JVM per step with a two-minute warm-up). This is a self-run capacity search with an
equivalent workload, not the official k6 run, and the 64k failure came with no Full GC:
CPU headroom ran out.

Caveats:

- Droplet CPU steal varies by host and by minute; the same JAR measured 225 µs per request
  on an `sgp1` droplet and about 135-150 µs on `syd1`. Results here are from `syd1`.
- Across three consecutive 56k runs on one JVM, the second and third runs each had one
  Full GC (about 0.8 s) near the end of the ramp, caused by the previous run's dead
  connection state still sitting in the old generation. All three runs passed; about 0.5%
  of requests in those runs took over 500 ms.
- The per-process descriptor limit (`LimitNOFILE=65535`) caps one process at roughly
  65.5k connections.

## Checks

`build.sh` runs the Java tests; the upstream runner covers the 42 API checks. Extra scripts
in `checks/` cover Unicode, concurrent likes and creates, committed-write visibility, feed
ordering, SQL failure recovery and empty feeds:

```sh
# Server running on a disposable seed copy:
python3 submissions/java-micronaut-Clickin/checks/verify-extra.py
# Starts its own server and temporary database:
python3 submissions/java-micronaut-Clickin/checks/verify-feed.py
```

`checks/mixed.lua` is a closed-loop `wrk` script (100 feed : 100 post reads : 15 likes :
2 creates) for local A/B comparisons; it is not the official k6 score.
