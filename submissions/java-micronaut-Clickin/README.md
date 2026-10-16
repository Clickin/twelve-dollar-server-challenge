# Java 25 + Micronaut + SQLite

| | |
| --- | --- |
| Language | Java 25 (Eclipse Temurin `25.0.2+10`, SHA-256 verified by `install.sh`) |
| Framework | Micronaut 5.2.15 (Platform 5.2.2) on Netty 4.2.19.Final (`nio`) |
| JSON | Micronaut Serialization (Serde) 3.2.4 with **build-time bean introspection**: `@Serdeable` records are processed by `micronaut-serde-processor` at compile time, so there is no runtime reflection and no `jackson-databind`. Jackson annotations (`@JsonPropertyOrder`) fix the field order, and only the Jackson 3.2.3 streaming core reads and writes the JSON text. It handles request bodies and the small write, health, and error responses; feed and post reads are JSON bytes rendered by SQLite and bypass it. |
| SQLite driver | `org.xerial:sqlite-jdbc` 3.53.4.0 |
| Build | Gradle 9.5.0 wrapper (checksum-verified), Shadow 9.6.1, dependencies pinned in `gradle.lockfile` |
| **Nginx or direct** | **Direct** on `HOST:PORT` (`0.0.0.0:80` on the benchmark VM, default `127.0.0.1:3000`) |

## Run

```sh
sudo bash submissions/java-micronaut-Clickin/install.sh
bash seed/make-seed.sh
bash test/run.sh submissions/java-micronaut-Clickin
```

`build.sh` runs the JUnit suite and builds `build/libs/twelve-dollar-micronaut.jar`. `start.sh` runs that JAR in the foreground using `SQLITE_PATH`, `JWT_SECRET`, `HOST`, and `PORT`. Startup fails if `$SQLITE_PATH` is missing; the server never creates or alters the schema.

## Design

This submission stays inside standard Micronaut and `sqlite-jdbc`—no custom HTTP parser, raw socket server, or FFM binding—and removes per-request work on one vCPU:

- **Single NIO event loop + single DB owner.** One Netty `nio` event loop handles HTTP parsing, Micronaut routing, JWT verification, and input validation. One dedicated `db-owner` thread owns a single SQLite connection with all statements (`SELECT 1`, feed, post, create, like, post-exists, `BEGIN IMMEDIATE`, `COMMIT`, `ROLLBACK`) prepared at startup. JDBC never blocks the event loop.
- **Opportunistic group commit.** `db-owner` blocks on a bounded 4,096-slot queue and drains up to 256 already-waiting operations per pass, with no batch timer or minimum batch size. If a batch contains any write, the whole batch runs inside `BEGIN IMMEDIATE … COMMIT`. Every response in the batch—including reads that observed uncommitted writes—is completed only after `COMMIT` succeeds. Unexpected SQL or commit failures roll back and fail the batch with 500; a full queue returns 503.
- **SQLite-rendered JSON bytes.** Feed and single-post reads return a single UTF-8 `BLOB` built by SQLite (`json_object` and `group_concat(... ORDER BY created_at DESC, id DESC)`). `SqlJsonBodyWriter` passes that `byte[]` directly to Micronaut's `ByteBodyFactory`, skipping Java row mapping and Jackson re-serialization on the read path (92% of traffic). Likes use `INSERT INTO likes ... SELECT ?, id FROM posts WHERE id = ? ON CONFLICT DO NOTHING` and only query `posts` when zero rows are inserted to distinguish duplicates (`200`) from missing posts (`404`).
- **Per-request HS256 verification.** Every authenticated request verifies the signature with a thread-local pre-keyed `HmacSHA256` `Mac`, constant-time digest comparison (`MessageDigest.isEqual`), and fresh `alg`, `exp`, `sub`, and `username` checks. Verified tokens and query results are never cached.
- **SQLite pragmas.** `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=1000`, `cache_size=-8192` (8 MiB), and `mmap_size=268435456` (256 MiB).

### JVM & GC (`start.sh`)

```
-XX:ActiveProcessorCount=1 -XX:+UseSerialGC
-Xms768m -Xmx768m -Xmn384m -XX:MaxTenuringThreshold=3
-XX:MetaspaceSize=64m -XX:MaxMetaspaceSize=64m -XX:+AlwaysPreTouch
```

At 56k–64k open keep-alive connections, live old-generation footprint is ~220–250 MB (~4 KB per connection) and metaspace is ~31 MB:

- **Serial GC over G1.** On 1 vCPU there is no spare core for concurrent marking threads. At 56,000 VUs on the same 768 MB / 384 MB heap, G1 spent 3.0–3.3 s in GC pauses (max 181–197 ms) over a 5-minute hold versus 2.1–2.3 s (max 85–100 ms) with Serial GC.
- **Fixed 384 MB young generation (`-Xmn384m`) and `-XX:MaxTenuringThreshold=3`.** With a dynamic `-Xms128m -Xmx512m` heap, `DefNew` stayed near its initial 38 MB, overflowed survivor spaces during the ramp, promoted ~99 MB of short-lived objects into the 341 MB old generation, and triggered a ~936 ms Full GC at 56,000 VUs. Fixing the young generation at 384 MB with tenuring threshold 3 keeps steady-state holds in minor collections only and raised the passing level on `syd1` from 54,000 to 62,500 VUs.

## Measurements (`syd1`, `s-1vcpu-2gb`)

**Setup.** Server: DigitalOcean `s-1vcpu-2gb` (1 vCPU, 2 GB RAM, no swap, Ubuntu 24.04, Direct on port 80). Load: two 4 vCPU droplets in the same `syd1` VPC, each running an `rlt`-based Rust implementation of `bench/load.js` (verified against k6 1.3.0 for identical request mix, think times, headers, and server-side CPU/req; the ramp follows k6's `ramping-vus` activation schedule). This avoids k6's memory use at tens of thousands of VUs, and a single source IP can hold only about 64.5k connections to one destination port, so the VUs are split in half across the two generators.

**Procedure** (the README's scoring procedure): one server process for the whole search, a 1,000-VU 2-minute warm-up that is not scored, then 2,500 VUs doubling until a run fails, narrowing to within 250 VUs, and a confirmation hold at the best passing level (stepping down by 250 on failure). Every run is the official shape: 60 s ramp, 5-minute hold, 30 s ramp-down. Pass: p95 < 500 ms, p99 < 1 s, errors < 1% (a run fails if either generator fails). Hold columns show the worse of the two generators.

**Confirmed maximum: 79,500 VUs** (hold p99 232 ms, 0% errors, 92% CPU, 110 µs per request). 80,000 VUs failed (p99 1.4 s).

| Step | VUs | Result | Hold req/s | Hold p95 (ms) | Hold p99 (ms) | Errors | Server CPU | CPU / req | Full GC |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| double | 2,500 | pass | 264 | 1 | 6 | 0.00% | 10% | 384 µs | 0 |
| double | 5,000 | pass | 529 | 1 | 6 | 0.00% | 14% | 268 µs | 0 |
| double | 10,000 | pass | 1,060 | 1 | 10 | 0.00% | 24% | 224 µs | 0 |
| double | 20,000 | pass | 2,115 | 2 | 15 | 0.00% | 42% | 196 µs | 0 |
| double | 40,000 | pass | 4,231 | 7 | 24 | 0.00% | 65% | 153 µs | 0 |
| double | 80,000 | **fail** | 8,452 | 387 | 1,436 | 0.33% | 92% | 109 µs | 1 |
| narrow | 60,000 | pass | 6,345 | 29 | 75 | 0.00% | 82% | 130 µs | 1 |
| narrow | 70,000 | pass | 7,399 | 58 | 104 | 0.00% | 87% | 118 µs | 1 |
| narrow | 75,000 | pass | 7,923 | 77 | 134 | 0.00% | 90% | 114 µs | 1 |
| narrow | 77,500 | pass | 8,185 | 82 | 163 | 0.00% | 91% | 112 µs | 1 |
| narrow | 78,750 | pass | 8,311 | 117 | 218 | 0.00% | 92% | 111 µs | 1 |
| narrow | 79,250 | pass | 8,343 | 291 | 747 | 0.27% | 93% | 112 µs | 1 |
| narrow | 79,500 | pass | 8,390 | 136 | 316 | 0.00% | 92% | 109 µs | 1 |
| narrow | 79,750 | pass | 8,390 | 326 | 736 | 0.16% | 93% | 111 µs | 1 |
| confirm | 79,750 | **fail** | 8,402 | 742 | 2,087 | 1.13% | 93% | 111 µs | 1 |
| confirm | 79,500 | pass | 8,394 | 127 | 232 | 0.00% | 92% | 110 µs | 1 |

Notes:

- **Raised descriptor limit.** This search ran the server with `LimitNOFILE=131072`, because more than 65,535 connections need more descriptors than the challenge's default per-process limit of 65,535 (it reached 70,018 open descriptors at 70,000 VUs). With the default limit, one process holds at most about 65.5k connections, so the score cannot exceed that regardless of CPU. On the default limit, with a single generator and a fresh JVM per step, **64,000 VUs** passed (6,766 req/s, p95 48 ms, p99 283 ms, 0% errors, 82% CPU, 120 µs/req, 0 Full GCs). A 65,000-VU run failed on the generator's side (it stopped at 64,512 connections, the source-port limit), while the server peaked at 64,530 descriptors.
- **Near the limit it is flaky.** Between 79,250 and 79,750 VUs, p99 swings between 232 ms and 2.1 s from run to run (79,750 passed its search run, then failed its confirmation run), with the CPU at 92–93%. A single Full GC of 0.5–1.0 s per run appears from 60,000 VUs up, because earlier runs' dead connection state remains in the old generation of the same JVM.
- **Host variance.** The same JAR cost 225 µs per request on an `sgp1` droplet and 110–150 µs on `syd1` droplets; steal spikes and host differences move the ceiling by several thousand VUs. The 62,500-VU pass on an earlier `syd1` droplet pair, where 64,000 failed at 135 µs per request, is the same JAR on a busier host.
- **Not an official score.** This is a self-run search with a Rust generator, not the k6 run on the maintainer's droplet.

## Verification

- **Official suite:** `test/run.sh submissions/java-micronaut-Clickin` passes **42/42**.
- **Unit & batch-durability tests (`build.sh`):** 8 JUnit tests in `BehaviorTest` and `DbExecutorTest` verify JWT edge cases, auth-before-validation ordering, Unicode length limits, post-commit read/write publication, batch rollback on statement or `COMMIT` failure, admission stop on lost transactions, 503 queue-overflow rejection, and graceful shutdown draining.
- **Supplemental HTTP & SQLite integrity checks:**

```sh
# Against a running server on a disposable seed copy:
python3 submissions/java-micronaut-Clickin/checks/verify-extra.py

# Standalone (starts its own server and temporary database):
python3 submissions/java-micronaut-Clickin/checks/verify-feed.py
```

Every benchmark and verification run also checked `PRAGMA integrity_check` (`ok`) and `PRAGMA foreign_key_check` (empty) after shutdown. `checks/mixed.lua` is included for local `wrk` A/B runs (100 feeds : 100 post reads : 15 likes : 2 creates).
