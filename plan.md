# LQFT Industrial Roadmap

## Goal

Turn the current split design of `LQFT` and `MutableLQFT` into one practical, unique, industrial data structure:

- fast mutable writes
- immutable snapshots
- structural sharing across versions
- subtree and value deduplication
- predictable read performance
- measurable benchmark and memory discipline

The target product is not "another dict". The target product is a versioned, deduplicating key-value map with a mutable head and a persistent canonical body.

---

## Hard Constraints

These constraints should guide the design and prevent chasing impossible asymptotic goals.

- Total memory for storing `n` distinct exact items cannot be `O(log n)`. Exact general-purpose storage is at least `Omega(n)`.
- End-to-end key operations cannot be true `O(1)` for arbitrary strings because hashing the key is `O(|key|)`.
- Caching improves constants and hot-path behavior. It does not change the fundamental worst-case complexity.
- Deduplication is most valuable across similar versions, repeated values, and repeated subtrees.
- Billion and trillion scale usually requires paging, disk, sharding, or distribution. A single-process in-memory structure is not enough by itself.

---

## Product Definition

Redefine the project as a single logical structure:

`LQFTMap = Mutable Delta + Canonical Persistent Tree + Snapshot System`

### Public API Direction

Core API:

- `put(key, value)`
- `get(key)`
- `delete(key)`
- `contains(key)`
- `snapshot()`
- `rollback(snapshot)`
- `diff(snapshot_a, snapshot_b)`
- `compact()`
- `stats()`

Optional API:

- `begin_batch()`
- `commit_batch()`
- `seal_reads()`
- `merge(snapshot)`
- `export_snapshot()`
- `load_snapshot()`

`MutableLQFT` should eventually stop being a separate product. It should become the transient write mode inside `LQFTMap`.

---

## Architecture

### 1. Mutable Head

Purpose:

- absorb writes cheaply
- keep overwrite and delete fast
- isolate hot mutation from expensive canonicalization

Recommended implementation:

- native open-addressed hash table
- Robin Hood or similar probing if lookup clustering becomes a problem
- tombstones for deletes
- per-instance state, not global shared state

Operations:

- `put`: write to mutable head
- `delete`: write tombstone
- `get`: check mutable head first

### 2. Canonical Persistent Body

Purpose:

- hold committed state
- provide structural sharing across snapshots
- canonicalize identical subtrees

Recommended implementation:

- wide hash-partitioned persistent trie
- bitmap-indexed sparse child arrays
- leaf records with key fingerprint, full key hash, and value reference
- node interning keyed by node signature

Operations:

- `snapshot`: merge mutable head into a new canonical root
- `rollback`: switch active root to an older snapshot handle
- `diff`: compare committed roots and their changed paths

### 3. Value Interning Layer

Purpose:

- reduce memory from repeated values
- improve sharing across committed versions

Recommended behavior:

- intern repeated values adaptively
- avoid forcing pooling on unique one-off values if profiling shows a regression

### 4. Snapshot System

Purpose:

- preserve historical roots cheaply
- enable rollback, diff, and consistent reads

Representation:

- root pointer or root handle
- generation number
- metadata such as creation timestamp, delta size, and parent snapshot id

---

## Why This Is Unique

The unique part is not the mutable hash table alone.

The unique value is the combination of:

- mutable writes
- persistent snapshots
- subtree deduplication
- pooled repeated values
- low incremental memory across nearby versions

The correct positioning is:

"A versioned, deduplicating, snapshot-friendly key-value structure with fast mutable writes and structurally shared persistent reads."

---

## Complexity Targets

All complexity claims should be written honestly, with key hashing included.

Let:

- `n` = number of committed items
- `m` = size of current mutable delta
- `B` = trie branching factor
- `|key|` = key length in bytes or characters

### Mutable Head

- `put(key, value)`: expected `O(|key| + 1)`
- `get(key)`: expected `O(|key| + 1)`
- `delete(key)`: expected `O(|key| + 1)`
- worst case for probing: `O(n)`
- space: `O(m)`

### Persistent Body

- committed lookup: `O(|key| + log_B n)`
- with fixed hash width and fixed branching factor, practical depth is bounded and small
- extra committed memory per updated key: `O(log_B n)`
- total committed memory for one live version: `O(n)`

### Snapshots

- `snapshot()` for delta size `m`: roughly `O(sum(|key_i| + log_B n))` over all modified keys
- memory added by a small snapshot: proportional to changed paths, not full dataset copy
- multiple similar snapshots: approximately `O(n + delta_changes * log_B n)` ignoring constant-factor dedup wins

### Caching

- cache hits may make hot-path reads feel constant time in practice
- theoretical worst-case complexity does not change

---

## Performance Recording Standard

Every architectural change should be benchmarked and recorded using the same format.

### Record For Every Benchmark Run

- date
- commit hash or local revision note
- Python version
- compiler and platform
- dataset size
- key distribution
- value distribution
- write ratio vs read ratio
- snapshot frequency
- number of retained snapshots
- whether reads are sealed
- whether caches are enabled
- mutable load factor
- memory usage at start and end
- elapsed time
- ops per second
- p50, p95, p99 latency if available

### Required Workload Categories

1. Pure mutable ingest
2. Read-heavy steady state
3. Mixed read/write
4. Snapshot-heavy workload
5. Rollback workload
6. Version retention workload
7. Repeated-value workload
8. Mostly-unique-value workload
9. Large-key workload
10. Concurrent readers with single writer

### Benchmark File Convention

Store results in a machine-readable format such as JSON or CSV plus a short markdown summary.

Suggested fields:

```json
{
  "name": "snapshot_heavy_1m_keys",
  "date": "2026-03-06",
  "python": "3.14.3",
  "platform": "windows-amd64",
  "dataset_size": 1000000,
  "delta_size": 10000,
  "snapshots_retained": 100,
  "reads_sealed": true,
  "cache_enabled": true,
  "ops_per_sec": 0,
  "p50_us": 0,
  "p95_us": 0,
  "rss_mb": 0,
  "estimated_native_bytes": 0,
  "notes": ""
}
```

### Success Metrics

Track these over time:

- ops per second by workload class
- memory per logical item
- memory per retained snapshot
- incremental memory per changed key
- snapshot creation latency
- rollback latency
- diff latency
- read amplification across generations
- cache hit rate
- dedup ratio for values
- dedup ratio for internal nodes

---

## Caching Plan

Caching should be treated as a controlled optimization layer, not as the core algorithm.

Recommended caches:

- last-key hash cache for repeated Python string objects
- hot-key lookup cache
- hot-prefix path cache for repeated traversal prefixes
- snapshot-local read cache for repeated queries against an immutable version
- decoded child-index cache for bitmap navigation if profiling proves value

Rules:

- caches must be optional and measurable
- caches must not make correctness depend on object identity without owning references safely
- caches must report hit rates in metrics
- caches should be bounded to avoid hiding memory regressions

---

## Roadmap

### Phase 1. Stabilize The Current Baseline

Goal:

- stop adding speculative features
- make the current behavior measurable and correct

Status:

- closed as the baseline stabilization milestone

Tasks:

- eliminate hidden global-state coupling where possible
- document current complexity honestly
- add benchmark harnesses and result recording
- add memory accounting for values, nodes, and child arrays
- add regression tests for insert, search, delete, freeze, and dedup correctness

Exit criteria:

- reproducible benchmark suite
- stable correctness baseline
- benchmark results recorded in a standard format

Closure note:

- the repository now has a reproducible benchmark harness with machine-readable artifacts and Markdown summaries
- baseline correctness coverage exists for insert, search, delete, freeze materialization, and core dedup/value-pool behavior
- runtime/native memory accounting is surfaced in stats and benchmark artifacts
- unsupported roadmap workload categories are recorded explicitly instead of being filled with speculative numbers
- remaining unification and native-engine redesign work is intentionally carried by later phases instead of leaving Phase 1 open-ended

### Phase 2. Unify The Product Model

Goal:

- replace `LQFT` and `MutableLQFT` as separate concepts with a unified logical map

Status:

- closed at the public API and product-model layer

Tasks:

- define one public API surface
- keep mutable head internal
- turn `freeze()` into `snapshot()` semantics
- introduce explicit snapshot handles
- move merge logic from wrapper glue into the native engine

Exit criteria:

- one primary user-facing structure
- snapshots available as first-class objects

Closure note:

- `LQFTMap` is now the primary user-facing structure
- snapshots are first-class handles through `LQFTSnapshot`
- wrapper-level split is no longer the intended product model, even though `LQFT` and `MutableLQFT` remain as compatibility surfaces
- native snapshot registration, ownership, rollback restore, diff, retention-aware compaction, and benchmark telemetry now exist behind the unified model
- remaining instance-local native-state work was carried by Phase 3
- remaining deeper commit-pipeline and tombstone work belongs to Phase 4

### Phase 3. Instance-Local Native Engine

Goal:

- remove global singleton assumptions and support multiple independent maps cleanly

Status:

- closed at the native-state isolation layer

Tasks:

- create per-instance root, mutable table, value pool, cache state, and metrics
- eliminate shared global roots where not intentionally required
- ensure destruction and cleanup are instance-safe

Exit criteria:

- multiple independent map instances can coexist safely
- benchmarks can isolate one map from another

Closure note:

- `PersistentSharedState` now owns canonical registry state, value interning, node chunks, and recycled-node pools per logical lineage instead of relying on process-global persistent ownership
- persistent wrapper stats now report `engine_scope=instance-local`, reflecting that separate `LQFT` and `LQFTMap` lineages no longer depend on shared global roots or allocator state
- the Python wrapper now requires the stateful native backend, and the legacy module-global persistent compatibility API has been removed from `lqft_engine.c` entirely
- multiple independent instances can now coexist without hidden singleton coupling, while snapshots within one lineage still intentionally share native state through their owning lineage
- rebuild and regression validation completed cleanly after the final cleanup pass, with the full suite passing at 18 tests
- remaining mutable-head commit semantics, tombstones, and delete-through-snapshot correctness belong to Phase 4 rather than remaining as Phase 3 blockers

### Phase 4. Commit Pipeline And Tombstones

Goal:

- make the mutable head and persistent body cooperate correctly

Status:

- closed at the mutable-head commit and delete-correctness layer

Tasks:

- add tombstone semantics in mutable head
- implement commit from delta into canonical root
- ensure reads check mutable head first, then committed root
- support delete correctness across snapshots

Exit criteria:

- fast writes with correct committed snapshots
- deletes remain correct before and after commit

Current benchmark note:

- 2026-03-10 Phase 4 benchmark artifacts now include a supported `delete_heavy_snapshot_workload` to measure delete-through-commit behavior explicitly.
- At dataset_size=500, the delete-heavy row recorded 528.701 ops/s versus 479.322 ops/s for `snapshot_heavy_workload`, with lower snapshot latency (`p50` 1671.9 us versus 2039.35 us).
- At dataset_size=5000, the delete-heavy row recorded 136.936 ops/s versus 103.698 ops/s for `snapshot_heavy_workload`, widening the relative advantage while also keeping lower snapshot latency (`p50` 6158.2 us versus 9189.6 us).
- In both artifacts the delete-heavy row ended with a much smaller committed native footprint than the snapshot-heavy row, but a higher end RSS, so the current evidence supports better delete-heavy commit throughput rather than a broad memory-win claim.
- `rollback_workload` remains much cheaper than commit in absolute terms, but its current benchmark row is still high-noise and should be treated as directional rather than as a stable headline comparison against snapshot creation.

Closure note:

- `LQFTMap` now operates as a committed-root-plus-mutable-delta structure instead of a mutable mirror, so reads, deletes, rollback, and snapshot creation all respect committed state plus pending head mutations.
- Tombstones now live in the mutable head itself, including the native mutable table, which keeps delete intent correct before commit and after snapshot materialization.
- Snapshot commit now prefers a one-shot native delta-apply path that clones the committed persistent root and applies the mutable table in one C call, with narrower fallback paths retained only for compatibility.
- Unified-map regression coverage now explicitly exercises overlay reads, clear semantics, delete-through-snapshot correctness, native clone behavior, native tombstone behavior, and one-shot native delta application.
- The benchmark harness now measures delete-heavy snapshot creation directly, and current artifacts support the claim that Phase 4 improved delete-heavy commit throughput without yet proving a general memory-density win.
- Remaining structural sharing and dedup-efficiency claims are intentionally carried by Phase 5 rather than leaving Phase 4 open-ended.

### Phase 5. Structural Dedup Becomes The Core Feature

Goal:

- make canonical subtree reuse visible, measurable, and defensible

Status:

- closed at the subtree-sharing measurement layer

Implementation focus:

- separate value pooling wins from true subtree and node-reuse wins so metrics do not overstate deduplication
- define stable canonical node signatures before treating interning hit rates as evidence of structural sharing
- report per-snapshot incremental native growth alongside dedup ratios so reuse claims stay tied to memory outcomes
- require adversarial correctness coverage before using dedup numbers as headline performance claims

Measurement strategy:

- primary metric path: explicit native counters for internal-node and subtree reuse, not benchmark-only inference
- secondary validation path: retained-snapshot lineage sampling that measures shared versus unique reachable nodes across snapshot pairs or windows
- benchmark-derived proxies such as bytes per retained snapshot, node density, and registry hit rate should remain supporting evidence rather than the headline subtree-sharing claim

Tasks:

- strengthen node interning signatures
- split canonical reuse accounting into leaf/value reuse versus internal-node and subtree reuse
- add retained-snapshot lineage sampling or equivalent native traversal to measure shared versus unique reachable nodes across snapshots
- expose explicit subtree-reuse metrics in `stats()` and benchmark artifacts alongside the current value/node-reuse telemetry
- benchmark retained-snapshot sharing with subtree-specific metrics, using current retention-window proxies only as supporting context

Exit criteria:

- subtree dedup ratio reported in metrics
- adversarial correctness coverage protects against incorrect canonical reuse across overwrites, sibling paths, and snapshot evolution
- benchmark artifacts report subtree-specific sharing indicators for retention workloads, not just committed-root and retention-window proxies
- snapshot memory growth is shown clearly below full-copy baselines on versioned workloads using explicit subtree-sharing metrics plus memory outcomes

Closure note:

- Phase 5 now has a complete measurement and correctness surface instead of only roadmap intent: persistent-root and unified-map stats expose dedup telemetry, benchmark artifacts carry the same fields, and retention-window summaries compare structural-sharing indicators directly.
- Unified-map stats now also include explicit retained-snapshot subtree-sharing metrics, including pairwise unchanged-item reuse, estimated shared subtree nodes, and subtree/internal subtree reuse ratios.
- Retention-window benchmark summaries and comparison payloads now carry those subtree-sharing ratios, so cross-run changes can be tracked as first-class structural signals rather than only value/node reuse proxies.
- Regression coverage includes adversarial canonical-reuse cases, including same-key overwrite replacement and snapshot evolution where one sibling changes while another must remain stable.

### Phase 6. Snapshot Tooling

Goal:

- make the versioned story practically useful

Status:

- closed at the unified Python API layer

Tasks:

- add `rollback(snapshot)`
- add `diff(snapshot_a, snapshot_b)`
- add export and restore of snapshot metadata
- add snapshot retention policies

Exit criteria:

- users can create, keep, compare, and restore snapshots

Closure note:

- `LQFTMap` now exposes the full Phase 6 surface at the wrapper level: `snapshot`, `rollback`, `diff`, `compact`, `export_snapshot`, and `load_snapshot`.
- Snapshot export now produces a structured payload containing explicit snapshot metadata and serialized key/value contents instead of relying on implicit in-process handles only.
- Snapshot restore now rebuilds a persistent root from that payload, registers it as a first-class `LQFTSnapshot`, and can optionally activate it as the map's current committed snapshot.
- Snapshot retention policies remain integrated with the same registry-backed handle model through `compact(retain_last=N)`, so kept, restored, and current snapshots share one consistent control surface.

### Phase 7. Read Optimization And Controlled Caching

Goal:

- improve practical latency without breaking correctness claims

Status:

- closed at the unified Python API layer

Tasks:

- add bounded caches with metrics
- add cache hit counters and invalidation rules
- benchmark cache on realistic repeated-read workloads
- disable caches by default unless they prove broadly useful

Exit criteria:

- measurable latency improvement on hot workloads
- no correctness regressions from cache state

Closure note:

- `LQFTMap` now exposes an explicit bounded read cache through `enable_read_cache(max_entries)`, `disable_read_cache()`, and `clear_read_cache()`.
- The cache remains disabled by default and only serves committed-root lookups when there is no pending mutable delta, which keeps cache scope and invalidation rules narrow enough to defend.
- Cache telemetry is now part of `stats()`, including hits, misses, lookups, hit rate, evictions, live size, and configured capacity.
- State transitions that can invalidate committed-root answers, including writes, clear, snapshot creation, rollback, compaction, and activating a loaded snapshot, now clear the cache before further reads.
- Benchmark artifacts now include explicit cached-versus-uncached hot-read workloads so Phase 7 claims are tied to recorded hit rates and throughput deltas rather than anecdotal microbenchmarks.

### Phase 8. Concurrency And Background Work

Goal:

- make the structure useful under industrial read-heavy workloads

Status:

- closed at the unified Python API layer

Tasks:

- keep readers on immutable roots whenever possible
- move commit and compaction into controlled background work where safe
- define safe reclamation strategy for shared nodes
- measure reader throughput while writes continue

Exit criteria:

- concurrent readers can operate on stable snapshots without blocking writers heavily

Closure note:

- `LQFTMap.read_snapshot()` now pins immutable snapshot handles for reader sessions, so readers can stay on stable committed roots while the live map continues to accept writes and advance snapshots.
- `LQFTMap.snapshot_async()` now captures a stable submission-time commit plan while briefly holding the Python map lock, clones the native mutable delta during that short capture window, performs native root cloning and delta materialization off-lock, and only promotes the result to the live committed root if no newer writes advanced the map first.
- `LQFTMap.compact_async()` still provides controlled background work through the same serialized worker instead of relying on implicit or unsafe concurrent mutation.
- Wrapper-level reclamation rules now preserve any snapshots currently pinned by active reader sessions during compaction, which gives shared snapshot roots a concrete safety rule at the Python layer.
- `stats()` and benchmark artifacts now expose reader and background-task telemetry, and the baseline harness now records both synchronous and async concurrent reader/writer workloads so the off-lock `snapshot_async()` path is measured directly instead of inferred from API tests alone.
- The native mutable-clone refinement materially improved the measured async concurrency row: the comparable dataset_size=200 single-run artifact `phase1-baseline-20260311-181025` raised `concurrent_readers_async_snapshot_writer` from 46,871.845 ops/s to 76,857.785 ops/s against `phase1-baseline-20260311-175646`, while still reporting `background_tasks_submitted=16`, `background_tasks_completed=16`, and `background_tasks_failed=0`.

### Phase 9. Storage And Scale Extension

Goal:

- prepare for very large datasets beyond single-process RAM comfort

Status:

- open; the persisted-artifact and trust-policy slice is implemented, but the broader storage-engine work is not

Tasks:

- define on-disk snapshot format
- explore mmap or paged node storage
- add write-ahead logging if durability becomes a goal
- design sharding boundaries for distributed growth

Exit criteria:

- clear path from in-memory engine to storage engine

Implemented in the current Phase 9 slice:

- `LQFTMap.save_snapshot()` and `load_snapshot_file()` now provide a concrete on-disk storage slice using the existing `lqft-snapshot-v1` payload format instead of limiting snapshot transport to in-memory dictionaries.
- `LQFTSnapshot.save()` exposes the same file format directly from retained snapshot handles, which makes persisted snapshot export part of the current public API rather than a wrapper-only internal exercise.
- `LQFTMap.export_snapshot_bundle()` / `load_snapshot_bundle()` and their file-backed counterparts now provide a manifest layer for multiple retained snapshots, so the storage story is no longer limited to one snapshot per file.
- `LQFTMap.enable_write_ahead_log(...)` now adds an initial durability journal: `put`, `delete`, and `clear` append JSONL records before mutating the live map, while committed-state transitions such as `snapshot()`, `load_snapshot()`, `load_snapshot_bundle()`, `rollback()`, and `compact()` append replayable state-checkpoint records.
- `LQFTMap.recover_from_write_ahead_log(...)` can now rebuild both the live map view and retained snapshot registry from that journal, and it can optionally truncate an incomplete tail record left by an interrupted append.
- `LQFTStorageEngine(...)` now defines the first explicit file-backed storage boundary in the codebase: it can attach WAL journaling to a live map, checkpoint the retained snapshot bundle to a persisted file, optionally prune retained snapshots during checkpoint rotation through `checkpoint_retain_last` or `checkpoint(..., retain_last=...)`, rotate the WAL after checkpoint, and recover a map by loading the checkpoint first and then replaying the remaining journal.
- That storage boundary can now be serialized and signed as well: `LQFTStorageEngine.save_manifest(...)` / `load_manifest(...)` persist a `lqft-storage-engine-manifest-v1` document that carries checkpoint path, WAL path, retention settings, and path-based checkpoint signing plus verification policy together inside the existing persisted-file envelope, so a loaded manifest can recover state and emit future signed checkpoints without the caller re-supplying signing material manually.
- Storage-engine manifests now also have a first-class inspection surface: `inspect_storage_engine_manifest(...)` reports envelope signature/validity metadata, the manifest's checkpoint and WAL settings, and structured `policy_check` / `effective_policy` fields so automation can audit manifest verification and freshness state without performing recovery.
- That inspection surface now also has CLI parity: `python -m lqft_engine inspect-storage-engine-manifest ...` emits the same JSON summary and supports `--fail-on-policy-fail` so shell automation can branch on the same category-specific policy exit codes used by the other inspection commands.
- Storage-engine manifest authoring now has CLI parity too: `python -m lqft_engine save-storage-engine-manifest ...` writes a `lqft-storage-engine-manifest-v1` file from explicit checkpoint/WAL/retention and path-based checkpoint signing or verification settings, so storage-engine configuration can be created from shell automation without dropping into Python first.
- Recovery now has CLI parity too: `python -m lqft_engine recover-storage-engine-manifest ...` loads and verifies a storage-engine manifest, applies checkpoint-plus-WAL recovery, and emits a JSON summary of the recovered map state so shell automation can drive recovery without writing Python glue.
- Recovery CLI output now also supports full logical-state export: `python -m lqft_engine recover-storage-engine-manifest ... --include-current-state-payload` adds a non-mutating `lqft-current-state-v1` payload so automation can consume recovered key/value state directly instead of only summary counters.
- Checkpoint rotation now has CLI parity too: `python -m lqft_engine checkpoint-storage-engine-manifest ...` loads and verifies a storage-engine manifest, performs recovery, runs checkpoint rotation with the manifest's retention policy or an override, truncates the WAL, and emits a JSON summary of the post-checkpoint state so shell automation can drive the full recovery-plus-rotation loop.
- The current file format is written with an atomic replace step, wrapped in a checksummed file envelope, can be gzip-compressed by path suffix, and now supports optional HMAC signing plus Ed25519 signatures with a stable signer key id for trust verification without changing the underlying snapshot payload schema.
- The Python surface now also includes `generate_ed25519_keypair()` and `write_ed25519_keypair(...)`, plus a module CLI at `python -m lqft_engine generate-ed25519-keypair ...`, so producer-identity signing can be provisioned inside the project instead of relying on an external key tool.
- Verifier-side producer allowlists now exist too: `build_signer_trust_store(...)` and the `trusted_signers=` file-load parameter let callers verify Ed25519-signed files by trusted `key_id` instead of threading a single public key through each load call.
- Those producer allowlists can now be loaded from disk too: `load_signer_trust_store(...)` accepts either a directory of `.pem` public keys or a JSON manifest that maps signer ids to PEM paths relative to the manifest.
- Manifest-backed trust stores now also support signer metadata plus revocation flags and reasons, so a producer can be annotated or removed from the active allowlist without changing any signed snapshot or bundle payload.
- Trust-store manifests can now also be signed and verified through the same authenticity envelope used elsewhere: `save_signer_trust_store_manifest(...)` writes a checksummed manifest document with optional HMAC or Ed25519 signing, and `load_signer_trust_store(..., verify_key=..., trusted_manifest_signers=...)` can require that manifest signature before loading any producers from it.
- Callers can now inspect trust policy explicitly too: `inspect_signer_trust_store(...)` reports active signer count, revoked signer count, signer metadata, and signed-manifest metadata without requiring manual traversal of the loaded allowlist structure.
- The trust model now has a tested split between producer trust and policy trust: `trusted_signers=` governs who may sign snapshots and bundles, while `trusted_manifest_signers=` can independently govern who is allowed to sign the manifest that defines those trusted producers.
- Trust-store manifests now also support optional validity windows via `validity.not_before` and `validity.expires_at`; `load_signer_trust_store(...)` rejects manifests that are not currently valid, while `inspect_signer_trust_store(...)` reports the validity status so stale policy can be surfaced before use.
- Validity windows now also support an optional warning threshold through `validity.warn_before_expiry_seconds`, allowing `inspect_signer_trust_store(...)` to surface `expiring-soon` policy before expiry while `load_signer_trust_store(...)` continues to accept the manifest until the actual expiry boundary.
- Load-time policy can now be stricter than basic validity too: `load_signer_trust_store(..., min_remaining_validity_seconds=N)` rejects manifests that either omit `expires_at` or do not have at least `N` seconds of remaining validity, which gives production callers a hard freshness floor instead of relying only on inspection-side warnings.
- Trust-store inspection now mirrors that same freshness policy: `inspect_signer_trust_store(..., min_remaining_validity_seconds=N)` reports a `policy_check` block showing whether the manifest would currently pass the loader's freshness requirement, without mutating or hydrating the active allowlist.
- The same envelope-level validity model now applies to persisted snapshot and bundle files: `save_snapshot(...)`, `LQFTSnapshot.save(...)`, and `save_snapshot_bundle(...)` accept an optional `validity` block, while `load_snapshot_file(..., min_remaining_validity_seconds=N)` and `load_snapshot_bundle_file(..., min_remaining_validity_seconds=N)` can reject artifacts that are stale, not yet valid, or too close to expiry for production use.
- Persisted artifact inspection now exists too: `inspect_persisted_file(...)` reports the envelope signature summary, checksum status, validity window state, payload type/metadata, and whether the current verification/freshness policy would allow loading, so operators can audit snapshot and bundle files without mutating any map state.
- Inspection results now also expose a combined `effective_policy` summary, so persisted-file and trust-store inspection surfaces report one top-level pass/fail view across verification mode, signer-trust requirements, and freshness constraints instead of leaving callers to reconstruct that verdict from lower-level fields.
- Those inspection results now also carry machine-readable policy codes alongside the human-readable error strings: `policy_check.error_code` and `effective_policy.reason_code` let automation branch on stable failure categories such as freshness-floor violations and invalid freshness-floor configuration, checksum mismatches, malformed persisted envelopes and trust-store metadata, missing or malformed trusted-signer inputs, empty trust-store sources, trusted-signer configuration mistakes, or unsupported verification modes without parsing message text. They now also expose `policy_check.error_code_is_fallback` and `effective_policy.reason_code_is_fallback`, and the module-level `classify_policy_error(...)` helper returns the same `is_fallback` signal so future generic matches are obvious.
- The same machine-readable policy categories now flow through the direct load APIs too: `load_snapshot_file(...)`, `load_snapshot_bundle_file(...)`, and `load_signer_trust_store(...)` raise `LQFTPolicyError` with stable `.code` and `.component` fields while preserving the existing human-readable `ValueError` messages.
- Those inspection surfaces are now available from the CLI too: `python -m lqft_engine inspect-persisted-file ...` and `python -m lqft_engine inspect-signer-trust-store ...` emit the same JSON inspection summaries for shell automation and operator workflows.
- The inspection CLI now also supports opt-in automation-friendly exit codes: `--fail-on-policy-fail` still prints the full JSON inspection report, but now returns category-specific statuses when `effective_policy.allowed` is false: `10` for freshness failures, `11` for signer-trust failures, `12` for signature verification failures, `13` for unsupported verification/configuration, `15` for persisted-file integrity failures, `16` for malformed persisted-file or trust-store schema failures, `17` for source failures such as an empty trust-store directory, and `14` for any remaining uncategorized policy failures.
- `LQFTShardedMap(shard_count=N)` now defines an explicit in-process sharding boundary at the API layer: keys are routed deterministically to shard-local `LQFTMap` instances, while stats, snapshots, and current-state export can be aggregated across shards.
- `LQFTShardedStorageEngine(shard_count=N, checkpoint_dir=..., wal_dir=...)` now orchestrates per-shard checkpoint/WAL lifecycle as one storage unit, with attach, checkpoint rotation, and recovery fanout across shard-local storage engines.
- `LQFTShardedStorageEngine.save_manifest(...)` / `load_manifest(...)` now persist a single signed `lqft-sharded-storage-engine-manifest-v1` bundle containing shard count and per-shard storage-engine configuration for repeatable recovery.
- Sharded storage orchestration now also has CLI parity through `save-sharded-storage-engine-manifest`, `recover-sharded-storage-engine-manifest`, `checkpoint-sharded-storage-engine-manifest`, and `inspect-sharded-storage-engine-manifest`.
- Phase 9 remains open because the roadmap exit criteria still call for a clear path from the in-memory engine to a scalable storage engine, and this codebase still does not implement paged node storage, mmap-backed node access, or distributed sharding beyond the current in-process shard boundary.

---

## Data Structure Sketch

### Logical Object

```text
LQFTMap
  mutable_delta
  committed_root
  snapshot_table
  node_intern_table
  value_intern_table
  cache_state
  metrics
```

### Mutable Delta Entry

```text
DeltaEntry
  key_ref
  value_ref or tombstone
  key_hash
  generation
```

### Canonical Node

```text
CanonicalNode
  kind: internal | leaf
  bitmap
  children[]
  key_hash
  key_fingerprint
  key_ref
  value_ref
  subtree_hash
  refcount or epoch metadata
```

### Snapshot Handle

```text
Snapshot
  root
  generation
  parent_generation
  mutation_count
  timestamp
```

---

## Recommended Positioning

Do not market this as:

- a structure with total memory `O(log n)`
- a structure that beats every hash table on raw insert and search
- a universal replacement for `dict`

Market it as:

- a versioned mutable-to-persistent map
- optimized for snapshot retention and structural sharing
- strong for workloads with many nearby versions
- strong for repeated values and repeated subtrees
- suitable for read-heavy, version-aware systems

---

## Near-Term Deliverables

1. Replace the wrapper-level split with a unified design document and API contract.
2. Add benchmark and metrics recording infrastructure before major architectural rewrites.
3. Move toward per-instance native state.
4. Introduce tombstones, snapshots, and commit semantics.
5. Expand Phase 9 from the current persisted-artifact slice toward paged/mmap-backed storage and sharding boundaries.
6. Add deeper storage-engine benchmarks that pair retention and WAL recovery with larger retained snapshot windows.

---

## Definition Of Success

The redesign succeeds if the project can credibly claim all of the following:

- mutable writes remain practically fast
- committed snapshots are immutable and cheap to retain
- unchanged structure is heavily shared across nearby versions
- deduplication is measurable, not just described
- complexity claims are honest
- benchmarks show wins on versioned and snapshot-heavy workloads, not only microbenchmarks against `dict`
