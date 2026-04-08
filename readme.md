# Log-Quantum Fractal Tree (LQFT)

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](#)
[![C-Engine](https://img.shields.io/badge/Native-C-red.svg)](#)
[![Concurrency](https://img.shields.io/badge/Concurrency-Benchmark_Dependent-yellow.svg)](#)
[![Architecture](https://img.shields.io/badge/Architecture-Merkle_HAMT-pink.svg)](#)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE.md)

## Project Overview

The **Log-Quantum Fractal Tree (LQFT)** is a native Python extension that combines HAMT-style routing with structural sharing. The project is still interesting as a systems exercise and as a specialized persistent structure, but the benchmark results in this repository do not support a general claim that LQFT is faster than mainstream in-memory structures in practice.


---

## Release Note (v1.2.0)

This release keeps the paired key/value batching patch and native mutable frontend improvements, closes the Phase 2 product-model unification pass with the wrapper-level `LQFTMap` as the intended primary entry point, and materially advances the Phase 4 commit pipeline with native tombstones and a one-shot delta-commit path.

What improved:

- `MutableLQFT` moved materially closer to Python `dict` in write-heavy, mixed, and churn-heavy workloads.
- The persistent `LQFT` wrapper kept improving in native-backed write and read paths.
- `LQFTMap` now provides one unified wrapper API with explicit snapshot handles, while `LQFT` and `MutableLQFT` remain available as compatibility and lower-level control surfaces.
- The persistent native engine is now instance-local across roots, canonical registry state, value interning, and allocator ownership for each logical lineage.
- `LQFTMap.snapshot()` now prefers a native one-shot delta-commit path that clones the committed root and applies the mutable head in one C call when the native backend is available.
- Delete intent now survives commit through native mutable tombstones, so pre-snapshot and post-snapshot delete behavior is aligned instead of relying on wrapper-only bookkeeping.
- `LQFTMap` now supports snapshot export and restore through `export_snapshot()` and `load_snapshot()`, and can also package multiple retained snapshots through `export_snapshot_bundle()` and `load_snapshot_bundle()`.
- `LQFTMap` now also supports file-backed snapshot persistence through `save_snapshot()` and `load_snapshot_file()`, and `LQFTSnapshot.save()` can write a snapshot handle directly to disk using a checksummed file envelope around the same snapshot JSON payload, with optional gzip compression when the path ends in `.gz` and optional HMAC signing or Ed25519 signing when a secret key or PEM keypair is supplied.
- `LQFTMap` now supports an opt-in bounded read cache with hit/miss/eviction telemetry, giving a controlled hot-read optimization path without changing default correctness or memory behavior.
- `LQFTMap` now also supports pinned read sessions plus background snapshot and compaction tasks, so stable snapshot readers can continue while the writer advances the live map.

What did not improve:

- LQFT is still not generally competitive with Python dict or straightforward hash-table implementations.
- Read-heavy workloads are still weaker than mainstream alternatives.
- Some results remain benchmark-dependent, especially persistent unique-insert throughput.

Practical claim for this release:

- v1.2.0 now exposes one intended high-level model, `LQFTMap`, while retaining `LQFT` and `MutableLQFT` as compatibility surfaces.
- v1.2.0 is a substantially better mutable/write-heavy LQFT than earlier releases.
- v1.2.0 also has a materially stronger mutable-to-persistent commit path for snapshot-oriented workloads, especially when deletes are part of the delta.
- v1.2.0 is not a proof that LQFT beats common in-memory data structures overall.

---

## Performance Snapshot (v1.2.0)

Verified environment: Windows workstation, Python 3.14 local build, native extension compiled in-place, benchmark matrix used during development before packaging cleanup.

| Metric | Current Observation | Architectural Driver |
| :--- | :--- | :--- |
| **Pure Write Throughput** | **Improved strongly vs. v1.0.9/local baseline** | Native paired key/value batching for unique-value writes |
| **Pure Read Throughput** | **Still workload-dependent and behind dict/hash-table peers** | Traversal cost + concurrency overhead |
| **Mixed Throughput** | **Improved modestly at best** | Write batching helps, but read-side costs still dominate |
| **Memory Density** | **Tracked at runtime via `estimated_native_bytes / physical_nodes`** | Real node bytes + active child arrays + pooled values |
| **Practical Competitiveness** | **Not generally competitive yet** | Constant-factor overhead still too high |

Benchmark note: throughput is workload- and environment-dependent. The release claim for this package should stay conservative and centered on write-heavy improvement rather than broad superiority.

## Phase 4 Commit Status

Current implementation status:

- `LQFTMap` now behaves as a committed-root-plus-delta structure instead of a mutable mirror, so reads check the mutable head first and then fall back to the committed persistent root.
- The native mutable head tracks logical tombstones, and snapshot commit preserves those deletes through the native pipeline instead of reconstructing delete intent in Python.
- Native snapshot materialization now prefers one-shot delta application, with clone-plus-materialize and Python replay retained only as fallback paths.
- Current benchmark artifacts show the strongest relative gain in delete-heavy snapshot creation, but the repository still treats memory claims conservatively because end RSS did not improve in lockstep with committed native bytes.

## Phase 5 Dedup Status

Current implementation status:

- `stats()` now exposes Phase 5 structural-dedup telemetry for persistent roots and unified-map committed roots, including value dedup savings, canonical registry reuse counters, value-pool reuse counters, and node-shape counts.
- `LQFTMap.stats()` now also reports explicit retained-snapshot subtree sharing metrics, including pairwise unchanged-item reuse, estimated shared subtree nodes, and subtree/internal subtree reuse ratios across retained snapshot pairs.
- Benchmark artifacts now carry the same Phase 5 fields in JSON rows and summarize retention-window structural-sharing indicators in Markdown, including bytes per retained snapshot, value dedup ratio, node density, canonical registry hit rate, value-pool hit rate, and subtree reuse ratios.
- Adversarial regression coverage now protects against incorrect canonical reuse in two important cases: same-key overwrite with same-length replacement values, and snapshot evolution where one sibling changes while another must remain stable.
- Phase 5 is now treated as closed at the measurement layer: subtree-sharing indicators are first-class metrics in both runtime stats and retention-window benchmark summaries.

## Phase 6 Snapshot Tooling Status

Phase 6 is now closed at the Python API layer.

Current implementation status:

- `LQFTMap` exposes `snapshot`, `rollback`, `diff`, `compact`, `export_snapshot`, and `load_snapshot` on the unified wrapper surface.
- `export_snapshot()` emits a structured payload with explicit snapshot metadata plus serialized key/value contents.
- `load_snapshot()` restores that payload into a fresh persistent root, registers it as a first-class `LQFTSnapshot`, and can optionally activate it as the current committed snapshot.
- `export_snapshot_bundle()` and `load_snapshot_bundle()` extend the same model to an ordered set of snapshots plus bundle metadata such as the recorded current snapshot id.
- `save_snapshot()` and `load_snapshot_file()` add a file-backed version of that same flow using atomic writes, and `LQFTSnapshot.save()` exposes the same on-disk format from an individual snapshot handle.
- `save_snapshot_bundle()` and `load_snapshot_bundle_file()` add the same file-backed path for multi-snapshot manifests.
- The file-backed APIs now also accept optional `sign_key`, `verify_key`, and `trusted_signers` parameters so persisted files can carry authenticity metadata instead of relying only on corruption checks, including asymmetric Ed25519 verification with a recorded signer key id.
- Snapshot retention policies remain available through `compact(retain_last=N)`, so exported, restored, active, and retained snapshots all use the same registry-backed handle model.

## Phase 9 Storage Slice Status

Phase 9 is still open overall. What is implemented today is the persisted-artifact and trust-policy slice of that phase, not the full storage-engine milestone.

Current implementation status:

- `LQFTMap.save_snapshot(path, snapshot=None)` writes the existing `lqft-snapshot-v1` payload to disk with an atomic replace step, so snapshot export is no longer limited to in-memory dictionaries.
- `LQFTMap.load_snapshot_file(path, activate=False)` restores that same on-disk snapshot file directly into a map instance.
- `LQFTSnapshot.save(path)` lets a retained snapshot handle persist itself without routing back through the map.
- `LQFTMap.save_snapshot_bundle(path, snapshots=None)` writes a multi-snapshot manifest that can restore a retained snapshot set and optionally reactivate the recorded current snapshot.
- Saved files now carry a checksum over the embedded snapshot payload, so `load_snapshot_file()` can reject corruption instead of relying on downstream JSON or schema errors.
- Saved files can also carry an optional HMAC-SHA256 signature over the embedded payload, or an Ed25519 signature with an embedded `key_id` derived from the producer's public key fingerprint; `verify_key` can require a specific signer directly, and `trusted_signers` can verify against an allowlist keyed by signer id.
- `LQFTMap.enable_write_ahead_log(path, ...)` now adds an opt-in JSONL durability log for live mutations. `put`, `delete`, and `clear` are appended ahead of mutation, while snapshot-registry transitions such as `snapshot()`, `load_snapshot()`, `load_snapshot_bundle()`, `rollback()`, and `compact()` emit replayable state-checkpoint records.
- `LQFTMap.recover_from_write_ahead_log(path, ...)` can rebuild map state from that journal, including retained snapshots and the current committed snapshot, and can optionally truncate an incomplete tail record left by an interrupted append.
- `LQFTStorageEngine(checkpoint_path=..., wal_path=...)` now defines the first explicit file-backed storage boundary in the project. It can attach WAL journaling to a map, checkpoint the retained snapshot bundle, optionally prune retained snapshots during checkpoint rotation with `checkpoint_retain_last` or `checkpoint(..., retain_last=...)`, rotate the WAL after checkpoint, and recover a map by loading the checkpoint first and then replaying the remaining journal.
- That storage boundary can now be transported too: `LQFTStorageEngine.save_manifest(...)` and `load_manifest(...)` persist a signed `lqft-storage-engine-manifest-v1` document that carries checkpoint path, WAL path, retention settings, and path-based checkpoint signing and verification policy in one persisted-file envelope, so a loaded manifest can recover state and write future signed checkpoints without re-supplying key material manually.
- Storage-engine manifests can now be audited directly too: `inspect_storage_engine_manifest(...)` reports the persisted envelope signature and validity summary, the manifest's checkpoint/WAL/retention settings, and a structured `policy_check` / `effective_policy` verdict for manifest verification and freshness requirements.
- That audit surface is now available from the CLI too: `python -m lqft_engine inspect-storage-engine-manifest ...` emits the same JSON inspection report and supports `--fail-on-policy-fail` for category-specific automation exits.
- Storage-engine manifests can now also be authored from the CLI: `python -m lqft_engine save-storage-engine-manifest ...` writes a signed `lqft-storage-engine-manifest-v1` file from explicit checkpoint/WAL/retention and path-based checkpoint signing or verification settings, so shell workflows do not need to instantiate `LQFTStorageEngine` in Python just to persist engine configuration.
- Recovery now has CLI parity too: `python -m lqft_engine recover-storage-engine-manifest ...` loads and verifies a storage-engine manifest, applies checkpoint-plus-WAL recovery, and emits a JSON summary of the recovered map state for shell automation.
- Recovery CLI output can now optionally include the full logical current state payload too: `python -m lqft_engine recover-storage-engine-manifest ... --include-current-state-payload` adds a non-mutating `current_state` export (`lqft-current-state-v1`) so shell pipelines can consume recovered key/value state directly instead of only summary stats.
- Checkpoint rotation now has CLI parity too: `python -m lqft_engine checkpoint-storage-engine-manifest ...` loads and verifies a storage-engine manifest, performs recovery, runs checkpoint rotation with the manifest's retention policy or an override, truncates the WAL, and emits a JSON summary of the post-checkpoint state.
- `LQFTShardedMap(shard_count=N)` now provides an explicit in-process sharding boundary at the API layer, including stable key-to-shard routing, per-shard `LQFTMap` ownership, aggregated stats, and sharded snapshot/state export surfaces.
- `LQFTShardedStorageEngine(shard_count=N, checkpoint_dir=..., wal_dir=...)` now orchestrates per-shard checkpoint/WAL flows as one unit, with attach/checkpoint/recovery fanout across shard-local `LQFTStorageEngine` instances.
- `LQFTShardedStorageEngine.save_manifest(...)` and `load_manifest(...)` now persist a single signed `lqft-sharded-storage-engine-manifest-v1` bundle that captures shard count and per-shard storage-engine configuration.
- Sharded storage orchestration now has CLI parity too: `python -m lqft_engine save-sharded-storage-engine-manifest ...`, `recover-sharded-storage-engine-manifest ...`, `checkpoint-sharded-storage-engine-manifest ...`, and `inspect-sharded-storage-engine-manifest ...`.
- `stats()` now reports whether WAL is enabled plus the active path, fsync mode, and next record sequence.
- Paths ending in `.gz` now use gzip compression automatically, so the initial storage slice can trade CPU for smaller snapshot files without changing the payload schema.
- This Phase 9 slice now covers persisted snapshot files, multi-snapshot manifests, signing and trust-store verification, validity and freshness policy, inspection APIs, CLI inspection/reporting, an initial write-ahead log durability path, a file-backed storage-engine boundary over checkpoints plus WAL replay, an explicit in-process sharding boundary API, and bundled per-shard storage orchestration, but it still does not provide mmap or paged-node storage.

## Phase 7 Read Cache Status

Phase 7 is now closed at the Python API layer.

Current implementation status:

- `LQFTMap` now exposes `enable_read_cache(max_entries)`, `disable_read_cache()`, and `clear_read_cache()`.
- The read cache is bounded and disabled by default. It only serves committed-root lookups when the map has no pending mutable delta, which keeps invalidation rules narrow and explicit.
- `stats()` now reports cache telemetry including enablement, max entries, live size, hits, misses, lookups, hit rate, and evictions.
- Any state change that can invalidate committed-root answers, including writes, clear, snapshot creation, rollback, and activating a loaded snapshot, clears the cache before further reads.
- The benchmark harness now includes explicit uncached and cached hot-read workloads so cache gains and hit rates are recorded in generated artifacts instead of being inferred informally.

## Phase 8 Concurrency And Background Work Status

Phase 8 is now closed at the Python API layer.

Current implementation status:

- `LQFTMap.read_snapshot()` pins an immutable `LQFTSnapshot` for a reader session, so reads can stay on a stable committed root while the live map continues to accept writes and create newer snapshots.
- `LQFTMap.snapshot_async()` now captures a submission-time snapshot plan under the map lock, clones the native mutable delta under that same short lock window, materializes the native root off-lock, and only promotes that snapshot to the live committed root if no newer writes raced ahead while the background task was running.
- `LQFTMap.compact_async()` still routes compaction through the same serialized background worker, keeping the control surface explicit instead of hiding concurrency behind implicit threads.
- `compact(retain_last=N)` now preserves any snapshots that are currently pinned by active reader sessions, which gives the wrapper a concrete reclamation rule for shared nodes and snapshot roots.
- `stats()` now reports concurrency telemetry including active reader counts, pinned snapshot ids, and background task submission, completion, failure, and pending counts.
- The benchmark harness now records both `concurrent_readers_single_writer` and `concurrent_readers_async_snapshot_writer`, and the latest artifact pair shows the async row rising from 46,871.845 ops/s in `phase1-baseline-20260311-175646` to 76,857.785 ops/s in `phase1-baseline-20260311-181025` while still emitting `background_tasks_submitted=16`, `background_tasks_completed=16`, and `background_tasks_failed=0` under concurrent reader pressure.

## Phase 2 API Status

Current implementation status:

- Phase 2 is closed at the public product-model layer: `LQFTMap` is the primary unified Python entry point and exposes `put`, `get`, `delete`, `contains`, `snapshot`, `rollback`, `diff`, `compact`, and `stats`.
- `snapshot()` now returns an explicit `LQFTSnapshot` handle with metadata plus a frozen persistent `root` view.
- `compact()` still clears Python-side snapshot caches by default, and `compact(retain_last=N)` can now prune native-retained snapshots while preserving the current snapshot handle.
- `stats()` now reports retention and pruning telemetry for the unified map, including retained/native snapshot counts plus compact/prune counters for the most recent run and cumulative pruning.
- `stats()` also aliases the current committed snapshot root's native memory counters into the map view, including `estimated_native_bytes`, `physical_nodes`, and `estimated_bytes_per_retained_snapshot`, so Phase 2 workloads can report non-zero committed native footprint.
- `stats()` now also surfaces Phase 5 structural-dedup telemetry from the committed root, including value dedup savings (`value_dedup_*`), canonical registry reuse counters (`canonical_registry_*`), value-pool reuse counters (`value_pool_*`), and node-shape counts (`nodes_with_children`, `hybrid_nodes`, `leaf_only_nodes`).
- Retention-window benchmark summaries now also compare structural-sharing indicators directly, so nearby-version reuse can be inspected from generated artifacts without manually diffing raw JSON rows.
- `freeze()` semantics are now represented by `snapshot()` in the unified model, while the older wrappers remain available for compatibility, benchmarking, and lower-level experiments.
- Remaining native-engine work is intentionally deferred to later roadmap phases: Phase 3 still owns full instance-local native state, and Phase 4 still owns the deeper commit pipeline and tombstone semantics.
- `LQFT` and `MutableLQFT` still exist, but they are no longer the intended top-level product model.

## Phase 1 Baseline Status

Phase 1 is closed as the measured baseline milestone for this repository.

Current implementation status:

- `LQFT` is the native trie-backed engine. Persistent roots, canonical registry state, and value interning are now isolated per logical map lineage, so separate `LQFT` objects no longer share committed keyspace or dedup pools implicitly.
- The remaining allocator-sharing gap is now closed as well: node chunks and recycled-node pools are owned per logical lineage instead of process-global state.
- `MutableLQFT` is the mutable frontend. It uses the native mutable hash table when available and otherwise falls back to a Python `dict`.
- `freeze()` materializes the current mutable contents into the native engine. It is not yet a first-class snapshot, rollback, or diff system.
- `seal_reads()` now applies per persistent instance.
- The Phase 1 exit criteria are treated as satisfied for the baseline layer: benchmark artifacts are reproducible, correctness coverage exists for the baseline surfaces, and native memory accounting is recorded in the current harness.
- Remaining product unification, instance-local native-state work, and deeper commit/snapshot semantics are intentionally tracked in later phases rather than left as open Phase 1 work.

## Current Complexity And Limits

The baseline should be described honestly:

- `MutableLQFT.insert/search/delete` are expected average-case $O(|key| + 1)$ operations with worst-case probe behavior of $O(n)$.
- `LQFT.insert/search/delete` include key hashing cost $O(|key|)$, followed by a bounded-depth native traversal with workload-dependent constant factors.
- The persistent engine exposes useful memory accounting today through `get_stats()`, including `estimated_native_bytes`, `active_child_bytes`, `value_pool_bytes`, `bytes_per_physical_node`, value dedup savings ratios, and canonical/value-pool reuse counters.
- `LQFTMap` now provides real snapshot retention, rollback, diff, and compaction semantics at the unified wrapper level, and the lower-level compatibility surfaces now report fully instance-local native state per logical lineage.

The C extension does contain thread-affinity helpers and a sealed-read mode, but benchmark claims should remain empirical. The repository should not claim universal lock-free behavior, guaranteed NUMA locality, or true $O(1)$ end-to-end operations for arbitrary string keys.

---

## Getting Started

### Installation

For normal users, install the published wheel directly from PyPI:

```bash
pip install lqft-python-engine
```

If a wheel is not available for your platform, `pip` will fall back to a source build. In that case you need a working C compiler toolchain (GCC/MinGW or MSVC).

```bash
# Clone the repository
git clone [https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git](https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git)
cd Log-Quantum-Fractal-Tree-LQFT-

# Build the native C-extension locally
python setup.py build_ext --inplace
```

### Unified Wrapper

The recommended entry point is now `LQFTMap`, which keeps a mutable head internal and produces explicit immutable snapshots.

```python
from lqft_engine import LQFTMap, generate_ed25519_keypair, inspect_persisted_file, inspect_signer_trust_store, load_signer_trust_store, save_signer_trust_store_manifest

store = LQFTMap()
store.put("alpha", "value-a")
store.put("beta", "value-b")

snapshot_a = store.snapshot()

store.put("beta", "value-b2")
store.delete("alpha")
snapshot_b = store.snapshot()

print(store.get("beta"))
print(store.diff(snapshot_a, snapshot_b))

store.enable_read_cache(max_entries=64)
print(store.get("beta"))
print(store.stats()["read_cache_hit_rate"])

with store.read_snapshot(snapshot_b) as reader:
	print(reader.get("beta"))

future = store.snapshot_async()
background_snapshot = future.result()
print(background_snapshot.snapshot_id)

payload = store.export_snapshot(snapshot_b)
restored_store = LQFTMap()
restored_snapshot = restored_store.load_snapshot(payload, activate=True)
print(restored_snapshot.snapshot_id, restored_store.get("beta"))

store.save_snapshot("snapshots/snapshot-b.json", snapshot_b)
disk_store = LQFTMap()
disk_snapshot = disk_store.load_snapshot_file("snapshots/snapshot-b.json", activate=True)
print(disk_snapshot.snapshot_id, disk_store.get("beta"))

store.save_snapshot("snapshots/snapshot-b.json.gz", snapshot_b)
compressed_store = LQFTMap()
compressed_snapshot = compressed_store.load_snapshot_file("snapshots/snapshot-b.json.gz", activate=True)
print(compressed_snapshot.snapshot_id, compressed_store.get("beta"))

store.save_snapshot("snapshots/snapshot-b.signed.json", snapshot_b, sign_key="shared-secret")
trusted_store = LQFTMap()
trusted_snapshot = trusted_store.load_snapshot_file(
	"snapshots/snapshot-b.signed.json",
	activate=True,
	verify_key="shared-secret",
)
print(trusted_snapshot.snapshot_id, trusted_store.get("beta"))

with open("keys/snapshot-signer-private.pem", "rb") as handle:
	ed25519_private_key = handle.read()
with open("keys/snapshot-signer-public.pem", "rb") as handle:
	ed25519_public_key = handle.read()

store.save_snapshot("snapshots/snapshot-b.ed25519.json", snapshot_b, sign_key=ed25519_private_key)
verified_store = LQFTMap()
verified_snapshot = verified_store.load_snapshot_file(
	"snapshots/snapshot-b.ed25519.json",
	activate=True,
	verify_key=ed25519_public_key,
)
print(verified_snapshot.snapshot_id, verified_store.get("beta"))

store.save_snapshot(
	"snapshots/snapshot-b.ed25519.valid-for-24h.json",
	snapshot_b,
	sign_key=ed25519_private_key,
	validity={
		"expires_at": "2026-03-12T18:00:00+00:00",
		"warn_before_expiry_seconds": 3600,
	},
)
fresh_store = LQFTMap()
fresh_snapshot = fresh_store.load_snapshot_file(
	"snapshots/snapshot-b.ed25519.valid-for-24h.json",
	activate=True,
	verify_key=ed25519_public_key,
	min_remaining_validity_seconds=900,
)
print(fresh_snapshot.snapshot_id, fresh_store.get("beta"))

try:
	fresh_store.load_snapshot_file(
		"snapshots/snapshot-b.ed25519.valid-for-24h.json",
		activate=True,
		verify_key=ed25519_public_key,
		min_remaining_validity_seconds=999999,
	)
except LQFTPolicyError as exc:
	print(exc.code, exc.component, exc.is_fallback, str(exc))

print(classify_policy_error("completely unknown failure message"))

snapshot_file_inspection = inspect_persisted_file(
	"snapshots/snapshot-b.ed25519.valid-for-24h.json",
	verify_key=ed25519_public_key,
	min_remaining_validity_seconds=900,
)
print(
	snapshot_file_inspection["payload"]["payload_format"],
	snapshot_file_inspection["signature"]["algorithm"],
	snapshot_file_inspection["validity"]["status"],
	snapshot_file_inspection["effective_policy"]["status"],
	snapshot_file_inspection["effective_policy"]["reason_code"],
	snapshot_file_inspection["policy_check"]["loadable"],
	snapshot_file_inspection["policy_check"]["error_code"],
)

keygen = generate_ed25519_keypair()
print(keygen["key_id"])

trusted_signers = {
	keygen["key_id"]: ed25519_public_key,
}
allowlisted_store = LQFTMap()
allowlisted_snapshot = allowlisted_store.load_snapshot_file(
	"snapshots/snapshot-b.ed25519.json",
	activate=True,
	trusted_signers=trusted_signers,
)
print(allowlisted_snapshot.snapshot_id, allowlisted_store.get("beta"))

file_backed_trusted_signers = load_signer_trust_store("keys/trusted-signers")
manifest_backed_trusted_signers = load_signer_trust_store("keys/trusted-signers.json")
verified_manifest_backed_trusted_signers = load_signer_trust_store(
	"keys/trusted-signers.signed.json.gz",
	verify_key=ed25519_public_key,
)
policy_authority = generate_ed25519_keypair()
policy_authority_signers = {
	policy_authority["key_id"]: policy_authority["public_key_pem"],
}
policy_verified_trusted_signers = load_signer_trust_store(
	"keys/trusted-signers.signed.json.gz",
	trusted_manifest_signers=policy_authority_signers,
)
production_trusted_signers = load_signer_trust_store(
	"keys/trusted-signers.signed.json.gz",
	trusted_manifest_signers=policy_authority_signers,
	min_remaining_validity_seconds=86400,
)
trust_store_inspection = inspect_signer_trust_store(
	"keys/trusted-signers.signed.json.gz",
	verify_key=ed25519_public_key,
	min_remaining_validity_seconds=86400,
)
print(
	trust_store_inspection["active_signer_count"],
	trust_store_inspection["revoked_signer_count"],
	trust_store_inspection["validity"]["status"],
	trust_store_inspection["validity"]["seconds_until_expiry"],
	trust_store_inspection["effective_policy"]["status"],
	trust_store_inspection["effective_policy"]["reason_code"],
	trust_store_inspection["policy_check"]["loadable"],
	trust_store_inspection["policy_check"]["error_code"],
)

# CLI alternative:
# python -m lqft_engine generate-ed25519-keypair --private-out keys/snapshot-signer-private.pem --public-out keys/snapshot-signer-public.pem
# python -m lqft_engine inspect-persisted-file snapshots/snapshot-b.ed25519.valid-for-24h.json --verify-key-file keys/snapshot-signer-public.pem --min-remaining-validity-seconds 900
# python -m lqft_engine inspect-signer-trust-store keys/trusted-signers.signed.json.gz --verify-key-file keys/policy-authority-public.pem --min-remaining-validity-seconds 86400
# python -m lqft_engine inspect-persisted-file snapshots/snapshot-b.ed25519.valid-for-24h.json --verify-key-file keys/snapshot-signer-public.pem --min-remaining-validity-seconds 900 --fail-on-policy-fail
# python -m lqft_engine inspect-signer-trust-store keys/trusted-signers.signed.json.gz --verify-key-file keys/policy-authority-public.pem --min-remaining-validity-seconds 86400 --fail-on-policy-fail
# `--fail-on-policy-fail` returns 10 for freshness failures, 11 for signer-trust failures,
# 12 for signature verification failures, 13 for unsupported verification/configuration,
# 15 for persisted-file integrity failures such as checksum mismatches, 16 for malformed
# persisted-file or trust-store schema failures, 17 for source failures such as an empty
# trust-store directory, and the same policy-code surface also covers trusted-signer
# configuration failures before verification begins plus invalid freshness-floor configuration,
# with 14 reserved for any remaining uncategorized policy failures while still
# printing the full JSON inspection output.
# Trust-store manifest example:
# {
#   "validity": {
#     "not_before": "2026-03-11T00:00:00+00:00",
#     "expires_at": "2027-03-11T00:00:00+00:00",
#     "warn_before_expiry_seconds": 2592000
#   },
#   "signers": [
#     {
#       "key_id": "sha256:...",
#       "path": "trusted-signers/producer-a.pem",
#       "metadata": {"label": "primary", "team": "infra"},
#       "revoked": false
#     }
#   ]
# }
# Signed manifest helper:
# save_signer_trust_store_manifest("keys/trusted-signers.signed.json.gz", manifest_payload, sign_key=policy_authority["private_key_pem"])
# Hard freshness policy:
# load_signer_trust_store("keys/trusted-signers.signed.json.gz", trusted_manifest_signers=policy_authority_signers, min_remaining_validity_seconds=86400)
# Separate authority model:
# - producer key signs snapshots and bundles
# - policy authority key signs trusted-signers manifests

bundle_path = "snapshots/history.json.gz"
store.save_snapshot_bundle(bundle_path, [snapshot_a, snapshot_b])
bundle_store = LQFTMap()
bundle_snapshots = bundle_store.load_snapshot_bundle_file(bundle_path, activate=True)
print([snapshot.snapshot_id for snapshot in bundle_snapshots], bundle_store.latest_snapshot().snapshot_id)

signed_bundle_path = "snapshots/history.valid.json.gz"
store.save_snapshot_bundle(
	signed_bundle_path,
	[snapshot_a, snapshot_b],
	sign_key=ed25519_private_key,
	validity={"expires_at": "2026-03-12T18:00:00+00:00"},
)
verified_bundle_store = LQFTMap()
verified_bundle_snapshots = verified_bundle_store.load_snapshot_bundle_file(
	signed_bundle_path,
	activate=True,
	verify_key=ed25519_public_key,
	min_remaining_validity_seconds=900,
)
print([snapshot.snapshot_id for snapshot in verified_bundle_snapshots], verified_bundle_store.latest_snapshot().snapshot_id)

bundle_file_inspection = inspect_persisted_file(
	signed_bundle_path,
	verify_key=ed25519_public_key,
	min_remaining_validity_seconds=900,
)
print(
	bundle_file_inspection["payload"]["snapshot_count"],
	bundle_file_inspection["validity"]["status"],
	bundle_file_inspection["effective_policy"]["status"],
	bundle_file_inspection["effective_policy"]["reason_code"],
	bundle_file_inspection["policy_check"]["loadable"],
	bundle_file_inspection["policy_check"]["error_code"],
)

store.compact()
store.rollback(snapshot_a)
print(store.get("alpha"))
```

### Python Wrapper Compatibility

The project can still be used through the lower-level wrappers when you want to work directly with the current native persistent or mutable frontends.

```python
from lqft_engine import LQFT

lqft = LQFT()
lqft.insert("alpha", "value-a")
lqft.insert("beta", "value-b")

result = lqft.search("alpha")
present = lqft.contains("beta")

metrics = lqft.get_stats()
print(result, present, metrics["physical_nodes"])
```

### Mutable Frontend

If the priority is to get much closer to Python dict on hot mutable workloads, use the mutable frontend and freeze into the native engine only when you need the structural LQFT form.

```python
from lqft_engine import MutableLQFT

mutable = MutableLQFT()
mutable.insert("alpha", "value-a")
mutable.insert("beta", "value-b")

print(mutable.search("alpha"))
print(mutable.contains("beta"))

native_snapshot = mutable.freeze()
print(native_snapshot.search("alpha"))
```

When the native mutable hash-table methods are available, `MutableLQFT` uses them automatically; otherwise it falls back to a Python dict frontend. This is the recommended path when you want dict-like mutation speed first and native LQFT structure second.

### Testing

Run the current regression baseline with:

```bash
python -m unittest discover -s tests -v
```

The Phase 1 suite covers insert, search, delete, freeze materialization, and baseline dedup/value-pool metrics.

### Benchmarking

Run the reproducible Phase 1 benchmark harness with:

```bash
python benchmarks/run_baseline.py --dataset-size 20000 --repetitions 3 --output-dir benchmarks/results
```

The harness writes JSON and Markdown result files using the roadmap recording format. Supported workloads now run multiple trials and report median metrics by default, with per-trial `ops_per_sec` samples and a `trial_ops_cv_pct` stability metric included in the JSON payload. The artifact also records the exact benchmark command as `invocation_command`, and the Markdown summary repeats it in a footer. Persistent benchmark rows also include both `engine_scope` and `shared_native_components`, so the output makes it explicit when a workload is fully instance-local versus intentionally shared. Workloads that are not yet supported by the public API are recorded explicitly as unsupported rather than silently skipped.

Each new benchmark artifact also compares itself to the immediately previous result in the same output directory, so the Markdown summary and JSON payload both carry per-workload deltas without requiring a manual diff pass. That comparison layer only tracks supported workloads, and it records aggregation mode and trial-count mismatches too, so moving from single-run to median-based baselines does not silently look more precise than it is.

### Phase 4 Commit Trend

The Phase 4 benchmark story is now visible directly in the harness through `snapshot_heavy_workload`, `delete_heavy_snapshot_workload`, and `rollback_workload`.

| Dataset Size | Snapshot Heavy Ops/s | Delete-Heavy Ops/s | Delete vs Snapshot | Snapshot Heavy P50 | Delete-Heavy P50 | Snapshot Native Bytes | Delete-Heavy Native Bytes |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 500 | 479.322 | 528.701 | +10.3% | 2039.35 us | 1671.9 us | 48490 | 11664 |
| 5000 | 103.698 | 136.936 | +32.1% | 9189.6 us | 6158.2 us | 491066 | 124918 |

Current interpretation:

- The native tombstone and one-shot delta-commit path is improving delete-heavy snapshot creation relative to the plain snapshot-heavy workload, and that relative advantage widened between the 500-key and 5000-key artifacts.
- The delete-heavy workload still ended with higher total RSS in both comparison runs, so the repository should not claim a general memory win from the current Phase 4 path.
- `rollback_workload` remains much cheaper than snapshot creation, but its current benchmark row is still noisy enough that it should be treated as a separate operational class rather than as a precise commit comparator.

## Public API

The primary user-facing model in `lqft_engine` is:

- `LQFTMap`: unified mutable map with explicit snapshot handles

Compatibility surfaces that remain available:

- `LQFT`: persistent native trie-backed engine
- `MutableLQFT`: mutable frontend optimized for active write and mixed workloads

Other implementation details in the repository are internal and should not be relied on as public imports.

## License
MIT License - Parjad Minooei (2026).
