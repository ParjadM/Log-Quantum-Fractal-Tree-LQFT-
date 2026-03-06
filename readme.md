# Log-Quantum Fractal Tree (LQFT)

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](#)
[![C-Engine](https://img.shields.io/badge/Native-C-red.svg)](#)
[![Concurrency](https://img.shields.io/badge/Concurrency-Benchmark_Dependent-yellow.svg)](#)
[![Architecture](https://img.shields.io/badge/Architecture-Merkle_HAMT-pink.svg)](#)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE.md)

## Project Overview

The **Log-Quantum Fractal Tree (LQFT)** is a native Python extension that combines HAMT-style routing with structural sharing. The project is still interesting as a systems exercise and as a specialized persistent structure, but the benchmark results in this repository do not support a general claim that LQFT is faster than mainstream in-memory structures in practice.


---

## Release Note (v1.1.6)

This release keeps the paired key/value batching patch, the native mutable frontend improvements, and tightens the public Python API to two supported models: `LQFT` and `MutableLQFT`.

What improved:

- `MutableLQFT` moved materially closer to Python `dict` in write-heavy, mixed, and churn-heavy workloads.
- The persistent `LQFT` wrapper kept improving in native-backed write and read paths.
- The public package surface is now intentionally limited to the two documented models.

What did not improve:

- LQFT is still not generally competitive with Python dict or straightforward hash-table implementations.
- Read-heavy workloads are still weaker than mainstream alternatives.
- Some results remain benchmark-dependent, especially persistent unique-insert throughput.

Practical claim for this release:

- v1.1.6 exposes two intended user-facing models: `LQFT` and `MutableLQFT`.
- v1.1.6 is a substantially better mutable/write-heavy LQFT than earlier releases.
- v1.1.6 is not a proof that LQFT beats common in-memory data structures overall.

---

## Performance Snapshot (v1.1.6)

Verified environment: Windows workstation, Python 3.14 local build, native extension compiled in-place, benchmark matrix used during development before packaging cleanup.

| Metric | Current Observation | Architectural Driver |
| :--- | :--- | :--- |
| **Pure Write Throughput** | **Improved strongly vs. v1.0.9/local baseline** | Native paired key/value batching for unique-value writes |
| **Pure Read Throughput** | **Still workload-dependent and behind dict/hash-table peers** | Traversal cost + concurrency overhead |
| **Mixed Throughput** | **Improved modestly at best** | Write batching helps, but read-side costs still dominate |
| **Memory Density** | **Tracked at runtime via `estimated_native_bytes / physical_nodes`** | Real node bytes + active child arrays + pooled values |
| **Practical Competitiveness** | **Not generally competitive yet** | Constant-factor overhead still too high |

Benchmark note: throughput is workload- and environment-dependent. The release claim for this package should stay conservative and centered on write-heavy improvement rather than broad superiority.

## Core Architecture

### 1. Hardware Synchronization (Thread Affinity & NUMA)
The LQFT explicitly pins OS threads to physical CPU cores to prevent scheduler migrations, guaranteeing that hot memory paths remain in the L1/L2 cache. Memory is mapped using `MAP_POPULATE` and `VirtualAlloc` to guarantee NUMA-local hardware proximity.

### 2. Lock-Free Search & Optimistic Concurrency
Read operations are 100% lock-free (RCU-inspired). Threads traverse the trie without acquiring mutexes or triggering atomic cache-line invalidations. Deallocations are deferred to Thread-Local Retirement Chains, eliminating global contention.

### 3. Scale-Invariant Big-O Complexity
The LQFT utilizes a fixed 64-bit hash space partitioned into 13 segments.
* **Time Complexity:** $O(1)$ — Every traversal requires exactly 13 hardware instructions.
* **Space Complexity:** $O(\Sigma)$ — Identical branches are folded into single pointers, mapping physical space to data *entropy* rather than data *volume*.

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

### Python Wrapper

The project is normally used through the wrapper, not by calling the C module directly. The supported public API consists of exactly two models: `LQFT` and `MutableLQFT`.

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

## Public API

The supported user-facing models in `lqft_engine` are:

- `LQFT`: persistent native trie-backed engine
- `MutableLQFT`: mutable frontend optimized for active write and mixed workloads

Other implementation details in the repository are internal and should not be relied on as public imports.

## License
MIT License - Parjad Minooei (2026).
