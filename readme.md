# Log-Quantum Fractal Tree (LQFT) 🚀

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](#)
[![C-Engine](https://img.shields.io/badge/Native-C-red.svg)](#)
[![Systems Architecture](https://img.shields.io/badge/Architecture-Merkle_HAMT-pink.svg)](#)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE.md)

## 📌 Project Overview

The **Log-Quantum Fractal Tree (LQFT)** is a high-performance, scale-invariant data structure engine designed for massive data deduplication and persistent state management. It synthesizes **Hash Array Mapped Trie (HAMT)** routing with **Merkle-DAG structural folding** to provide deterministic $O(1)$ search latency and sub-linear $O(\Sigma)$ space complexity.

By offloading core associative logic to a native C-Extension, the LQFT bypasses the Python Global Interpreter Lock (GIL) and provides significant memory reduction for versioned, redundant, or patterned datasets.

---

## 🧠 Core Architecture (v0.5.0 Enterprise Release)

### 1. Scale-Invariant Time Complexity: $O(1)$

The LQFT utilizes a fixed 64-bit hash space partitioned into 13 levels. Unlike standard balanced trees that grow in height as data increases ($O(\log N)$), the LQFT's pathing is physically capped.

* **Deterministic Latency:** Every search or insertion requires exactly 13 pointer hops.
* **Scale-Invariance:** Performance remains constant whether the dataset contains 10^3 or 10^9 items.

### 2. Entropy-Based Space Complexity: $O(\Sigma)$

Utilizing a global C-Registry, the engine implements structural interning. Nodes are identified by the cryptographic hash of their contents and child pointers (Merkle-DAG).

* **Structural Folding:** Identical sub-trees are shared physically in memory across different branches or versions.

### 3. Native Disk Persistence (New in v0.5.0)

The LQFT now functions as a true Database Engine. It can serialize its entire memory layout directly to disk as a dense binary file.

* **Pointer Reconstruction:** Rebuilds C-Pointers instantly from disk on boot.
* **Process Isolation:** Survives system reboots and RAM clears.

---

## 🚀 Performance Benchmarks

*Environment: Python 3.12 | GCC -O3 (Native C-Extension)*

| Metric | Result | 
| :--- | :--- | 
| **Search Latency (p50)** | ~500 ns | 
| **Read Throughput** | ~1.8 Million ops/sec | 
| **Space Efficiency** | Up to 1,500x reduction in versioned graph simulations | 
| **Stability** | Automatic Reference Counting (ARC) with Zero-Footprint GC | 

---

## 🛠️ Getting Started

### Installation

The engine requires a C compiler (GCC/MinGW or MSVC) to build the native extension.

```bash
# Clone the repository
git clone [https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git](https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git)
cd Log-Quantum-Fractal-Tree-LQFT-

# Build the native C-extension
python setup.py build_ext --inplace
```

### Basic Usage

The LQFT provides a Pythonic interface to its high-performance C-backend.

```python
from lqft_engine import AdaptiveLQFT

# Initialize the Adaptive Wrapper
db = AdaptiveLQFT(migration_threshold=50000)

# Native insertion (Type Safe & Memory Guarded)
db.insert("user_001", "enterprise_payload")

# Sub-microsecond native search
result = db.search("user_001")
print(result) # "enterprise_payload"

# --- Phase 1: Disk Persistence ---
# Save the entire Merkle-DAG to a binary file
db.save_to_disk("production_state.bin")

# Clear RAM completely
db.clear()

# Cold Start Deserialization (Instant Reconstruction)
db.load_from_disk("production_state.bin")
```

---

## ⚖️ License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
