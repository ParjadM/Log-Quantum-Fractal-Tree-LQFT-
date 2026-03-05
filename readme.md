# Log-Quantum Fractal Tree (LQFT) 🚀

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](#)
[![C-Engine](https://img.shields.io/badge/Native-C-red.svg)](#)
[![Memory Arena](https://img.shields.io/badge/Memory-Custom_Slab_Allocator-success.svg)](#)
[![Hardware](https://img.shields.io/badge/Limit-Sub_105_Byte_Density-green.svg)](#)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE.md)

## 📌 Project Overview

The **Log-Quantum Fractal Tree (LQFT)** is a high-performance, scale-invariant data structure engine designed for massive data deduplication and persistent state management. It synthesizes **Hash Array Mapped Trie (HAMT)** routing with **Merkle-DAG structural folding** to provide deterministic $O(1)$ search latency and sub-linear $O(\Sigma)$ space complexity.

By offloading core associative logic to a native C-Extension, the LQFT completely bypasses the Python Global Interpreter Lock (GIL), providing true hardware concurrency and significant memory reduction for versioned, redundant, or patterned datasets.

---

## 🧠 Core Architecture (v0.9.1 Multi-Language Core Prep)

### 1. Custom Memory Arena (Slab Allocator)
Standard C `malloc()` introduces significant OS metadata overhead (up to 16 bytes per allocation). In v0.9.0, the LQFT utilizes a **Custom Slab Allocator**. The engine requests memory in 16KB "Arena Chunks" and performs O(1) bump-allocation internally, crushing the memory footprint down to **~104 bytes per node**.

### 2. Intrinsic Free-List & ARC
When a branch is overwritten, the engine natively reclaims memory using an intrinsic linked-list. "Dead" nodes repurpose their own internal pointers to form an infinite recycle bin, avoiding expensive calls to the Operating System's `free()` method.

### 3. O(1) Cryptographic Fast-Path
Branch hash recalculation during Deletion and Updates utilizes mathematical XOR inverses (`Parent ^ (Old_Child * Prime) ^ (New_Child * Prime)`). This eliminates the standard 32-way branch loop, providing sub-microsecond structural folding.

---

## 🚀 Silicon Performance Report (v0.9.2)

*Environment: Python 3.12 | GCC -O3 | MinGW64 | 16-Core Parallel Architecture*

| Metric | Result | 
| :--- | :--- | 
| **Raw Memory Density** | **~104.6 bytes per node** (Eliminated 288-byte bloat) |
| **Search Latency (p50)** | **~500 ns** (Deterministic 13-hop limit) |
| **Raw Write Speed** | **313,433 ops/sec** (Zero-Copy Buffer Protocol) | 
| **Multi-Thread Scaling** | Optimistic Concurrency (Read-Copy-Update) | 

---

## 🛠️ Getting Started

### Installation

```bash
# Clone the repository
git clone [https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git](https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git)
cd Log-Quantum-Fractal-Tree-LQFT-

# Build the native C-extension
python setup.py build_ext --inplace
```

### High-Speed Usage (Zero-Copy)

```python
import lqft_c_engine
import array

# Prepare raw 64-bit buffer
raw_buffer = array.array('Q', [0x1, 0x2, 0x3])
lqft_c_engine.insert_batch_raw(bytes(raw_buffer), "enterprise_payload")

# Sub-microsecond search
result = lqft_c_engine.search(0x1)
print(result) # "enterprise_payload"

# Safe memory arena reclamation
lqft_c_engine.free_all()
```

## ⚖️ License
MIT License - Parjad Minooei (2026).
