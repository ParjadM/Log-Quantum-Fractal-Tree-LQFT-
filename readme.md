# Log-Quantum Fractal Tree (LQFT) 🚀

**Architect:** [Parjad Minooei](https://www.linkedin.com/in/parjadminooei) 
**Portfolio:** [parjadm.ca](https://www.parjadm.ca/)

---

## 📌 Executive Summary

The **Log-Quantum Fractal Tree (LQFT)** is a high-performance, scale-invariant data structure engine designed for massive data deduplication and persistent state management. By bridging a **native C-Engine** with a **Python Foreign Function Interface (FFI)**, this project bypasses the Global Interpreter Lock (GIL) to achieve sub-microsecond search latencies and memory efficiency that scales with data entropy rather than data volume.

---

## 🧠 Formal Complexity Analysis

As a Systems Architect, I have engineered the LQFT to move beyond the linear limitations of standard Python structures.

### 1. Time Complexity: $O(1)$ (Scale-Invariant)
Unlike standard Trees ($O(\log N)$) or Lists ($O(N)$), the LQFT uses a fixed-depth 64-bit address space.

* **Search/Insertion:** $O(1)$
* **Mechanism:** The 64-bit hash is partitioned into 13 segments of 5-bits. This ensures that the path from the root to any leaf is physically capped at 13 hops, providing **deterministic latency** regardless of whether the database holds 1,000 or 1,000,000,000 items.

### 2. Space Complexity: $O(\Sigma)$ (Entropy-Based)
Standard structures scale linearly based on the number of items ($N$). The LQFT scales based on the **Information Entropy** ($\Sigma$) of the dataset.

* **Space:** $O(\Sigma)$
* **Mechanism:** Utilizing **Merkle-DAG structural folding**, the engine detects identical data branches and reuses them in physical memory. In highly redundant datasets (e.g., DNA sequences or Log files), this results in sub-linear memory growth.

---

## 📊 Performance Benchmarks
*Tested in Scarborough Lab: Python 3.12 | MinGW-w64 GCC-O3 Optimization*

| Metric | Standard Python ($O(N)$) | LQFT C-Engine ($O(1)$) | Delta |
| :--- | :--- | :--- | :--- |
| **Search Latency (N=100k)** | ~3,564.84 μs | 0.50 μs | **7,129x Faster** |
| **Insertion Time (N=100k)** | 41.05s | 1.07s | **38x Faster** |
| **Memory (Versioning)** | $O(N \times V)$ | $O(\Sigma + V)$ | **99% Savings** |

---

## 🛠️ Architectural Pillars

* **Native C-Engine Core:** Pushes memory allocation and bit-manipulation to the C-layer for hardware-level execution.
* **Structural Folding:** A recursive structural hashing algorithm that collapses identical sub-trees into single pointers.
* **Adaptive Migration:** A polymorphic wrapper (`AdaptiveLQFT`) that manages the transition from lightweight Python dictionaries to the heavy-duty C-Engine.
* **Zero-Knowledge Integrity:** Fixed-depth pathing allows for 208-byte Merkle Proofs to verify data existence in microsecond time.

---

## ⚙️ Quick Start

### Compilation
Ensure you have a C compiler (GCC/Clang) installed to build the FFI layer.
```bash
python setup.py build_ext --inplace
```

### Usage 
from lqft_engine import AdaptiveLQFT

# Initialize engine with an auto-migration threshold
engine = AdaptiveLQFT(migration_threshold=50000)

# Insert and Search
engine.insert("secret_key", "confidential_data")
result = engine.search("secret_key")

print(f"Found: {result}")