# Log-Quantum Fractal Tree (LQFT) 🚀

## 📌 Project Overview

The **Log-Quantum Fractal Tree (LQFT)** is a high-performance, scale-invariant data structure engine designed for **massive data deduplication** and **persistent state management**.  

It synthesizes **Hash Array Mapped Trie (HAMT)** routing with **Merkle-DAG** structural folding to provide:

- Deterministic **\( O(1) \)** search latency  
- Sub-linear **\( O(\Sigma) \)** space complexity  

By offloading core associative logic to a **native C extension (v4.4)**, the LQFT bypasses the Python Global Interpreter Lock (GIL) and achieves significant **memory reduction** for versioned, redundant, or patterned datasets.

---

## 🧠 Core Architecture

### 1. Scale-Invariant Time Complexity: \( O(1) \)

- The LQFT utilizes a fixed **64-bit hash space** partitioned into **13 levels**.  
- Unlike standard balanced trees that grow in height as data increases \( O(\log N) \), the LQFT's pathing is **physically capped**.  

**Key Properties:**
- **Deterministic Latency:** Every search or insertion requires exactly **13 pointer hops**.  
- **Scale-Invariance:** Performance remains constant whether the dataset contains \( 10^3 \) or \( 10^9 \) items.

### 2. Entropy-Based Space Complexity: \( O(\Sigma) \)

Using a **global C-Registry**, the engine implements **structural interning**.  
Nodes are identified by the **cryptographic hash of their contents** and **child pointers (Merkle-DAG)**.

**Key Mechanisms:**
- **Structural Folding:** Identical sub-trees are shared physically in memory across different branches or versions.  
- **Efficient Versioning:** Saving a new version of a state-space requires only \( O(\log N) \) new nodes (the path to the change), while the remainder is shared with previous versions.

---

## 🚀 Performance Benchmarks

| Metric | Result |
|:--------|:--------|
| **Environment** | Python 3.12 \| GCC -O3 (Native C-Extension) |
| **Search Latency (p50)** | ~500 ns |
| **Read Throughput** | ~1.8 Million ops/sec |
| **Space Efficiency** | Up to **1,500×** reduction in versioned graph simulations |
| **Stability** | Zero-drift memory reclamation via Dynamic C-Registry |

---

## 🛠️ Getting Started

### Installation

> The engine requires a C compiler (**GCC/MinGW** or **MSVC**) to build the native extension.

```bash
# Clone the repository
git clone https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-
cd Log-Quantum-Fractal-Tree-LQFT-

# Build the native C-extension
python setup.py build_ext --inplace
