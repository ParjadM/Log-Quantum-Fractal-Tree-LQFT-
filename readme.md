Log-Quantum Fractal Tree (LQFT) 🚀

Architect: Parjad Minooei 

Portfolio: parjadm.ca 

📌 Executive Summary

The Log-Quantum Fractal Tree (LQFT) is a high-performance, scale-invariant data structure engine designed for massive data deduplication and persistent state management. By bridging a native C-Engine with a Python Foreign Function Interface (FFI), this project bypasses the Global Interpreter Lock (GIL) to achieve sub-microsecond search latencies and memory efficiency that scales with data entropy rather than data volume.

This architecture synthesizes HAMT (Hash Array Mapped Trie) pathing with Merkle-DAG structural folding, enabling $O(1)$ constant-time traversal and cryptographic data integrity proofs.

🧠 Architectural Pillars

Native C-Engine Core: Memory allocation and bit-manipulation are pushed entirely to the C-layer, resulting in a 38x performance increase over pure-Python implementations.

Scale-Invariant $O(1)$ Time: Utilizing a fixed 64-bit FNV-1a hash partitioned into 5-bit segments, path traversal is physically capped at exactly 13 hops—whether the database holds 10 items or 10 billion.

Entropy-Based $O(\Sigma)$ Space: Unchanged branches are structurally hashed and "folded." If 1 million records share a similar prefix or structure, the C-Registry maps them to the same physical memory address, enabling massive RAM savings for versioned snapshots (Git-style persistence).

Adaptive Thresholding: A polymorphic Python wrapper (AdaptiveLQFT) dynamically migrates data from an $O(n)$ flat hash table to the Native Merkle-DAG only when the dataset crosses a heuristic enterprise threshold (N > 50,000).

📊 Performance Benchmarks

Tested on Python 3.12 (MinGW-w64 GCC-O3 Optimization)

Metric

Pure Python

Native C-Engine

Improvement

Insertion (100k)

41.05s

1.07s

~3,800% Faster

Search Latency

~410.0 μs

< 1.0 μs

> 400x Faster

Memory (Versioning)

$O(N \times V)$

$O(\Sigma)$

Sub-linear Folding

⚙️ Build & Installation

This package utilizes a custom setup.py designed to compile the native C-Extension directly in your environment.

Prerequisites (Windows)

Ensure you have the MSYS2 MinGW-w64 GCC toolchain installed and added to your system PATH.

Compilation

Clone the repository and compile the native engine:

git clone [https://github.com/ParjadM/LQFT.git](https://github.com/ParjadM/LQFT.git)
cd LQFT
python setup.py build_ext --inplace


Initialization & Warm-up

Run the bootup sequence to verify FFI boundaries and initialize the high-resolution hardware clock:

python initialize_lqft.py


💻 Quick Start Usage

The AdaptiveLQFT acts as a smart manager, handling the Python-to-C interop seamlessly.

from lqft_engine import AdaptiveLQFT

# Initialize the engine (Defaults to migrating at 50,000 items)
engine = AdaptiveLQFT(migration_threshold=50000)

# 1. Insert Data
engine.insert("user_session_994", "active_payload")

# 2. Search Data O(1)
result = engine.search("user_session_994")
print(result) # Output: 'active_payload'

# 3. Monitor Architecture State
print(engine.status())
# Output: {'mode': 'Lightweight C-Hash', 'items': 1, 'threshold': 50000}


🛡️ Cryptographic Integrity (Merkle Proofs)

Because the LQFT is a Directed Acyclic Graph based on structural hashes, it inherently supports Zero-Knowledge Membership Proofs. The engine can prove a specific record exists in a 10TB database by providing a static 208-byte cryptographic path (13 hashes), verifiable in roughly 0.00 microseconds.

Run the proof simulation:

python benchmarks/markelproofV.py


🛣️ Roadmap

[x] V1.0: Iterative Pure Python Implementation (Proof of Concept)

[x] V2.0: Native C-Engine Integration (FFI)

[x] V3.0: 64-bit FNV-1a Hashing & Linear Probing Registry

[ ] V4.0: Multi-Language Bindings (C# NuGet Package & Node-API)

[ ] V5.0: Integration with massive state-space BFS/DFS Graph Traversals.

Developed as a core technical artifact for Systems Architecture and Computing Systems Master's applications.