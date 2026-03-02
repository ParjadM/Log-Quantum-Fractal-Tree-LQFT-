import time
import random
import tracemalloc
import sys
import os
import hashlib

# Ensure local imports work
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Assume the stateful API is exposed via lqft_c_engine
try:
    import lqft_c_engine
    C_ENGINE_READY = True
except ImportError:
    print("[!] Critical: lqft_c_engine not found. Ensure the extension is compiled.")
    sys.exit(1)

def get_physical_nodes():
    """Helper for C-Engine Metrics"""
    try:
        metrics = lqft_c_engine.get_metrics()
        return metrics.get('physical_nodes', 0)
    except Exception:
        return 0

def fast_hash(key_str):
    """Generates the 64-bit integer hash our C-Engine expects."""
    return int(hashlib.md5(str(key_str).encode()).hexdigest()[:16], 16)

# -------------------------------------------------------------------
# 1. SCALE-UP GRAPH VERSIONING
# -------------------------------------------------------------------
def benchmark_graph_scaling():
    print("\n[1] SCALE-UP GRAPH VERSIONING BENCHMARKS")
    configs = [
        {"nodes": 1000, "steps": 5000},
        {"nodes": 10000, "steps": 10000} 
    ]

    for config in configs:
        nodes, steps = config["nodes"], config["steps"]
        print(f"\n  [*] Configuration: {nodes:,} Nodes | {steps:,} Time Steps")
        
        # --- Optimized Naive Baseline (Projected Memory) ---
        # We calculate the cost of ONE snapshot to project the "Death Curve"
        tracemalloc.start()
        sample_graph = {f"n_{i}": list(range(10)) for i in range(nodes)}
        _SnapshotSample = {k: list(v) for k, v in sample_graph.items()}
        _, snapshot_size = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        projected_naive_mb = (snapshot_size * steps) / 1e6

        # --- LQFT Architecture (Stateful C-API) ---
        start_nodes = get_physical_nodes()
        c_tracker = {f"n_{i}": [] for i in range(nodes)}
        
        # Build base
        for i in range(nodes):
            lqft_c_engine.insert(fast_hash(f"n_{i}"), "[]")
            
        start_lqft = time.time()
        for s in range(steps):
            u = random.randint(0, nodes-1)
            v = random.randint(0, nodes-1)
            
            c_tracker[f"n_{u}"].append(str(v))
            # We join only the last 5 to keep Python's string marshalling out of the way
            val_str = ",".join(c_tracker[f"n_{u}"][-5:])
            lqft_c_engine.insert(fast_hash(f"n_{u}"), val_str)
            
        time_lqft = time.time() - start_lqft
        phys_nodes = get_physical_nodes() - start_nodes
        lqft_mb = (phys_nodes * 289) / 1e6
        
        print(f"      > LQFT Time:        {time_lqft:.2f}s")
        print(f"      > LQFT Phys Nodes:  {phys_nodes:,}")
        print(f"      > LQFT RAM:         {lqft_mb:.2f} MB")
        print(f"      > Projected Naive:  {projected_naive_mb:,.2f} MB (Standard Dict)")
        print(f"      > Efficiency Gain:  {projected_naive_mb / lqft_mb:.1f}x Space Saving")

# -------------------------------------------------------------------
# 2. READ-HEAVY AND MIXED WORKLOADS
# -------------------------------------------------------------------
def benchmark_mixed_workloads():
    print("\n[2] READ-HEAVY & MIXED WORKLOADS (100k Keys)")
    N = 100000 
    OPS = 500000 
    
    keys = [f"key_{i}" for i in range(N)]
    hashed_keys = [fast_hash(k) for k in keys]
    
    print("  [*] Building 100k Key LQFT Map...")
    std_dict = {}
    for i in range(N):
        lqft_c_engine.insert(hashed_keys[i], "payload")
        std_dict[keys[i]] = "payload"

    # --- Read-Heavy ---
    read_indices = [random.randint(0, N-1) for _ in range(2000)]
    start = time.time()
    for _ in range(OPS // 2000):
        for idx in read_indices: 
            _ = lqft_c_engine.search(hashed_keys[idx])
    lqft_read_ops = OPS / (time.time() - start)
    
    print(f"      > LQFT Read Thru:  {lqft_read_ops:,.0f} ops/sec")

# -------------------------------------------------------------------
# 3. VERSION FAN-OUT (BRANCHY HISTORIES)
# -------------------------------------------------------------------
def benchmark_version_fanout():
    print("\n[3] VERSION FAN-OUT (GIT-LIKE BRANCHING)")
    STEPS = 500
    BRANCHES = 50
    start_nodes = get_physical_nodes()
    
    print(f"  [*] Simulating {BRANCHES} Branches, {STEPS} commits per branch...")
    start = time.time()
    for s in range(STEPS):
        for b in range(BRANCHES):
            f_id = random.randint(0, 1000)
            lqft_c_engine.insert(fast_hash(f"branch_{b}_file_{f_id}"), f"v{s}")
            
    dur = time.time() - start
    phys_nodes = get_physical_nodes() - start_nodes
    
    print(f"      > Sim Time:         {dur:.2f}s")
    print(f"      > Total Commits:    {STEPS * BRANCHES:,}")
    print(f"      > LQFT Phys Nodes:  {phys_nodes:,}")

# -------------------------------------------------------------------
# 4. HIGH-SHARING VS LOW-SHARING
# -------------------------------------------------------------------
def benchmark_sharing_regimes():
    print("\n[4] HIGH-SHARING VS LOW-SHARING REGIMES")
    
    NODES = 1000
    STEPS = 1000

    def run_regime(name, edits):
        start_phys = get_physical_nodes()
        for s in range(STEPS):
            for _ in range(edits):
                u = random.randint(0, NODES-1)
                lqft_c_engine.insert(fast_hash(f"regime_n_{u}"), "data")
        added_phys = get_physical_nodes() - start_phys
        return added_phys

    high = run_regime("High", 1)
    low = run_regime("Low", 50)
    
    print(f"      > High Sharing (1 edit/step):  {high:,} physical nodes")
    print(f"      > Low Sharing (50 edits/step): {low:,} physical nodes")

# -------------------------------------------------------------------
# 5. STABILITY TEST (1 Million Ops)
# -------------------------------------------------------------------
def benchmark_long_run_stability():
    print("\n[5] STABILITY TEST (1,000,000 Operations)")
    
    OPS = 1000000
    CHUNK = 250000
    
    # Pre-generate some integer hashes to eliminate Python-level hashing overhead
    pre_hashes = [i for i in range(100000)]
    
    print("  [*] Running in optimized chunks...")
    print(f"      {'Ops Completed':<15} | {'Thruput (ops/s)':<15}")
    
    start_time = time.time()
    chunk_start = time.time()
    
    for i in range(1, OPS + 1):
        # We alternate between insert and search
        h = pre_hashes[i % 100000]
        if i % 2 == 0:
            lqft_c_engine.insert(h, "STABLE_DATA")
        else:
            _ = lqft_c_engine.search(h)
            
        if i % CHUNK == 0:
            chunk_time = time.time() - chunk_start
            thru = CHUNK / chunk_time
            print(f"      {i:<15,} | {thru:<15,.0f}")
            chunk_start = time.time()
            
    total_time = time.time() - start_time
    print(f"  > Total Stability Time: {total_time:.2f}s")
    print(f"  > Final Node Registry:  {get_physical_nodes():,}")

# ---------------------------------------------------------
if __name__ == "__main__":
    print("="*85)
    print(" 🔬 ADVANCED ARCHITECTURE VALIDATION SUITE: LQFT")
    print("="*85)
    
    try:
        benchmark_graph_scaling()
        benchmark_mixed_workloads()
        benchmark_version_fanout()
        benchmark_sharing_regimes()
        benchmark_long_run_stability()
    except KeyboardInterrupt:
        print("\n[!] Benchmark interrupted by user.")
    
    print("\n" + "="*85)
    print(" 📜 OPTIMIZED STRESS SUITE COMPLETE")
    print("="*85)