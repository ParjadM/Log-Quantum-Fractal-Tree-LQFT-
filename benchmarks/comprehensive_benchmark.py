import time
import random
import string
import hashlib
import sys
from lqft_engine import LQFT, LQFTNode

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__slots__'):
        size += sum([get_size(getattr(obj, s), seen) for s in obj.__slots__ if hasattr(obj, s)])
    return size

def run_scenario(name, keys, values):
    print(f"\n>>> SCENARIO: {name}")
    lqft = LQFT()
    n = len(keys)
    
    # Measure Insertion Time
    start_time = time.time()
    for k, v in zip(keys, values):
        lqft.insert(k, v)
    end_time = time.time()
    
    insert_duration = end_time - start_time
    
    # Measure Search Time (O(1) Verification)
    search_keys = random.sample(keys, min(1000, n))
    start_search = time.time()
    for sk in search_keys:
        lqft.search(sk)
    end_search = time.time()
    
    avg_search = (end_search - start_search) / len(search_keys)
    
    # Measure Physical Memory
    unique_nodes = len(LQFTNode._registry)
    efficiency = (1 - (unique_nodes / n)) * 100
    
    print(f"  - Items Inserted: {n}")
    print(f"  - Insertion Time: {insert_duration:.4f}s")
    print(f"  - Avg Search Time: {avg_search*1000000:.2f}μs")
    print(f"  - Physical Nodes: {unique_nodes}")
    print(f"  - Compression Gain: {efficiency:.2f}%")
    
    # Reset registry for next scenario
    LQFTNode._registry.clear()

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_dna_sequence(length=1000000):
    """Generates a long DNA string for LeetCode 187 simulation"""
    return ''.join(random.choices("ACGT", k=length))

if __name__ == "__main__":
    print("====================================================")
    print("   LQFT MULTI-COMPLEXITY PERFORMANCE ANALYSIS")
    print("====================================================")
    
    # Scenario 1: High Redundancy (Log files / Repetitive sensor data)
    n_high = 100000
    keys_high = [f"sensor_id_{i}" for i in range(n_high)]
    values_high = [f"STATUS_OK_VAL_{i % 5}" for i in range(n_high)]
    run_scenario("High Redundancy (5 Unique Values)", keys_high, values_high)

    # Scenario 2: Unique Data (UUIDs / Random hashes)
    n_unique = 50000
    keys_unique = [hashlib.md5(str(i).encode()).hexdigest() for i in range(n_unique)]
    values_unique = [generate_random_string(20) for _ in range(n_unique)]
    run_scenario("Low Redundancy (Fully Unique Data)", keys_unique, values_unique)

    # Scenario 3: Structural Overlap (Hierarchical paths / DNA)
    n_struct = 50000
    base_paths = ["/users/parjad/desktop/portfolio/", "/users/admin/system/logs/", "/tmp/cache/temp/"]
    keys_struct = [random.choice(base_paths) + generate_random_string(5) for _ in range(n_struct)]
    values_struct = ["BINARY_DATA_BLOB" for _ in range(n_struct)]
    run_scenario("Structural Overlap (File Paths)", keys_struct, values_struct)

    # Scenario 4: LeetCode 187 Simulation (Repeated DNA Sequences)
    # We find all 10-letter-long sequences that occur more than once.
    # Standard solution uses a Set. LQFT will use Deduplication to save space.
    print("\n>>> LEETCODE 187: Repeated DNA Sequences Simulation")
    dna_string = generate_dna_sequence(50000)
    dna_lqft = LQFT()
    repeated = []
    
    start_lc = time.time()
    for i in range(len(dna_string) - 9):
        seq = dna_string[i:i+10]
        count = dna_lqft.search(seq)
        if count is None:
            dna_lqft.insert(seq, 1)
        elif count == 1:
            repeated.append(seq)
            dna_lqft.insert(seq, 2)
    end_lc = time.time()
    
    nodes_lc = len(LQFTNode._registry)
    print(f"  - DNA Length: 50,000")
    print(f"  - 10-letter Subsequences Processed: {len(dna_string) - 9}")
    print(f"  - Total Repeated Sequences Found: {len(repeated)}")
    print(f"  - Execution Time: {end_lc - start_lc:.4f}s")
    print(f"  - LQFT Physical Nodes: {nodes_lc}")
    print(f"  - Space Efficiency: {(1 - (nodes_lc / (len(dna_string)-9)))*100:.2f}%")
    LQFTNode._registry.clear()

    print("\n====================================================")
    print("Analysis Complete.")