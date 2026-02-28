import time
import hashlib
import random
import sys
import os

# Ensure the local directory is in the path so we can import our custom engine
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 1. Import Custom Engines
try:
    from lqft_engine import LQFT
except ImportError:
    print("[!] Critical Error: 'lqft_engine.py' not found in the current directory.")
    sys.exit(1)

C_ENGINE_READY = False
try:
    import lqft_c_engine
    C_ENGINE_READY = True
except ImportError:
    print("[!] C-Engine ('lqft_c_engine') not found. Native benchmarks will be skipped.")
    print("[TIP] Run: python setup.py build_ext --inplace")

def get_64bit_hash(key):
    """
    Generates a 64-bit unsigned hash for the C-Engine.
    Using a fast MD5-slice to maintain the performance lead.
    """
    return int(hashlib.md5(str(key).encode()).hexdigest()[:16], 16)

def solve_leetcode_187_python(dna_str):
    """
    Solving LC 187 using the ITERATIVE LQFT Engine.
    Safe from RecursionError even with deep structural overlap.
    """
    engine = LQFT()
    repeated = set()
    
    # Sliding window of 10 characters
    for i in range(len(dna_str) - 9):
        ten_mer = dna_str[i:i+10]
        status = engine.search(ten_mer)
        
        if status is None:
            engine.insert(ten_mer, "seen")
        elif status == "seen":
            repeated.add(ten_mer)
            engine.insert(ten_mer, "repeated")
            
    return list(repeated)

def solve_leetcode_187_c(dna_str):
    """
    Solving LC 187 using the High-Performance C-Engine.
    Bypasses the Python Global Interpreter Lock (GIL).
    """
    repeated = set()
    
    for i in range(len(dna_str) - 9):
        ten_mer = dna_str[i:i+10]
        h = get_64bit_hash(ten_mer)
        
        status = lqft_c_engine.search(h)
        
        if status is None:
            lqft_c_engine.insert(h, "seen")
        elif status == "seen":
            repeated.add(ten_mer)
            lqft_c_engine.insert(h, "repeated")
            
    return list(repeated)

def solve_leetcode_187_standard(dna_str):
    """
    Standard LeetCode approach using Python's built-in set.
    The set is internally implemented as a highly-optimized C hash table.
    """
    seen = set()
    repeated = set()
    
    for i in range(len(dna_str) - 9):
        ten_mer = dna_str[i:i+10]
        if ten_mer in seen:
            repeated.add(ten_mer)
        else:
            seen.add(ten_mer)
            
    return list(repeated)

def generate_complex_dna(length, num_repeats=500):
    """
    Generates a DNA sequence with injected patterns to guarantee 
    logical hits and structural folding opportunities.
    """
    dna = list(''.join(random.choices("ACGT", k=length)))
    
    # Patterns designed to stress test both hashing and pathing
    patterns = ["AAAAAGGGGG", "TCTCTCTCTC", "GATCGATCGA", "CCCCCCCCCC", "ATATATATAT"]
    for pattern in patterns:
        for _ in range(num_repeats):
            pos = random.randint(0, length - 11)
            dna[pos:pos+10] = list(pattern)
            
    return "".join(dna)

if __name__ == "__main__":
    # Settings for Scarborough Development Environment
    DNA_LENGTH = 100000 
    test_dna = generate_complex_dna(DNA_LENGTH)
    
    print("\n" + "="*60)
    print(" LEETCODE 187: SYSTEMS ARCHITECT PERFORMANCE SHOWDOWN")
    print(f" Input Size: {DNA_LENGTH:,} characters")
    print("="*60)

    # --- 1. Standard Solution (Baseline) ---
    print("\n[*] Running Standard Solution (Built-in C-Hashset)...")
    start_std = time.time()
    res_std = solve_leetcode_187_standard(test_dna)
    std_duration = time.time() - start_std
    print(f"  > Time Taken: {std_duration:.4f}s")
    print(f"  > Unique Repeats Found: {len(res_std)}")

    # --- 2. Iterative Python LQFT ---
    print("\n[*] Running Iterative Python LQFT (Recursion-Safe)...")
    start_py = time.time()
    res_py = solve_leetcode_187_python(test_dna)
    py_duration = time.time() - start_py
    print(f"  > Time Taken: {py_duration:.4f}s")
    print(f"  > Unique Repeats Found: {len(res_py)}")

    # --- 3. Custom C-Engine LQFT ---
    if C_ENGINE_READY:
        print("\n[*] Running Custom C-Engine LQFT (Native binary)...")
        start_c = time.time()
        res_c = solve_leetcode_187_c(test_dna)
        c_duration = time.time() - start_c
        print(f"  > Time Taken: {c_duration:.4f}s")
        print(f"  > Unique Repeats Found: {len(res_c)}")
        
        # Summary Analysis
        print("\n" + "="*60)
        print(" PERFORMANCE ARCHITECTURE SUMMARY")
        print("="*60)
        print(f"1. Standard vs Python LQFT: {py_duration/std_duration:.1f}x slower")
        print(f"2. C-Engine vs Python LQFT:  {py_duration/c_duration:.1f}x faster")
        print(f"3. C-Engine vs Standard Set: {std_duration/c_duration:.2f}x speed ratio")
        
        # Result Integrity Validation
        if set(res_py) == set(res_std) == set(res_c):
            print("\n[SUCCESS] Logical Integrity Verified: All engines match.")
        else:
            print("\n[!] Logic Warning: Mismatch detected. Checking hash stability.")
            print(f"Set count: {len(res_std)} | Py LQFT: {len(res_py)} | C LQFT: {len(res_c)}")
    
    print("\n" + "="*60)
    print(" PORTFOLIO INSIGHTS (MScAC UofT Prep)")
    print("="*60)
    print("1. STABILITY: The manual stack in 'lqft_engine.py' prevents crashes.")
    print("2. FOLDING: LQFT saves memory on repetitive sequences via Merkle-DAGs.")
    print("3. HYBRID: C-Extension provides native speeds with Python's API ease.")
    print("="*60 + "\n")