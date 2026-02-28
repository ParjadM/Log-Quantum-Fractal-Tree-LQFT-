import time
import random
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    import lqft_c_engine
    C_ENGINE_READY = True
except ImportError:
    C_ENGINE_READY = False

# ---------------------------------------------------------
# 1. Search Complexity Test
# ---------------------------------------------------------
def run_crossover_test():
    # We test from tiny (10) to massive (100,000)
    scales = [10, 100, 1000, 10000, 100000]
    
    print("\n" + "="*75)
    print(" 🏁 THE CROSSOVER TEST: PYTHON LIST O(n) vs LQFT O(1)")
    print(" Goal: Find the point where the 'Constant Factor' is finally worth it.")
    print("="*75)
    print(f"{'Items (N)':<12} | {'List Search':<18} | {'LQFT Search':<18} | {'Winner'}")
    print("-" * 75)

    for n in scales:
        # Prep data
        data_list = [f"item_{i}" for i in range(n)]
        search_target = data_list[-1] # Worst case for list: search the very last item
        
        # Prep LQFT
        for i in range(n):
            h = abs(hash(f"item_{i}")) & 0xFFFFFFFFFFFFFFFF
            lqft_c_engine.insert(h, "val")

        TRIALS = 5000
        
        # 1. List Search Time (O(n))
        start = time.time()
        for _ in range(TRIALS):
            # The 'in' operator in Python is a linear O(n) scan
            _ = search_target in data_list
        list_time = (time.time() - start) / TRIALS * 10**6

        # 2. LQFT Search Time (O(1))
        h_target = abs(hash(search_target)) & 0xFFFFFFFFFFFFFFFF
        start = time.time()
        for _ in range(TRIALS):
            lqft_c_engine.search(h_target)
        lqft_time = (time.time() - start) / TRIALS * 10**6

        winner = "LIST" if list_time < lqft_time else "LQFT 🏆"
        
        print(f"{n:<12,} | {list_time:>10.3f} μs | {lqft_time:>10.3f} μs | {winner}")

    print("\n" + "="*75)
    print(" 🧠 SYSTEMS ARCHITECT LESSON")
    print("="*75)
    print("1. THE SMALL DATA PENALTY: At N=10, the List is likely faster.")
    print("   The CPU can scan a tiny array faster than it can calculate a hash")
    print("   and jump through 13 C-pointers. This is 'Constant Factor' at work.")
    
    print("\n2. THE LINEAR DEATH: By N=1,000 or N=10,000, the List search time")
    print("   explodes because it has to check thousands of items one-by-one.")
    
    print("\n3. THE O(1) STABILITY: Notice the LQFT search time stays almost")
    print("   IDENTICAL whether N is 10 or 100,000. This is 'Scale-Invariance'.")
    print("="*75 + "\n")

if __name__ == "__main__":
    if C_ENGINE_READY:
        run_crossover_test()
    else:
        print("C-Engine not found. Run 'python setup.py build_ext --inplace'")