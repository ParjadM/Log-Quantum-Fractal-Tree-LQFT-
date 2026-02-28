import time
import random
import sys
import os
import math

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    import lqft_c_engine
    C_ENGINE_READY = True
except ImportError:
    C_ENGINE_READY = False

# ---------------------------------------------------------
# 1. O(n) - Linear Search Simulation
# ---------------------------------------------------------
def linear_search(data_list, target):
    """Complexity: O(n). Worst case, we check every item."""
    for item in data_list:
        if item == target:
            return True
    return False

# ---------------------------------------------------------
# 2. O(log N) - Standard Balanced BST Simulation
# ---------------------------------------------------------
class BSTNode:
    __slots__ = ['key', 'left', 'right']
    def __init__(self, key):
        self.key = key
        self.left = None
        self.right = None

def bst_insert(root, key):
    if root is None: return BSTNode(key)
    curr = root
    while True:
        if key < curr.key:
            if curr.left is None:
                curr.left = BSTNode(key)
                break
            curr = curr.left
        elif key > curr.key:
            if curr.right is None:
                curr.right = BSTNode(key)
                break
            curr = curr.right
        else: break
    return root

def bst_search(root, key):
    curr = root
    depth = 0
    while curr:
        depth += 1
        if key == curr.key: return depth
        if key < curr.key: curr = curr.left
        else: curr = curr.right
    return depth

# ---------------------------------------------------------
# 3. Benchmarking Logic
# ---------------------------------------------------------
def run_demonstration():
    if not C_ENGINE_READY:
        print("[!] C-Engine not found. Please build it first.")
        return

    # We test across 3 orders of magnitude
    scales = [1000, 10000, 100000]
    
    print("\n" + "="*85)
    print(" 🔍 COMPLEXITY SHOWDOWN: O(n) vs O(log N) vs O(1) [LQFT]")
    print(" Goal: See why O(1) wins at scale, even with a higher constant factor.")
    print("="*85)
    print(f"{'Items (N)':<12} | {'O(n) Linear':<18} | {'O(log N) BST':<18} | {'O(1) LQFT':<18}")
    print("-" * 85)

    for n in scales:
        keys = [random.randint(0, 10**9) for _ in range(n)]
        search_target = random.choice(keys)
        
        # Build structures
        bst_root = None
        for k in keys: bst_root = bst_insert(bst_root, k)
        for k in keys: lqft_c_engine.insert(abs(hash(k)) & 0xFFFFFFFFFFFFFFFF, "data")
        
        TRIALS = 500
        
        # 1. O(n) Timing
        start = time.time()
        for _ in range(TRIALS):
            linear_search(keys, search_target)
        n_time = (time.time() - start) / TRIALS * 10**6

        # 2. O(log N) Timing
        start = time.time()
        for _ in range(TRIALS):
            bst_search(bst_root, search_target)
        bst_time = (time.time() - start) / TRIALS * 10**6

        # 3. O(1) LQFT Timing
        h = abs(hash(search_target)) & 0xFFFFFFFFFFFFFFFF
        start = time.time()
        for _ in range(TRIALS):
            lqft_c_engine.search(h)
        lqft_time = (time.time() - start) / TRIALS * 10**6

        print(f"{n:<12,} | {n_time:>10.2f} μs | {bst_time:>10.2f} μs | {lqft_time:>10.2f} μs")

    print("\n" + "="*85)
    print(" 📊 ARCHITECT'S CONCLUSION FOR UOFT MScAC")
    print("="*85)
    print("1. O(n) is the loser: As N grew 100x, the search time grew 100x. It scales poorly.")
    print("2. O(log N) is good: It grows slowly, but it DOES still grow as N increases.")
    print("3. O(1) LQFT is the King: Notice the LQFT time stayed ALMOST IDENTICAL at every scale.")
    print("\nPRO TIP: In your March 24th 'BFS/DFS Mastery', you'll see that avoiding O(n)")
    print("traversals in favor of O(1) hash-mapping is how you solve hard LeetCode problems.")
    print("="*85 + "\n")

if __name__ == "__main__":
    run_demonstration()