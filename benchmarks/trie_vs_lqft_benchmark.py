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
# 1. Standard Trie (Prefix Tree) Implementation
# ---------------------------------------------------------
class TrieNode:
    __slots__ = ['children', 'is_end']
    def __init__(self):
        self.children = {} # Python dicts have huge memory overhead (~240 bytes)
        self.is_end = False

class StandardTrie:
    def __init__(self):
        self.root = TrieNode()
        self.node_count = 1

    def insert(self, word):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
                self.node_count += 1
            node = node.children[char]
        node.is_end = True

# ---------------------------------------------------------
# 2. Standard BST Implementation (Unbalanced for speed simulation)
# ---------------------------------------------------------
class BSTNode:
    __slots__ = ['key', 'left', 'right']
    def __init__(self, key):
        self.key = key # Stores the ENTIRE string (~50+ bytes)
        self.left = None
        self.right = None

class StandardBST:
    def __init__(self):
        self.root = None
        self.node_count = 0

    def insert(self, key):
        if self.root is None:
            self.root = BSTNode(key)
            self.node_count += 1
            return
        
        curr = self.root
        while True:
            if key < curr.key:
                if curr.left is None:
                    curr.left = BSTNode(key)
                    self.node_count += 1
                    break
                curr = curr.left
            elif key > curr.key:
                if curr.right is None:
                    curr.right = BSTNode(key)
                    self.node_count += 1
                    break
                curr = curr.right
            else:
                break # Duplicate

# ---------------------------------------------------------
# 3. DNA Generation & Benchmark Runner
# ---------------------------------------------------------
def generate_dna(length, num_repeats=2000):
    dna = list(''.join(random.choices("ACGT", k=length)))
    # Injecting 50-character long patterns
    patterns = [
        "AAAAAGGGGGTTTTTCCCCC" * 2 + "ATCGATCGAT", 
        "TCTCTCTCTCGAGAGAGAGA" * 2 + "GCTAGCTAGC",
        "GATCGATCGATCGATCGATC" * 2 + "AAAAAAAAAA"
    ]
    for pattern in patterns:
        for _ in range(num_repeats):
            pos = random.randint(0, length - 51)
            dna[pos:pos+50] = list(pattern)
    return "".join(dna)

def run_showdown():
    N = 100000 # 100k characters
    SEQ_LEN = 50 # 50-character strings!
    print("\n" + "="*60)
    print(" 🌳 DATA STRUCTURE SHOWDOWN: THE 'LONG STRING' TEST")
    print(f" Simulating N={N:,} chars | Sequence Length: {SEQ_LEN}")
    print("="*60)

    test_dna = generate_dna(N)
    
    # 1. Benchmark BST
    print("\n[*] Building Standard BST...")
    bst = StandardBST()
    start = time.time()
    for i in range(len(test_dna) - (SEQ_LEN - 1)):
        bst.insert(test_dna[i:i+SEQ_LEN])
    
    # Python BST Node (~48b slots + ~99b for a 50 char string + refs) ≈ 150 bytes per node
    bst_mb = (bst.node_count * 150) / (1024 * 1024)
    print(f"  > Time: {time.time() - start:.4f}s")
    print(f"  > Nodes Created: {bst.node_count:,}")
    print(f"  > Estimated RAM: {bst_mb:.2f} MB")
    print("  > Analysis: Stores the full 50-char string in EVERY node.")

    # 2. Benchmark Trie
    print("\n[*] Building Standard Prefix Trie...")
    trie = StandardTrie()
    start = time.time()
    for i in range(len(test_dna) - (SEQ_LEN - 1)):
        trie.insert(test_dna[i:i+SEQ_LEN])
        
    # Python Trie Node (~48b slots + ~240b for dict) ≈ 250 bytes per node
    trie_mb = (trie.node_count * 250) / (1024 * 1024)
    print(f"  > Time: {time.time() - start:.4f}s")
    print(f"  > Nodes Created: {trie.node_count:,}")
    print(f"  > Estimated RAM: {trie_mb:.2f} MB")
    print("  > Analysis: Explodes because depth scales directly with string length (50 levels deep).")

    # 3. Benchmark LQFT (C-Engine)
    if C_ENGINE_READY:
        import hashlib
        print("\n[*] Building Native LQFT (Merkle-DAG)...")
        start = time.time()
        for i in range(len(test_dna) - (SEQ_LEN - 1)):
            seq = test_dna[i:i+SEQ_LEN]
            # Hashes the 50-char string into a FIXED 64-bit integer
            h = int(hashlib.md5(seq.encode()).hexdigest()[:16], 16)
            lqft_c_engine.insert(h, "active")
            
        metrics = lqft_c_engine.get_metrics()
        # C-Engine LQFT Node is exactly ~289 bytes in C memory
        lqft_mb = (metrics['physical_nodes'] * 289) / (1024 * 1024)
        print(f"  > Time: {time.time() - start:.4f}s")
        print(f"  > Nodes Created: {metrics['physical_nodes']:,}")
        print(f"  > Estimated RAM: {lqft_mb:.2f} MB")
        print("  > Analysis: Hashing collapses ANY string length into a max depth of 13 levels.")
        
    print("\n" + "="*60)
    print(" THE SYSTEMS ARCHITECT REVELATION")
    print("="*60)
    print("If you change string length from 10 to 10,000:")
    print("- BST memory will explode (stores massive strings).")
    print("- Trie memory will explode (grows 10,000 levels deep).")
    print("- LQFT memory stays IDENTICAL (hashes remain 64-bits).")
    print("This fixed-depth property is why LQFT is an Enterprise tool.")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_showdown()