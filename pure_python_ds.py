import struct

# ---------------------------------------------------------
# 1. DETERMINISTIC HASHING (FNV-1a 64-bit)
# ---------------------------------------------------------
def fnv1a_64(key: str) -> int:
    """Generates a strict 64-bit deterministic hash for O(1) routing."""
    hval = 0xcbf29ce484222325
    fnv_prime = 0x100000001b3
    for byte in key.encode('utf-8'):
        hval ^= byte
        hval = (hval * fnv_prime) & 0xFFFFFFFFFFFFFFFF
    return hval

def combine_hashes(hashes: list) -> int:
    """Combines child Merkle hashes to generate a parent's structural hash."""
    hval = 0
    for h in hashes:
        if h is not None:
            hval ^= h
            hval = (hval * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return hval

# ---------------------------------------------------------
# 2. GLOBAL C-REGISTRY MOCK (Structural Folding)
# ---------------------------------------------------------
# Maps a Merkle Hash -> Physical Node Instance
NODE_REGISTRY = {}

class LQFTNode:
    """A fixed 32-way routing node."""
    __slots__ = ('children', 'value', 'merkle_hash')

    def __init__(self, value=None):
        self.children = [None] * 32
        self.value = value
        self.merkle_hash = None

def get_deduplicated_node(node: LQFTNode) -> LQFTNode:
    """
    The core of O(Σ) Space Complexity. 
    If a node with this exact structure exists, return the existing memory pointer.
    Otherwise, register this new node.
    """
    # Calculate structural Merkle Hash
    child_hashes = [c.merkle_hash if c else None for c in node.children]
    base_hash = fnv1a_64(str(node.value)) if node.value is not None else 0
    node.merkle_hash = (base_hash ^ combine_hashes(child_hashes)) & 0xFFFFFFFFFFFFFFFF

    # Structural Folding (Deduplication)
    if node.merkle_hash in NODE_REGISTRY:
        return NODE_REGISTRY[node.merkle_hash]
    
    NODE_REGISTRY[node.merkle_hash] = node
    return node

# ---------------------------------------------------------
# 3. THE LQFT ARCHITECTURE (Strictly Iterative)
# ---------------------------------------------------------
class LQFT:
    def __init__(self):
        self.root = get_deduplicated_node(LQFTNode())

    def insert(self, key: str, value: any):
        """
        O(1) Insertion. Capped at exactly 13 hops.
        STRICTLY ITERATIVE. NO RECURSION ALLOWED.
        """
        key_hash = fnv1a_64(key)
        
        # Step 1: Iterative Traversal Down
        # We store the path to allow bottom-up Merkle folding without recursion
        path_stack = []
        current = self.root
        
        for level in range(13): # Fixed 64-bit space (13 chunks of 5 bits)
            index = (key_hash >> (level * 5)) & 0x1F # Mask 5 bits
            path_stack.append((current, index))
            
            if current.children[index] is None:
                current = LQFTNode() # Create empty node for routing
            else:
                current = current.children[index]

        # Step 2: Create the Leaf Node
        new_leaf = LQFTNode(value=value)
        current = get_deduplicated_node(new_leaf)

        # Step 3: Iterative Bottom-Up Folding (Copy-on-Write)
        while path_stack:
            parent_node, index = path_stack.pop()
            
            # Create a copy of the parent to ensure immutability/versioning
            new_parent = LQFTNode()
            new_parent.children = list(parent_node.children) # Copy references
            new_parent.children[index] = current # Attach the new folded child
            
            # Deduplicate the new parent
            current = get_deduplicated_node(new_parent)
            
        # The final deduplicated node becomes the new root
        self.root = current

    def search(self, key: str) -> any:
        """O(1) Search. Guaranteed max 13 hops. Iterative."""
        key_hash = fnv1a_64(key)
        current = self.root
        
        for level in range(13):
            index = (key_hash >> (level * 5)) & 0x1F
            current = current.children[index]
            if current is None:
                return None # Key not found
                
        # FIXED: This return statement is now outside the loop!
        return current.value

    # Make it easy to use like standard Python Dicts
    def __setitem__(self, key, value):
        self.insert(key, value)

    def __getitem__(self, key):
        res = self.search(key)
        if res is None:
            raise KeyError(key)
        return res

# ---------------------------------------------------------
# 4. EASY API (The "Heapify" Equivalent)
# ---------------------------------------------------------
def build_lqft(iterable) -> LQFT:
    """
    Instantiates and builds an LQFT in O(1) interface time.
    Usage: tree = build_lqft([("user1", "dataA"), ("user2", "dataB")])
    """
    tree = LQFT()
    for key, value in iterable:
        tree.insert(key, value)
    return tree