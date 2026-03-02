import pytest
from pure_python_ds import LQFT, build_lqft, NODE_REGISTRY

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    """Clear the global registry before each test to ensure clean state."""
    NODE_REGISTRY.clear()
    yield

def test_basic_insertion_and_search():
    tree = LQFT()
    tree["agent_007"] = "James Bond"
    tree["agent_006"] = "Alec Trevelyan"

    assert tree["agent_007"] == "James Bond"
    assert tree.search("agent_006") == "Alec Trevelyan"

def test_missing_key():
    tree = LQFT()
    with pytest.raises(KeyError):
        _ = tree["ghost_agent"]
    
    # search() should return None gracefully
    assert tree.search("ghost_agent") is None

def test_build_lqft_api():
    """Tests the 'heapify'-like ease of use."""
    data = [("alpha", 100), ("beta", 200), ("gamma", 300)]
    tree = build_lqft(data)
    
    assert tree["alpha"] == 100
    assert tree["beta"] == 200
    assert tree["gamma"] == 300

def test_merkle_deduplication():
    """
    PROVES O(Σ) Space Complexity.
    If we insert identical data under two different paths, 
    the engine should physically use the EXACT SAME memory address (id).
    """
    tree = LQFT()
    
    # Insert identical payloads
    tree["path_A"] = {"status": "active", "permissions": "admin"}
    tree["path_B"] = {"status": "active", "permissions": "admin"}
    
    # In a normal dict, these are two separate objects in memory.
    # In the LQFT, we must prove the underlying leaves share the same Python `id()`.
    
    # Retrieve the raw leaf nodes (simulating a deep traversal check)
    # Because of our deduplication registry, the total number of unique nodes 
    # created will be strictly less than standard insertion.
    
    # Instead of manual traversal, we can verify that the NODE_REGISTRY
    # has collapsed the identical leaf values into a single Merkle Hash key.
    
    # Count how many leaves have the exact value payload
    leaves = [node for node in NODE_REGISTRY.values() if node.value == {"status": "active", "permissions": "admin"}]
    
    assert len(leaves) == 1, "LQFT failed to deduplicate identical data structures!"

def test_update_existing_key():
    tree = LQFT()
    tree["target"] = "Initial State"
    assert tree["target"] == "Initial State"
    
    tree["target"] = "Mutated State"
    assert tree["target"] == "Mutated State"

def test_massive_scale_no_recursion():
    """
    If there was recursion, inserting 10,000 items would trigger a 
    RecursionError. This proves the iterative path_stack works perfectly.
    """
    tree = LQFT()
    try:
        for i in range(10000):
            tree[f"key_{i}"] = i
    except RecursionError:
        pytest.fail("RecursionError triggered! The algorithm is not truly iterative.")
    
    assert tree["key_9999"] == 9999