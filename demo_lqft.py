from pure_python_ds import LQFT, build_lqft

print("--- 🌳 Starting LQFT Simple Demo ---\n")

# 1. Build the tree instantly with initial data (like heapify!)
# Time Complexity: O(K) where K is the number of initial items. 
# (It performs K individual O(1) insertions)
initial_data = [
    ("player_1", {"name": "Parjad", "level": 99, "class": "Architect"}),
    ("player_2", {"name": "Cloud", "level": 45, "class": "Warrior"})
]
db = build_lqft(initial_data)

# 2. Retrieve data in O(1) time
# Time Complexity: O(1) Worst-Case (Capped at exactly 13 pointer hops)
print("Fetching player_1:")
print(db["player_1"])

# 3. Add new data using standard dictionary syntax
# Time Complexity: O(1) Worst-Case (Iterative CoW traversal, capped at 13 hops)
print("\nAdding player_3...")
db["player_3"] = {"name": "Aerith", "level": 42, "class": "Mage"}
print(db["player_3"])

# 4. Update existing data (Mutating state instantly)
# Time Complexity: O(1) Worst-Case (Path overwriting with Merkle deduplication)
print("\nLeveling up player_1...")
db["player_1"] = {"name": "Parjad", "level": 100, "class": "Systems Architect"}
print(db["player_1"])

# 5. Handle missing keys gracefully using the .search() method
# Time Complexity: O(1) Worst-Case (Fails fast if a 5-bit routing branch is empty)
print("\nTrying to find a missing player...")
result = db.search("player_99")
print(f"Result for player_99: {result} (Handled without crashing!)")

print("\n--- ✅ Demo Complete ---")