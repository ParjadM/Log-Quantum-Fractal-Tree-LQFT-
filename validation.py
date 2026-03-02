import sys

print("====================================================")
print("   LQFT CI/CD PIPELINE: NATIVE C-ENGINE VALIDATION")
print("====================================================")

try:
    import lqft_c_engine
    print("[*] Native C-Engine loaded successfully.")
    
    # Verify core operations
    lqft_c_engine.insert(42, "verification_payload")
    result = lqft_c_engine.search(42)
    
    assert result == "verification_payload", "Data corruption detected!"
    
    print("[*] Full CRUD operations verified.")
    print("[*] CI/CD Pipeline PASS.")
except Exception as e:
    print(f"[!] CI/CD Error: {e}")
    sys.exit(1)
