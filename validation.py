import sys
import hashlib
import os

def fast_hash(key):
    return int(hashlib.md5(str(key).encode()).hexdigest()[:16], 16)

try:
    import lqft_c_engine
    h = fast_hash("ci_verification_v091")
    lqft_c_engine.insert(h, "stable_core_v091")
    result = lqft_c_engine.search(h)
    assert result == "stable_core_v091", "Data integrity failure in Arena!"
    
    # Verify Deletion Fast-Path
    lqft_c_engine.delete(h)
    assert lqft_c_engine.search(h) is None, "Arena deletion failure!"
    
    lqft_c_engine.free_all()
    print("[*] CI/CD Pipeline PASS (v0.9.1 Core Verified).")
except Exception as e:
    print(f"[!] CI/CD Error: {e}")
    sys.exit(1)
