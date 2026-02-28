import os
import sys
import time

# ---------------------------------------------------------
# PHASE 1: THE PHYSICAL BUILD (Systems Level)
# ---------------------------------------------------------
def build_engine():
    """
    Ensures the C-Core is compiled and the shared library (.pyd/.so) 
    is present in the local directory.
    """
    if not os.path.exists("lqft_c_engine.pyd") and not os.path.exists("lqft_c_engine.so"):
        print("[*] Initializing Physical Build: Compiling C-Engine...")
        # Systems command to run the setup script we built earlier
        os.system(f"{sys.executable} setup.py build_ext --inplace")
        print("[+] Build Complete.")
    else:
        print("[+] Native C-Engine detected.")

# ---------------------------------------------------------
# PHASE 2: RUNTIME INSTANTIATION (Application Level)
# ---------------------------------------------------------
def initialize_runtime():
    """
    Imports the native bindings and initializes the high-level manager.
    """
    try:
        from lqft_engine import AdaptiveLQFT
        import lqft_c_engine
        
        # 1. State Verification
        # In a long-running persistent server, we would expose a .clear() 
        # method from C to wipe the registry. Since this is a fresh process bootup, 
        # the OS already guarantees a completely zeroed-out memory state.
        
        # 2. Instantiate the Manager
        # migration_threshold=1: Forces immediate folding for maximum RAM savings.
        # migration_threshold=100: Prioritizes CPU speed, folding only after 100 entries.
        engine = AdaptiveLQFT(migration_threshold=1)
        
        print("[+] LQFT Runtime Initialized Successfully.")
        return engine, lqft_c_engine
        
    except ImportError as e:
        print(f"[!] Initialization Failed: {e}")
        return None, None

# ---------------------------------------------------------
# BOOTUP SEQUENCE
# ---------------------------------------------------------
if __name__ == "__main__":
    print("="*60)
    print(" 🚀 LQFT SYSTEMS ARCHITECT: BOOTUP SEQUENCE")
    print("="*60)
    
    # Step 1: Physical Check
    build_engine()
    
    # Step 2: Runtime Setup
    lqft, c_core = initialize_runtime()
    
    if lqft:
        # Step 3: FFI Cold-Start Warmup
        # The first call to a C-Extension forces the OS to page the DLL into memory.
        # We run a dummy insert to absorb this ~40ms overhead.
        c_core.insert(0, "WARMUP_VECTOR")
        
        # Step 4: True Performance Metric (High-Precision Hardware Timer)
        test_payload = "INITIALIZATION_VECTOR_770"
        test_hash = abs(hash(test_payload)) & 0xFFFFFFFFFFFFFFFF
        
        # We switch to perf_counter_ns() to catch nanosecond-level C-execution speeds
        # because the engine is now too fast for standard time.time() resolution.
        start = time.perf_counter_ns()
        c_core.insert(test_hash, test_payload)
        latency_us = (time.perf_counter_ns() - start) / 1000.0
        
        print(f"[*] Post-Init Test: {test_payload}")
        print(f"[*] Insertion Latency: {latency_us:.3f} μs (Warm Cache, High-Res Clock)")
        print(f"[*] Active C-Nodes: {c_core.get_metrics()['physical_nodes']}")
        print("="*60)
        print(" ENGINE READY FOR PRODUCTION")
    else:
        print("[!] Fatal Error: Check your C-compiler paths (MinGW/GCC).")