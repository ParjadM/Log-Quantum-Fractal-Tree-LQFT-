import hashlib
import psutil
import os
import sys

# ---------------------------------------------------------
# STRICT NATIVE ENTERPRISE ENGINE (v0.7.0)
# ---------------------------------------------------------
# Architect: Parjad Minooei
# Status: Pure Python fallback removed. Strict C-Core interface.

try:
    import lqft_c_engine
except ImportError:
    print("\n[!] CRITICAL FATAL ERROR: Native C-Engine not found.")
    print("[!] The LQFT is now a strictly native database. Pure Python fallback is disabled.")
    print("[!] Run: python setup.py build_ext --inplace\n")
    sys.exit(1)

class LQFT:
    def __init__(self):
        self.is_native = True
        self.auto_purge_enabled = True
        self.max_memory_mb = 1000.0 
        self.total_ops = 0
        self._process = psutil.Process(os.getpid())

    def _validate_type(self, key, value=None):
        if not isinstance(key, str):
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")

    def _get_64bit_hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest()[:16], 16)

    def set_auto_purge_threshold(self, threshold: float):
        self.max_memory_mb = threshold

    def purge(self):
        current_mb = self._process.memory_info().rss / (1024 * 1024)
        print(f"\n[⚠️ CIRCUIT Breaker] Engine exceeded limit (Currently {current_mb:.1f} MB). Auto-Purging!")
        self.clear()

    # --- Native Disk Persistence ---
    def save_to_disk(self, filepath: str):
        lqft_c_engine.save_to_disk(filepath)

    def load_from_disk(self, filepath: str):
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Missing LQFT database file: {filepath}")
        lqft_c_engine.load_from_disk(filepath)

    # --- Core Operations ---
    def insert(self, key, value):
        self._validate_type(key, value)
        self.total_ops += 1
        
        # Memory Circuit Breaker
        if self.auto_purge_enabled and self.total_ops % 5000 == 0:
            current_mb = self._process.memory_info().rss / (1024 * 1024)
            if current_mb >= self.max_memory_mb:
                self.purge()

        h = self._get_64bit_hash(key)
        lqft_c_engine.insert(h, value)

    def remove(self, key):
        self._validate_type(key)
        h = self._get_64bit_hash(key)
        lqft_c_engine.delete(h)

    def delete(self, key):
        self.remove(key)

    def search(self, key):
        self._validate_type(key)
        h = self._get_64bit_hash(key)
        return lqft_c_engine.search(h)

    # --- Pythonic Syntactic Sugar ---
    def __setitem__(self, key, value):
        self.insert(key, value)

    def __getitem__(self, key):
        res = self.search(key)
        if res is None:
            raise KeyError(key)
        return res

    def clear(self):
        return lqft_c_engine.free_all()

    def get_stats(self):
        return lqft_c_engine.get_metrics()

    def __del__(self):
        try: self.clear()
        except: pass

    def status(self):
        return {
            "mode": "Strict Native C-Engine",
            "items": lqft_c_engine.get_metrics().get('physical_nodes', 0),
            "threshold": "DISABLED (Pure Hardware Mode)"
        }

# Alias mapping so older benchmark scripts don't crash
AdaptiveLQFT = LQFT