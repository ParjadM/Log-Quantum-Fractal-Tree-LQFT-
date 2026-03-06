import os
import sys
import hashlib
import threading

try:
    import psutil
except Exception:
    psutil = None

# ---------------------------------------------------------
# STRICT NATIVE ENTERPRISE WRAPPER (v1.0.5)
# ---------------------------------------------------------
# Architect: Parjad Minooei
# Target: McMaster B.Tech / UofT MScAC Portfolio

try:
    import lqft_c_engine
except ImportError:
    print("\n[!] CRITICAL FATAL ERROR: Native C-Engine not found.")
    print("[!] The LQFT is now a strictly native database. Pure Python fallback is disabled.")
    print("[!] Run: python setup.py build_ext --inplace\n")
    sys.exit(1)

class LQFT:
    _instance_lock = threading.Lock()
    _live_instances = 0

    # F-03 & F-04: Restored migration_threshold to sync API signatures across the suite
    def __init__(self, migration_threshold=50000):
        self.is_native = True
        # Keep destructive purge opt-in; global C-engine state can be shared by multiple wrappers.
        self.auto_purge_enabled = False
        self.max_memory_mb = 1000.0
        self.total_ops = 0
        self.migration_threshold = migration_threshold
        self._process = psutil.Process(os.getpid()) if psutil else None
        self._closed = False
        with LQFT._instance_lock:
            LQFT._live_instances += 1

    def _validate_type(self, key, value=None):
        if not isinstance(key, str):
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")

    def _get_64bit_hash(self, key):
        # Deterministic 64-bit hash keeps key mapping stable across processes/runs.
        return int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest(), "little")

    def _current_memory_mb(self):
        if self._process is None:
            return 0.0
        try:
            return self._process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def set_auto_purge_threshold(self, threshold: float):
        self.max_memory_mb = threshold

    def purge(self):
        current_mb = self._current_memory_mb()
        with LQFT._instance_lock:
            live = LQFT._live_instances
        if live > 1:
            print(
                f"\n[⚠️ CIRCUIT Breaker] Memory {current_mb:.1f} MB but purge skipped "
                f"because {live} LQFT instances are active (shared global engine state)."
            )
            return
        print(f"\n[⚠️ CIRCUIT Breaker] Engine exceeded limit (Currently {current_mb:.1f} MB). Auto-Purging!")
        self.clear()

    def get_stats(self):
        return lqft_c_engine.get_metrics()

    # F-02: Standardized Metric Mapping (Dunder Method)
    def __len__(self):
        """Allows native Python len() to fetch logical_inserts from the C-Engine."""
        stats = self.get_stats()
        # Maps directly to the sharded hardware counters in the C-kernel
        return stats.get('logical_inserts', 0)

    def clear(self):
        # Global clear (shared native state). Keep explicit to avoid accidental data loss.
        return lqft_c_engine.free_all()

    def insert(self, key, value):
        self._validate_type(key, value)
        self.total_ops += 1
        
        # Heuristic Circuit Breaker check
        if self.auto_purge_enabled and self.total_ops % 5000 == 0:
            current_mb = self._current_memory_mb()
            if current_mb >= self.max_memory_mb:
                self.purge()

        h = self._get_64bit_hash(key)
        lqft_c_engine.insert(h, value)

    def search(self, key):
        self._validate_type(key)
        h = self._get_64bit_hash(key)
        return lqft_c_engine.search(h)

    def remove(self, key):
        self._validate_type(key)
        h = self._get_64bit_hash(key)
        if hasattr(lqft_c_engine, 'delete'):
            lqft_c_engine.delete(h)

    def delete(self, key):
        self.remove(key)

    def __setitem__(self, key, value):
        self.insert(key, value)

    def __getitem__(self, key):
        res = self.search(key)
        if res is None:
            raise KeyError(key)
        return res

    def __delitem__(self, key):
        self.delete(key)

    def __del__(self):
        try:
            if not self._closed:
                with LQFT._instance_lock:
                    LQFT._live_instances = max(0, LQFT._live_instances - 1)
                self._closed = True
        except Exception:
            pass

    def status(self):
        return {
            "mode": "Strict Native C-Engine (Arena Allocator)",
            "items": lqft_c_engine.get_metrics().get('physical_nodes', 0),
            "threshold": f"{self.max_memory_mb} MB Circuit Breaker"
        }

# Retain AdaptiveLQFT alias to support legacy benchmark scripts gracefully
AdaptiveLQFT = LQFT