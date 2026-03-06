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
    __slots__ = (
        "is_native",
        "auto_purge_enabled",
        "max_memory_mb",
        "total_ops",
        "migration_threshold",
        "_process",
        "_closed",
        "_native_insert_kv",
        "_native_search_key",
        "_native_delete_key",
        "_native_contains_key",
        "_native_bulk_insert_keys",
        "_native_bulk_contains_count",
        "_native_bulk_insert_range",
        "_native_bulk_contains_range_count",
    )

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
        self._native_insert_kv = getattr(lqft_c_engine, "insert_key_value", None)
        self._native_search_key = getattr(lqft_c_engine, "search_key", None)
        self._native_delete_key = getattr(lqft_c_engine, "delete_key", None)
        self._native_contains_key = getattr(lqft_c_engine, "contains_key", None)
        self._native_bulk_insert_keys = getattr(lqft_c_engine, "bulk_insert_keys", None)
        self._native_bulk_contains_count = getattr(lqft_c_engine, "bulk_contains_count", None)
        self._native_bulk_insert_range = getattr(lqft_c_engine, "bulk_insert_range", None)
        self._native_bulk_contains_range_count = getattr(lqft_c_engine, "bulk_contains_range_count", None)
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
            # Fallback for environments where psutil binary wheels are unavailable.
            if os.name == "nt":
                try:
                    import ctypes
                    from ctypes import wintypes

                    class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                        _fields_ = [
                            ("cb", wintypes.DWORD),
                            ("PageFaultCount", wintypes.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t),
                            ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                            ("PagefileUsage", ctypes.c_size_t),
                            ("PeakPagefileUsage", ctypes.c_size_t),
                        ]

                    counters = PROCESS_MEMORY_COUNTERS()
                    counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                    handle = ctypes.windll.kernel32.GetCurrentProcess()
                    get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
                    get_process_memory_info.argtypes = [
                        wintypes.HANDLE,
                        ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
                        wintypes.DWORD,
                    ]
                    get_process_memory_info.restype = wintypes.BOOL
                    ok = get_process_memory_info(
                        handle,
                        ctypes.byref(counters),
                        counters.cb,
                    )
                    if ok:
                        return counters.WorkingSetSize / (1024 * 1024)
                except Exception:
                    return 0.0
            return 0.0
        try:
            return self._process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def set_auto_purge_threshold(self, threshold: float):
        threshold = float(threshold)
        if threshold <= 0:
            raise ValueError("Auto-purge threshold must be > 0 MB.")
        self.max_memory_mb = threshold
        self.auto_purge_enabled = True

    def disable_auto_purge(self):
        self.auto_purge_enabled = False

    def purge(self):
        current_mb = self._current_memory_mb()
        with LQFT._instance_lock:
            live = LQFT._live_instances
        if live > 1:
            print(
                f"\n[WARN CIRCUIT Breaker] Memory {current_mb:.1f} MB but purge skipped "
                f"because {live} LQFT instances are active (shared global engine state)."
            )
            return
        print(f"\n[WARN CIRCUIT Breaker] Engine exceeded limit (Currently {current_mb:.1f} MB). Auto-Purging!")
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
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        
        # Heuristic Circuit Breaker check
        if self.auto_purge_enabled:
            self.total_ops += 1
            if self.total_ops % 5000 == 0:
                current_mb = self._current_memory_mb()
                if current_mb >= self.max_memory_mb:
                    self.purge()

        if self._native_insert_kv is not None:
            self._native_insert_kv(key, value)
            return

        h = self._get_64bit_hash(key)
        lqft_c_engine.insert(h, value)

    def search(self, key):
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if self._native_search_key is not None:
            return self._native_search_key(key)

        h = self._get_64bit_hash(key)
        return lqft_c_engine.search(h)

    def remove(self, key):
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if self._native_delete_key is not None:
            self._native_delete_key(key)
            return

        h = self._get_64bit_hash(key)
        if hasattr(lqft_c_engine, 'delete'):
            lqft_c_engine.delete(h)

    def delete(self, key):
        self.remove(key)

    def contains(self, key):
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if self._native_contains_key is not None:
            return bool(self._native_contains_key(key))
        return self.search(key) is not None

    def bulk_insert(self, keys, value):
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        if self._native_bulk_insert_keys is not None:
            self._native_bulk_insert_keys(keys, value)
            return
        for key in keys:
            self.insert(key, value)

    def bulk_contains_count(self, keys):
        if self._native_bulk_contains_count is not None:
            return int(self._native_bulk_contains_count(keys))
        count = 0
        for key in keys:
            if self.contains(key):
                count += 1
        return count

    def bulk_insert_range(self, prefix, start, count, value):
        if type(prefix) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(prefix).__name__}")
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        if self._native_bulk_insert_range is not None:
            self._native_bulk_insert_range(prefix, int(start), int(count), value)
            return
        end = int(start) + int(count)
        for i in range(int(start), end):
            self.insert(f"{prefix}{i}", value)

    def bulk_contains_range_count(self, prefix, start, count):
        if type(prefix) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(prefix).__name__}")
        if self._native_bulk_contains_range_count is not None:
            return int(self._native_bulk_contains_range_count(prefix, int(start), int(count)))
        hit = 0
        end = int(start) + int(count)
        for i in range(int(start), end):
            if self.contains(f"{prefix}{i}"):
                hit += 1
        return hit

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
            "threshold": f"{self.max_memory_mb} MB Circuit Breaker",
            "auto_purge_enabled": self.auto_purge_enabled,
        }

# Retain AdaptiveLQFT alias to support legacy benchmark scripts gracefully
AdaptiveLQFT = LQFT