import hashlib
import weakref
import psutil
import os
import json

# ---------------------------------------------------------
# LEGACY PURE PYTHON LQFT (For reference/fallback)
# ---------------------------------------------------------
class LQFTNode:
    __slots__ = ['children', 'value', 'key_hash', 'struct_hash', '__weakref__']
    _registry = weakref.WeakValueDictionary()
    _null_cache = {}

    def __init__(self, value=None, children=None, key_hash=None):
        self.value = value
        self.key_hash = key_hash
        self.children = children or {}
        self.struct_hash = self._calculate_struct_hash()

    def _calculate_struct_hash(self):
        child_sigs = tuple(sorted([(k, v.struct_hash) for k, v in self.children.items()]))
        k_identity = str(self.key_hash) if self.key_hash is not None else ""
        data = f"v:{self.value}|k:{k_identity}|c:{child_sigs}".encode()
        return hashlib.md5(data).hexdigest()

    @classmethod
    def get_canonical(cls, value, children, key_hash=None):
        if children == {}: children = None
        child_sigs = tuple(sorted([(k, v.struct_hash) for k, v in (children or {}).items()]))
        k_identity = str(key_hash) if key_hash is not None else ""
        lookup_hash = hashlib.md5(f"v:{value}|k:{k_identity}|c:{child_sigs}".encode()).hexdigest()
        if lookup_hash in cls._registry: return cls._registry[lookup_hash]
        new_node = cls(value, children, key_hash)
        cls._registry[lookup_hash] = new_node
        return new_node

    @classmethod
    def get_null(cls):
        if 'null' not in cls._null_cache:
            cls._null_cache['null'] = cls.get_canonical(None, None, None)
        return cls._null_cache['null']

class LQFT:
    def __init__(self, bit_partition=5, max_bits=256):
        self.partition = bit_partition 
        self.max_bits = max_bits 
        self.mask = (1 << bit_partition) - 1
        self.root = LQFTNode.get_null()

    def _get_hash(self, key):
        return int(hashlib.sha256(str(key).encode()).hexdigest(), 16)

    def insert(self, key, value):
        h = self._get_hash(key)
        null_node = LQFTNode.get_null()
        path, curr, bit_depth = [], self.root, 0
        
        while curr is not null_node and curr.value is None:
            segment = (h >> bit_depth) & self.mask
            path.append((curr, segment))
            if segment not in curr.children:
                curr = null_node
                break
            curr = curr.children[segment]
            bit_depth += self.partition
            
        new_sub_node = None
        if curr is null_node:
            new_sub_node = LQFTNode.get_canonical(value, None, h)
        elif curr.key_hash == h:
            new_sub_node = LQFTNode.get_canonical(value, curr.children, h)
        else:
            old_h, old_val, temp_depth = curr.key_hash, curr.value, bit_depth
            while temp_depth < self.max_bits:
                s_old, s_new = (old_h >> temp_depth) & self.mask, (h >> temp_depth) & self.mask
                if s_old != s_new:
                    c_old = LQFTNode.get_canonical(old_val, None, old_h)
                    c_new = LQFTNode.get_canonical(value, None, h)
                    new_sub_node = LQFTNode.get_canonical(None, {s_old: c_old, s_new: c_new}, None)
                    break
                else:
                    path.append(("split", s_old))
                    temp_depth += self.partition
            if new_sub_node is None:
                new_sub_node = LQFTNode.get_canonical(value, curr.children, h)
                
        for entry in reversed(path):
            if entry[0] == "split":
                new_sub_node = LQFTNode.get_canonical(None, {entry[1]: new_sub_node}, None)
            else:
                p_node, segment = entry
                new_children = dict(p_node.children)
                new_children[segment] = new_sub_node
                new_sub_node = LQFTNode.get_canonical(p_node.value, new_children, p_node.key_hash)
        self.root = new_sub_node

    def search(self, key):
        h, curr, null_node, bit_depth = self._get_hash(key), self.root, LQFTNode.get_null(), 0
        while curr is not null_node:
            if curr.value is not None: return curr.value if curr.key_hash == h else None
            segment = (h >> bit_depth) & self.mask
            if segment not in curr.children: return None
            curr, bit_depth = curr.children[segment], bit_depth + self.partition
            if bit_depth >= self.max_bits: break
        return None

# ---------------------------------------------------------
# ADAPTIVE ENTERPRISE ENGINE (v0.5.0 - Disk Persistence)
# ---------------------------------------------------------
try:
    import lqft_c_engine
    C_ENGINE_READY = True
except ImportError:
    C_ENGINE_READY = False

class AdaptiveLQFT:
    def __init__(self, migration_threshold=50000):
        self.threshold = migration_threshold
        self.size = 0
        self.is_native = False
        self._light_store = {} 
        
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
        self.purge_threshold = threshold

    def purge(self):
        current_mb = self._process.memory_info().rss / (1024 * 1024)
        if current_mb >= self.max_memory_mb:
            print(f"\n[⚠️ CIRCUIT Breaker] Engine exceeded {self.max_memory_mb} MB limit (Currently {current_mb:.1f} MB). Auto-Purging!")
        self.clear()

    # --- Phase 1: Native Disk Persistence ---
    def save_to_disk(self, filepath: str):
        """Serializes the engine state to disk (O(1) Time context scaling)."""
        if not self.is_native:
            with open(filepath, 'w') as f:
                json.dump(self._light_store, f)
        else:
            lqft_c_engine.save_to_disk(filepath)

    def load_from_disk(self, filepath: str):
        """Instantly reconstructs C-Pointers from a binary file."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Missing LQFT database file: {filepath}")
            
        if not self.is_native:
            # Check if it's a C-Engine binary file (magic bytes "LQFT")
            with open(filepath, 'rb') as f:
                if f.read(4) == b'LQFT':
                    self._migrate_to_native()
                    lqft_c_engine.load_from_disk(filepath)
                    return
            with open(filepath, 'r') as f:
                self._light_store = json.load(f)
                self.size = len(self._light_store)
        else:
            lqft_c_engine.load_from_disk(filepath)

    def _migrate_to_native(self):
        if not C_ENGINE_READY:
            self.threshold = float('inf') 
            return
        for key, val in self._light_store.items():
            h = self._get_64bit_hash(key)
            lqft_c_engine.insert(h, val)
        self._light_store.clear()
        self.is_native = True

    def insert(self, key, value):
        self._validate_type(key, value)
        self.total_ops += 1
        if self.auto_purge_enabled and self.total_ops % 5000 == 0:
            current_mb = self._process.memory_info().rss / (1024 * 1024)
            if current_mb >= self.max_memory_mb:
                self.purge()

        if not self.is_native:
            if key not in self._light_store: 
                self.size += 1
            self._light_store[key] = value
            if self.size >= self.threshold: 
                self._migrate_to_native()
        else:
            h = self._get_64bit_hash(key)
            lqft_c_engine.insert(h, value)

    def remove(self, key):
        self._validate_type(key)
        if not self.is_native:
            if key in self._light_store:
                del self._light_store[key]
                self.size -= 1
        else:
            h = self._get_64bit_hash(key)
            lqft_c_engine.delete(h)

    def delete(self, key):
        self.remove(key)

    def search(self, key):
        self._validate_type(key)
        if not self.is_native: 
            return self._light_store.get(key, None)
        else:
            h = self._get_64bit_hash(key)
            return lqft_c_engine.search(h)

    def clear(self):
        self._light_store.clear()
        self.size = 0
        if C_ENGINE_READY: 
            return lqft_c_engine.free_all()
        return 0

    def get_stats(self):
        if self.is_native and C_ENGINE_READY: 
            return lqft_c_engine.get_metrics()
        return {"physical_nodes": 0}

    def __del__(self):
        try: self.clear()
        except: pass

    def status(self):
        return {
            "mode": "Native Merkle-DAG" if self.is_native else "Lightweight C-Hash",
            "items": self.size if not self.is_native else lqft_c_engine.get_metrics().get('physical_nodes', self.size),
            "threshold": self.threshold
        }