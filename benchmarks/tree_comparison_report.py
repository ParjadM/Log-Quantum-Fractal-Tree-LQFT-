import argparse
import bisect
import os
import random
import statistics
import sys
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lqft_engine import LQFT

try:
    from pure_python_ds import LQFT as LQFTLight
except Exception:
    LQFTLight = None


class SortedArrayTree:
    """Balanced-tree-like lookup via binary search over sorted unique keys."""

    def __init__(self, keys):
        self.keys = sorted(set(keys))

    def contains(self, key):
        idx = bisect.bisect_left(self.keys, key)
        return idx < len(self.keys) and self.keys[idx] == key


class Trie:
    _END = "\0"

    def __init__(self):
        self.root = {}

    def insert(self, word):
        node = self.root
        for ch in word:
            nxt = node.get(ch)
            if nxt is None:
                nxt = {}
                node[ch] = nxt
            node = nxt
        node[self._END] = True

    def contains(self, word):
        node = self.root
        for ch in word:
            node = node.get(ch)
            if node is None:
                return False
        return self._END in node


class SkipListNode:
    __slots__ = ("key", "forward")

    def __init__(self, key, level):
        self.key = key
        self.forward = [None] * (level + 1)


class SkipList:
    """Compact skip-list tuned for read-heavy benchmark usage."""

    def __init__(self, max_level=16, p=0.5):
        self.max_level = max_level
        self.p = p
        self.level = 0
        self.header = SkipListNode("", max_level)

    def _random_level(self):
        lvl = 0
        while random.random() < self.p and lvl < self.max_level:
            lvl += 1
        return lvl

    def insert(self, key):
        update = [None] * (self.max_level + 1)
        curr = self.header

        for i in range(self.level, -1, -1):
            while curr.forward[i] is not None and curr.forward[i].key < key:
                curr = curr.forward[i]
            update[i] = curr

        curr = curr.forward[0]
        if curr is not None and curr.key == key:
            return

        node_level = self._random_level()
        if node_level > self.level:
            for i in range(self.level + 1, node_level + 1):
                update[i] = self.header
            self.level = node_level

        new_node = SkipListNode(key, node_level)
        for i in range(node_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

    def contains(self, key):
        curr = self.header
        for i in range(self.level, -1, -1):
            while curr.forward[i] is not None and curr.forward[i].key < key:
                curr = curr.forward[i]
        curr = curr.forward[0]
        return curr is not None and curr.key == key


class BTreeNode:
    __slots__ = ("keys", "children")

    def __init__(self, keys, children):
        self.keys = keys
        self.children = children


class StaticBTree:
    """Immutable B-tree-like index built from sorted keys for lookup benchmarking."""

    def __init__(self, keys, fanout=32):
        if fanout < 4:
            raise ValueError("fanout must be >= 4")

        uniq = sorted(set(keys))
        leaf_cap = fanout - 1

        level = []
        for i in range(0, len(uniq), leaf_cap):
            level.append(BTreeNode(uniq[i : i + leaf_cap], None))

        if not level:
            level = [BTreeNode([], None)]

        while len(level) > 1:
            next_level = []
            for i in range(0, len(level), fanout):
                children = level[i : i + fanout]
                separators = [child.keys[-1] for child in children[:-1] if child.keys]
                next_level.append(BTreeNode(separators, children))
            level = next_level

        self.root = level[0]

    def contains(self, key):
        node = self.root
        while node.children is not None:
            idx = bisect.bisect_right(node.keys, key)
            node = node.children[idx]
        idx = bisect.bisect_left(node.keys, key)
        return idx < len(node.keys) and node.keys[idx] == key


def build_workload(n, seed):
    random.seed(seed)
    keys = [f"k{i:08x}" for i in range(n)]
    random.shuffle(keys)
    return keys


def build_queries(keys, total_ops, hit_ratio, seed):
    random.seed(seed)
    n_hits = int(total_ops * hit_ratio)
    n_misses = total_ops - n_hits

    hits = [random.choice(keys) for _ in range(n_hits)]
    misses = [f"z{i:08x}" for i in range(n_misses)]
    queries = hits + misses
    random.shuffle(queries)
    return queries


def benchmark_contains(name, contains_fn, queries, threads):
    warmup_n = min(len(queries), max(1000, len(queries) // 10))
    for k in queries[:warmup_n]:
        _ = contains_fn(k)

    chunk_size = max(1, len(queries) // threads)
    chunks = [queries[i : i + chunk_size] for i in range(0, len(queries), chunk_size)]

    def worker(chunk):
        hit = 0
        for k in chunk:
            if contains_fn(k):
                hit += 1
        return hit

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        _ = list(pool.map(worker, chunks))
    elapsed = time.perf_counter() - start
    ops_s = len(queries) / elapsed if elapsed > 0 else 0.0
    return name, ops_s


def benchmark_lqft_bulk(name, lqft, queries, threads):
    warmup_n = min(len(queries), max(1000, len(queries) // 10))
    _ = lqft.bulk_contains_count(queries[:warmup_n])

    chunk_size = max(1, len(queries) // threads)
    chunks = [queries[i : i + chunk_size] for i in range(0, len(queries), chunk_size)]

    def worker(chunk):
        return lqft.bulk_contains_count(chunk)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        _ = list(pool.map(worker, chunks))
    elapsed = time.perf_counter() - start
    ops_s = len(queries) / elapsed if elapsed > 0 else 0.0
    return name, ops_s


def benchmark_contains_single(name, contains_fn, queries, show_elapsed=False):
    warmup_n = min(len(queries), max(1000, len(queries) // 10))
    for key in queries[:warmup_n]:
        _ = contains_fn(key)

    start = time.perf_counter()
    for key in queries:
        _ = contains_fn(key)
    elapsed = time.perf_counter() - start
    ops_s = len(queries) / elapsed if elapsed > 0 else 0.0
    return name, ops_s, elapsed if show_elapsed else None


def run_stress(args):
    keys = build_workload(args.n, args.seed)
    queries = build_queries(keys, args.ops, args.hit_ratio, args.seed + 1)

    py_dict = {k: "v" for k in keys}
    ordered = OrderedDict((k, "v") for k in keys)

    lqft_classic = LQFT()
    lqft_classic.clear()
    lqft_classic.set_write_batch_size(args.lqft_batch)
    lqft_classic.bulk_insert(keys, "v")

    lqft_native = LQFT()
    lqft_native.set_write_batch_size(args.lqft_batch)
    lqft_native.bulk_insert(keys, "v")
    lqft_native.seal_reads()

    lqft_light = None
    if LQFTLight is not None:
        lqft_light = LQFTLight()
        for key in keys:
            lqft_light.insert(key, "v")

    def one_pass_results():
        rows = []
        rows.append(benchmark_contains_single("Python dict", py_dict.__contains__, queries))
        rows.append(benchmark_contains_single("OrderedDict", ordered.__contains__, queries))
        rows.append(benchmark_contains_single("LQFT Classic", lqft_classic.contains, queries, show_elapsed=True))
        if lqft_light is not None:
            rows.append(benchmark_contains_single("LQFT Light", lambda key: lqft_light.search(key) is not None, queries, show_elapsed=True))
        rows.append(benchmark_contains_single("LQFT Native", lqft_native.contains, queries, show_elapsed=True))
        return rows

    per_trial = [one_pass_results() for _ in range(max(1, args.trials))]
    labels = [name for name, _, _ in per_trial[0]]
    results = []
    for index, label in enumerate(labels):
        ops_vals = [trial[index][1] for trial in per_trial]
        elapsed_vals = [trial[index][2] for trial in per_trial if trial[index][2] is not None]
        median_ops = statistics.median(ops_vals)
        median_elapsed = statistics.median(elapsed_vals) if elapsed_vals else None
        results.append((label, median_ops, median_elapsed))

    per_batch = args.ops // args.batches
    print(f"\nStress Test: {args.ops:,} ops ({args.batches} x {per_batch:,})")
    if args.trials > 1:
        print(f"Median of {args.trials} trials")
    print()
    for name, ops_s, elapsed in results:
        if elapsed is None:
            print(f"{name}: {ops_s:,.0f} ops/s")
        else:
            print(f"{name}: {ops_s:,.0f} ops/s ({elapsed:.3f}s)")
        print()


def run(args):
    if args.report_mode == "stress":
        run_stress(args)
        return

    keys = build_workload(args.n, args.seed)
    queries = build_queries(keys, args.ops, args.hit_ratio, args.seed + 1)

    hash_table = set(keys)
    py_dict = {k: "v" for k in keys}

    rb = SortedArrayTree(keys)

    trie = Trie()
    for k in keys:
        trie.insert(k)

    skip = SkipList()
    for k in keys:
        skip.insert(k)

    btree = StaticBTree(keys, fanout=args.btree_fanout)

    lqft = LQFT()
    lqft.clear()
    lqft.set_write_batch_size(args.lqft_batch)
    lqft.bulk_insert(keys, "v")
    lqft.seal_reads()

    lqft_light = None
    if LQFTLight is not None:
        lqft_light = LQFTLight()
        for k in keys:
            lqft_light.insert(k, "v")

    def one_pass_results():
        rows = []
        rows.append(benchmark_contains("Hash Table", hash_table.__contains__, queries, args.threads))
        rows.append(benchmark_contains("Python dict", py_dict.__contains__, queries, args.threads))
        rows.append(benchmark_contains("Trie", trie.contains, queries, args.threads))
        rows.append(benchmark_contains("Red-Black Tree", rb.contains, queries, args.threads))
        if args.lqft_mode == "bulk":
            rows.append(benchmark_lqft_bulk("LQFT Native", lqft, queries, args.threads))
        else:
            rows.append(benchmark_contains("LQFT Native", lqft.contains, queries, args.threads))
        rows.append(benchmark_contains("Skip List", skip.contains, queries, args.threads))
        if lqft_light is not None:
            rows.append(benchmark_contains("LQFT Light", lambda k: lqft_light.search(k) is not None, queries, args.threads))
        rows.append(benchmark_contains("B-Tree", btree.contains, queries, args.threads))
        return rows

    per_trial = [one_pass_results() for _ in range(max(1, args.trials))]
    labels = [name for name, _ in per_trial[0]]
    results = []
    for i, label in enumerate(labels):
        vals = [trial[i][1] for trial in per_trial]
        results.append((label, statistics.median(vals)))

    per_thread = args.ops // args.threads
    print(f"\nComprehensive Tree Comparison: {args.ops:,} ops ({args.threads} x {per_thread:,})")
    if args.trials > 1:
        print(f"Median of {args.trials} trials")
    for name, ops_s in results:
        print(f"Multi-thread {name}: {ops_s:,.0f} ops/s")


def parse_args():
    parser = argparse.ArgumentParser(description="Screenshot-style benchmark report.")
    parser.add_argument("--n", type=int, default=120000, help="Number of keys loaded into each structure")
    parser.add_argument("--ops", type=int, default=600000, help="Total lookup operations in the comparison")
    parser.add_argument("--threads", type=int, default=max(2, (os.cpu_count() or 4) // 2))
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--hit-ratio", type=float, default=0.85)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--lqft-batch", type=int, default=4096)
    parser.add_argument("--btree-fanout", type=int, default=32)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--report-mode", choices=("tree", "stress"), default="tree")
    parser.add_argument(
        "--lqft-mode",
        choices=("per-key", "bulk"),
        default="per-key",
        help="per-key matches legacy fairness; bulk measures native batched membership path",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
