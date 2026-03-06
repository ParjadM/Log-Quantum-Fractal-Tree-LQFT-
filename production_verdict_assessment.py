import argparse
import gc
import random
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Optional

from lqft_engine import LQFT

try:
    import psutil
except Exception:
    psutil = None

try:
    from sortedcontainers import SortedDict
except Exception:
    SortedDict = None


@dataclass
class RunResult:
    name: str
    insert_ops_s: float
    search_ops_s: float
    delete_ops_s: float
    overall_ops_s: float
    size_bytes_estimate: Optional[int]


def get_rss_bytes() -> Optional[int]:
    if psutil is None:
        return None
    try:
        return int(psutil.Process().memory_info().rss)
    except Exception:
        return None


def deep_sizeof(obj, seen=None) -> int:
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        for k, v in obj.items():
            size += deep_sizeof(k, seen)
            size += deep_sizeof(v, seen)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        for item in obj:
            size += deep_sizeof(item, seen)
    elif hasattr(obj, "__dict__"):
        size += deep_sizeof(vars(obj), seen)
    elif hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            if hasattr(obj, slot):
                size += deep_sizeof(getattr(obj, slot), seen)
    return size


def run_dict(keys, values, search_keys, delete_keys) -> RunResult:
    data = {}

    t0 = time.perf_counter()
    for k, v in zip(keys, values):
        data[k] = v
    t_insert = time.perf_counter() - t0

    t0 = time.perf_counter()
    for k in search_keys:
        _ = k in data
    t_search = time.perf_counter() - t0

    t0 = time.perf_counter()
    for k in delete_keys:
        data.pop(k, None)
    t_delete = time.perf_counter() - t0

    total_ops = len(keys) + len(search_keys) + len(delete_keys)
    total_time = t_insert + t_search + t_delete

    return RunResult(
        name="dict",
        insert_ops_s=len(keys) / t_insert,
        search_ops_s=len(search_keys) / t_search,
        delete_ops_s=len(delete_keys) / t_delete,
        overall_ops_s=total_ops / total_time,
        size_bytes_estimate=deep_sizeof(data),
    )


def run_sorted_dict(keys, values, search_keys, delete_keys) -> Optional[RunResult]:
    if SortedDict is None:
        return None

    data = SortedDict()

    t0 = time.perf_counter()
    for k, v in zip(keys, values):
        data[k] = v
    t_insert = time.perf_counter() - t0

    t0 = time.perf_counter()
    for k in search_keys:
        _ = k in data
    t_search = time.perf_counter() - t0

    t0 = time.perf_counter()
    for k in delete_keys:
        data.pop(k, None)
    t_delete = time.perf_counter() - t0

    total_ops = len(keys) + len(search_keys) + len(delete_keys)
    total_time = t_insert + t_search + t_delete

    return RunResult(
        name="SortedDict",
        insert_ops_s=len(keys) / t_insert,
        search_ops_s=len(search_keys) / t_search,
        delete_ops_s=len(delete_keys) / t_delete,
        overall_ops_s=total_ops / total_time,
        size_bytes_estimate=deep_sizeof(data),
    )


def run_lqft(keys, values, search_keys, delete_keys, batch_size: int) -> RunResult:
    lqft = LQFT()
    lqft.clear()
    lqft.set_write_batch_size(batch_size)

    rss_before = get_rss_bytes()
    same_value = values[0] if values and all(v == values[0] for v in values) else None

    t0 = time.perf_counter()
    if same_value is not None:
        lqft.bulk_insert(keys, same_value)
    else:
        for k, v in zip(keys, values):
            lqft.insert(k, v)
    _ = lqft.contains(keys[0])
    t_insert = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = lqft.bulk_contains_count(search_keys)
    t_search = time.perf_counter() - t0

    t0 = time.perf_counter()
    for k in delete_keys:
        lqft.remove(k)
    t_delete = time.perf_counter() - t0

    total_ops = len(keys) + len(search_keys) + len(delete_keys)
    total_time = t_insert + t_search + t_delete

    size_estimate = None
    try:
        stats = lqft.get_stats()
        native_bytes = int(stats.get("estimated_native_bytes", 0))
        if native_bytes > 0:
            size_estimate = native_bytes
    except Exception:
        size_estimate = None

    if size_estimate is None:
        rss_after = get_rss_bytes()
        if rss_before is not None and rss_after is not None and rss_after >= rss_before:
            size_estimate = rss_after - rss_before

    return RunResult(
        name="LQFT",
        insert_ops_s=len(keys) / t_insert,
        search_ops_s=len(search_keys) / t_search,
        delete_ops_s=len(delete_keys) / t_delete,
        overall_ops_s=total_ops / total_time,
        size_bytes_estimate=size_estimate,
    )


def fmt_ops(v: float) -> str:
    return f"{v:,.0f}"


def fmt_bytes(v: Optional[int]) -> str:
    if v is None:
        return "n/a"
    return f"{v / (1024 * 1024):,.2f} MB"


def median_result(name: str, runs: list[RunResult]) -> RunResult:
    return RunResult(
        name=name,
        insert_ops_s=statistics.median(r.insert_ops_s for r in runs),
        search_ops_s=statistics.median(r.search_ops_s for r in runs),
        delete_ops_s=statistics.median(r.delete_ops_s for r in runs),
        overall_ops_s=statistics.median(r.overall_ops_s for r in runs),
        size_bytes_estimate=statistics.median([r.size_bytes_estimate for r in runs if r.size_bytes_estimate is not None])
        if any(r.size_bytes_estimate is not None for r in runs)
        else None,
    )


def print_table(results: list[RunResult]) -> None:
    print("\nProduction Verdict Assessment")
    print("-----------------------------")
    print(f"{'Structure':<14}{'Overall':>12}{'Insert':>12}{'Search':>12}{'Delete':>12}{'Size Est.':>14}")
    for r in sorted(results, key=lambda x: x.overall_ops_s, reverse=True):
        print(
            f"{r.name:<14}{fmt_ops(r.overall_ops_s):>12}{fmt_ops(r.insert_ops_s):>12}"
            f"{fmt_ops(r.search_ops_s):>12}{fmt_ops(r.delete_ops_s):>12}{fmt_bytes(r.size_bytes_estimate):>14}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Production-level LQFT vs map/tree verdict with real sizing paths.")
    parser.add_argument("--n", type=int, default=80000)
    parser.add_argument("--q", type=int, default=30000)
    parser.add_argument("--d", type=int, default=30000)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--lqft-batch-size", type=int, default=2048)
    args = parser.parse_args()

    random.seed(args.seed)
    keys = [f"k{i}" for i in range(args.n)]
    values = ["v"] * args.n
    search_keys = random.sample(keys, min(args.q, len(keys)))
    delete_keys = random.sample(keys, min(args.d, len(keys)))

    dict_runs = []
    lqft_runs = []
    sorted_runs = []

    for _ in range(args.trials):
        dict_runs.append(run_dict(keys, values, search_keys, delete_keys))
        lqft_runs.append(run_lqft(keys, values, search_keys, delete_keys, args.lqft_batch_size))
        s = run_sorted_dict(keys, values, search_keys, delete_keys)
        if s is not None:
            sorted_runs.append(s)
        gc.collect()

    results = [median_result("dict", dict_runs), median_result("LQFT", lqft_runs)]
    if sorted_runs:
        results.append(median_result("SortedDict", sorted_runs))

    print_table(results)


if __name__ == "__main__":
    main()
