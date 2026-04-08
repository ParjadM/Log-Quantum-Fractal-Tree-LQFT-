import unittest

from lqft_engine import LQFTShardedMap, LQFTShardedSnapshot


class Phase9ShardedMapTestCase(unittest.TestCase):
    def setUp(self):
        self.map = LQFTShardedMap(shard_count=8)
        self.addCleanup(self.map.shutdown_background_worker)

    def test_put_get_delete_round_trip(self):
        self.map.put("alpha", "one")
        self.map.put("beta", "two")

        self.assertEqual(self.map.get("alpha"), "one")
        self.assertTrue(self.map.contains("beta"))
        self.assertEqual(len(self.map), 2)

        self.map.delete("beta")

        self.assertFalse(self.map.contains("beta"))
        self.assertIsNone(self.map.get("beta"))
        self.assertEqual(len(self.map), 1)

    def test_shard_for_key_is_stable_for_same_key(self):
        first = self.map.shard_for_key("stable-key")
        second = self.map.shard_for_key("stable-key")

        self.assertEqual(first, second)
        self.assertGreaterEqual(first, 0)
        self.assertLess(first, self.map.shard_count)

    def test_snapshot_isolated_from_future_mutations(self):
        self.map.put("alpha", "one")
        snap = self.map.snapshot()

        self.assertIsInstance(snap, LQFTShardedSnapshot)
        self.assertTrue(snap.contains("alpha"))
        self.assertEqual(snap.get("alpha"), "one")

        self.map.put("alpha", "updated")
        self.map.put("beta", "two")

        self.assertEqual(snap.get("alpha"), "one")
        self.assertFalse(snap.contains("beta"))
        self.assertEqual(self.map.get("alpha"), "updated")

    def test_stats_aggregate_across_shards(self):
        for index in range(40):
            self.map.put(f"key-{index}", f"value-{index}")

        stats = self.map.stats()

        self.assertEqual(stats["model"], "lqft-sharded-map")
        self.assertEqual(stats["shard_count"], 8)
        self.assertEqual(stats["logical_items"], 40)
        self.assertEqual(len(stats["shard_item_counts"]), 8)
        self.assertEqual(sum(stats["shard_item_counts"]), 40)
        self.assertGreaterEqual(stats["max_shard_items"], stats["min_shard_items"])

    def test_export_current_state_payload_contains_all_items(self):
        self.map.put("alpha", "one")
        self.map.put("beta", "two")
        self.map.put("gamma", "three")

        payload = self.map.export_current_state_payload()

        self.assertEqual(payload["format"], "lqft-sharded-current-state-v1")
        self.assertEqual(payload["metadata"]["logical_items"], 3)
        exported = {item["key"]: item["value"] for item in payload["items"]}
        self.assertEqual(exported, {"alpha": "one", "beta": "two", "gamma": "three"})


if __name__ == "__main__":
    unittest.main()
