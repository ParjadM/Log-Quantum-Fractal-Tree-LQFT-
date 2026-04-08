import tempfile
import unittest
from pathlib import Path

from lqft_engine import LQFTShardedMap, LQFTShardedStorageEngine


class Phase9ShardedStorageEngineTestCase(unittest.TestCase):
    def setUp(self):
        self.map = LQFTShardedMap(shard_count=4)
        self.addCleanup(self.map.shutdown_background_worker)

    def test_checkpoint_and_recover_map_across_shards(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            engine = LQFTShardedStorageEngine(
                shard_count=4,
                checkpoint_dir=temp_root / "checkpoints",
                wal_dir=temp_root / "wal",
            )
            engine.attach(self.map, truncate_wal=True)

            self.map.put("alpha", "one")
            self.map.put("beta", "two")
            self.map.snapshot()
            self.map.put("gamma", "three")

            checkpoint_paths = engine.checkpoint(self.map)
            self.assertEqual(len(checkpoint_paths), 4)

            recovered = engine.recover_map()
            self.addCleanup(recovered.shutdown_background_worker)

            self.assertEqual(recovered.get("alpha"), "one")
            self.assertEqual(recovered.get("beta"), "two")
            self.assertEqual(recovered.get("gamma"), "three")
            self.assertEqual(len(recovered), 3)

    def test_manifest_round_trip_for_sharded_storage_engine(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            engine = LQFTShardedStorageEngine(
                shard_count=4,
                checkpoint_dir=temp_root / "checkpoints",
                wal_dir=temp_root / "wal",
                checkpoint_retain_last=2,
                checkpoint_indent=4,
                checkpoint_sort_keys=True,
            )
            manifest_path = temp_root / "sharded-storage-manifest.json.gz"

            saved_path = engine.save_manifest(manifest_path, sign_key="manifest-secret")
            loaded = LQFTShardedStorageEngine.load_manifest(
                manifest_path,
                verify_key="manifest-secret",
            )

        self.assertEqual(saved_path, str(manifest_path))
        self.assertEqual(loaded.shard_count, 4)
        self.assertEqual(Path(loaded.checkpoint_dir).name, "checkpoints")
        self.assertEqual(Path(loaded.wal_dir).name, "wal")
        self.assertEqual(loaded.checkpoint_retain_last, 2)
        self.assertEqual(loaded.checkpoint_indent, 4)
        self.assertTrue(loaded.checkpoint_sort_keys)

    def test_attach_rejects_shard_count_mismatch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            engine = LQFTShardedStorageEngine(
                shard_count=2,
                checkpoint_dir=temp_root / "checkpoints",
                wal_dir=temp_root / "wal",
            )

            with self.assertRaisesRegex(ValueError, "shard_count mismatch"):
                engine.attach(self.map)


if __name__ == "__main__":
    unittest.main()
