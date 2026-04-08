import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import lqft_engine
from lqft_engine import LQFTShardedMap, LQFTShardedStorageEngine


class Phase9ShardedStorageCliTestCase(unittest.TestCase):
    def setUp(self):
        self.map = LQFTShardedMap(shard_count=4)
        self.addCleanup(self.map.shutdown_background_worker)

    def test_cli_save_and_inspect_sharded_storage_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = temp_root / "sharded-storage-manifest.json.gz"
            checkpoint_dir = temp_root / "checkpoints"
            wal_dir = temp_root / "wal"

            with mock.patch("sys.stdout") as fake_stdout:
                exit_code = lqft_engine._main(
                    [
                        "save-sharded-storage-engine-manifest",
                        str(manifest_path),
                        "--shard-count",
                        "4",
                        "--checkpoint-dir",
                        str(checkpoint_dir),
                        "--wal-dir",
                        str(wal_dir),
                        "--manifest-sign-key-text",
                        "manifest-secret",
                    ]
                )
            save_payload = json.loads("".join(call.args[0] for call in fake_stdout.write.call_args_list))

            with mock.patch("sys.stdout") as fake_stdout:
                inspect_exit = lqft_engine._main(
                    [
                        "inspect-sharded-storage-engine-manifest",
                        str(manifest_path),
                        "--verify-key-text",
                        "manifest-secret",
                    ]
                )
            inspect_payload = json.loads("".join(call.args[0] for call in fake_stdout.write.call_args_list))

        self.assertEqual(exit_code, 0)
        self.assertEqual(inspect_exit, 0)
        self.assertEqual(save_payload["path"], str(manifest_path))
        self.assertEqual(save_payload["manifest"]["shard_count"], 4)
        self.assertEqual(inspect_payload["manifest"]["shard_count"], 4)
        self.assertTrue(inspect_payload["policy_check"]["loadable"])

    def test_cli_recover_and_checkpoint_sharded_storage_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = temp_root / "sharded-storage-manifest.json"
            checkpoint_dir = temp_root / "checkpoints"
            wal_dir = temp_root / "wal"

            engine = LQFTShardedStorageEngine(
                shard_count=4,
                checkpoint_dir=checkpoint_dir,
                wal_dir=wal_dir,
            )
            engine.attach(self.map, truncate_wal=True)
            self.map.put("alpha", "one")
            self.map.snapshot()
            self.map.put("beta", "two")
            engine.checkpoint(self.map)
            engine.save_manifest(manifest_path, sign_key="manifest-secret")

            with mock.patch("sys.stdout") as fake_stdout:
                recover_exit = lqft_engine._main(
                    [
                        "recover-sharded-storage-engine-manifest",
                        str(manifest_path),
                        "--verify-key-text",
                        "manifest-secret",
                        "--include-current-state-payload",
                    ]
                )
            recover_payload = json.loads("".join(call.args[0] for call in fake_stdout.write.call_args_list))

            with mock.patch("sys.stdout") as fake_stdout:
                checkpoint_exit = lqft_engine._main(
                    [
                        "checkpoint-sharded-storage-engine-manifest",
                        str(manifest_path),
                        "--verify-key-text",
                        "manifest-secret",
                        "--retain-last",
                        "1",
                    ]
                )
            checkpoint_payload = json.loads("".join(call.args[0] for call in fake_stdout.write.call_args_list))

        self.assertEqual(recover_exit, 0)
        self.assertEqual(checkpoint_exit, 0)
        self.assertEqual(recover_payload["storage"]["shard_count"], 4)
        self.assertEqual(recover_payload["stats"]["logical_items"], 2)
        self.assertEqual(recover_payload["current_state"]["format"], "lqft-sharded-current-state-v1")
        self.assertEqual(len(checkpoint_payload["checkpoint_paths"]), 4)
        self.assertEqual(checkpoint_payload["storage"]["shard_count"], 4)

    def test_cli_inspect_sharded_manifest_reports_schema_policy_code(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "invalid-sharded-storage-manifest.json"
            lqft_engine._write_json_file_atomic(
                manifest_path,
                lqft_engine._build_persisted_file_document(
                    {
                        "format": "lqft-sharded-storage-engine-manifest-v1",
                        "shard_count": 4,
                        "storage": "not-a-dict",
                        "checkpoint_policy": {},
                        "serialization": {},
                        "shards": [],
                    },
                    compression="none",
                ),
                sort_keys=True,
            )

            inspection = lqft_engine.inspect_sharded_storage_engine_manifest(manifest_path)

            with mock.patch("sys.stdout") as fake_stdout:
                exit_code = lqft_engine._main(
                    [
                        "inspect-sharded-storage-engine-manifest",
                        str(manifest_path),
                        "--fail-on-policy-fail",
                    ]
                )
            cli_payload = json.loads("".join(call.args[0] for call in fake_stdout.write.call_args_list))

        self.assertFalse(inspection["policy_check"]["loadable"])
        self.assertEqual(inspection["policy_check"]["error_code"], "schema_invalid_storage_engine_manifest")
        self.assertEqual(inspection["effective_policy"]["reason_code"], "schema_invalid_storage_engine_manifest")
        self.assertEqual(exit_code, 16)
        self.assertEqual(cli_payload["policy_check"]["error_code"], "schema_invalid_storage_engine_manifest")

    def test_cli_inspect_sharded_manifest_fail_on_policy_uses_verification_exit_code(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = temp_root / "sharded-storage-manifest.json"
            checkpoint_dir = temp_root / "checkpoints"
            wal_dir = temp_root / "wal"

            signer = lqft_engine.generate_ed25519_keypair()
            wrong_verifier = lqft_engine.generate_ed25519_keypair()
            engine = LQFTShardedStorageEngine(
                shard_count=4,
                checkpoint_dir=checkpoint_dir,
                wal_dir=wal_dir,
            )
            engine.save_manifest(manifest_path, sign_key=signer["private_key_pem"])

            with mock.patch("sys.stdout") as fake_stdout:
                exit_code = lqft_engine._main(
                    [
                        "inspect-sharded-storage-engine-manifest",
                        str(manifest_path),
                        "--verify-key-text",
                        wrong_verifier["public_key_pem"].decode("utf-8"),
                        "--fail-on-policy-fail",
                    ]
                )
            payload = json.loads("".join(call.args[0] for call in fake_stdout.write.call_args_list))

        self.assertEqual(exit_code, 12)
        self.assertFalse(payload["effective_policy"]["allowed"])
        self.assertEqual(payload["policy_check"]["error_code"], "verification_signer_identity_mismatch")
        self.assertEqual(payload["effective_policy"]["reason_code"], "verification_signer_identity_mismatch")


if __name__ == "__main__":
    unittest.main()
