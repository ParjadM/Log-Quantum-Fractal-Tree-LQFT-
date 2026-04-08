import argparse
import importlib
import base64
import gzip
import hashlib
import hmac
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
import time

try:
    import psutil
except Exception:
    psutil = None

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519
except Exception:
    InvalidSignature = None
    serialization = None
    ed25519 = None

# ---------------------------------------------------------
# STRICT NATIVE ENTERPRISE WRAPPER (v1.0.5)
# ---------------------------------------------------------
# Architect: Parjad Minooei
# Target: McMaster B.Tech / UofT MScAC Portfolio

try:
    lqft_c_engine = importlib.import_module("lqft_c_engine")
except ImportError as exc:
    raise ImportError(
        "Native module 'lqft_c_engine' is unavailable. "
        "Install a published wheel with 'pip install lqft-python-engine' or build the extension locally with "
        "'python setup.py build_ext --inplace'."
    ) from exc


def _new_native_lqft(migration_threshold=50000, shared_with=None, clone_from=None, state_capsule=None):
    return LQFT(
        migration_threshold=migration_threshold,
        _shared_with=shared_with,
        _clone_from=clone_from,
        _state_capsule=state_capsule,
    )


__all__ = [
    "LQFT",
    "MutableLQFT",
    "LQFTMap",
    "LQFTSnapshot",
    "LQFTShardedMap",
    "LQFTShardedSnapshot",
    "LQFTShardedStorageEngine",
    "LQFTStorageEngine",
    "LQFTPolicyError",
    "build_signer_trust_store",
    "classify_policy_error",
    "generate_ed25519_keypair",
    "inspect_sharded_storage_engine_manifest",
    "inspect_storage_engine_manifest",
    "inspect_signer_trust_store",
    "load_signer_trust_store",
    "save_signer_trust_store_manifest",
    "write_ed25519_keypair",
]


_READ_CACHE_ABSENT = object()


class LQFTPolicyError(ValueError):
    def __init__(self, message, *, code, component, is_fallback=False):
        super().__init__(message)
        self.code = code
        self.component = component
        self.is_fallback = bool(is_fallback)


@dataclass(slots=True)
class LQFTStorageEngine:
    checkpoint_path: str | None = None
    wal_path: str | None = None
    wal_fsync: bool = True
    checkpoint_retain_last: int | None = None
    checkpoint_sign_key: object = None
    checkpoint_validity: dict | None = None
    checkpoint_verify_key: object = None
    checkpoint_trusted_signers: object = None
    checkpoint_min_remaining_validity_seconds: int | None = None
    checkpoint_indent: int = 2
    checkpoint_sort_keys: bool = False

    def __post_init__(self):
        if self.checkpoint_path is None and self.wal_path is None:
            raise ValueError("storage engine requires at least one of checkpoint_path or wal_path")
        if self.checkpoint_path is not None:
            self.checkpoint_path = _coerce_filesystem_path(self.checkpoint_path)
        if self.wal_path is not None:
            self.wal_path = _coerce_filesystem_path(self.wal_path)
            if _path_uses_gzip(self.wal_path):
                raise ValueError("storage engine WAL paths must not use gzip")
        if self.checkpoint_retain_last is not None:
            if type(self.checkpoint_retain_last) is not int or self.checkpoint_retain_last < 0:
                raise ValueError("checkpoint_retain_last must be a non-negative integer or null")

    def attach(self, lqft_map, *, truncate_wal=False):
        if not isinstance(lqft_map, LQFTMap):
            raise TypeError("lqft_map must be an LQFTMap")
        if self.wal_path is not None:
            lqft_map.enable_write_ahead_log(self.wal_path, fsync=self.wal_fsync, truncate=truncate_wal)
        return lqft_map

    @staticmethod
    def _manifest_path_field(value, *, field_name):
        if value is None:
            return None
        try:
            return _coerce_filesystem_path(value)
        except (TypeError, ValueError) as exc:
            raise TypeError(f"{field_name} must be a filesystem path or null to export a storage-engine manifest") from exc

    @staticmethod
    def _resolve_verification_key_material(value):
        if isinstance(value, (str, os.PathLike)):
            candidate_path = os.fspath(value)
            if os.path.exists(candidate_path) and os.path.isfile(candidate_path):
                return _read_bytes_file(candidate_path)
        return value

    @staticmethod
    def _resolve_signing_key_material(value):
        if isinstance(value, (str, os.PathLike)):
            candidate_path = os.fspath(value)
            if os.path.exists(candidate_path) and os.path.isfile(candidate_path):
                return _read_bytes_file(candidate_path)
        return value

    def export_manifest(self):
        return {
            "format": "lqft-storage-engine-manifest-v1",
            "storage": {
                "checkpoint_path": self.checkpoint_path,
                "wal_path": self.wal_path,
                "wal_fsync": bool(self.wal_fsync),
                "checkpoint_retain_last": self.checkpoint_retain_last,
            },
            "checkpoint_policy": {
                "sign_key_path": self._manifest_path_field(
                    self.checkpoint_sign_key,
                    field_name="checkpoint_sign_key",
                ),
                "verify_key_path": self._manifest_path_field(
                    self.checkpoint_verify_key,
                    field_name="checkpoint_verify_key",
                ),
                "trusted_signers_path": self._manifest_path_field(
                    self.checkpoint_trusted_signers,
                    field_name="checkpoint_trusted_signers",
                ),
                "min_remaining_validity_seconds": self.checkpoint_min_remaining_validity_seconds,
                "validity": None if self.checkpoint_validity is None else dict(self.checkpoint_validity),
            },
            "serialization": {
                "indent": int(self.checkpoint_indent),
                "sort_keys": bool(self.checkpoint_sort_keys),
            },
            "exported_at_ns": time.time_ns(),
        }

    @classmethod
    def from_manifest(cls, manifest_payload):
        if not isinstance(manifest_payload, dict):
            raise TypeError("manifest_payload must be a dictionary")
        if manifest_payload.get("format") != "lqft-storage-engine-manifest-v1":
            raise ValueError("unsupported storage engine manifest format")

        storage = manifest_payload.get("storage")
        checkpoint_policy = manifest_payload.get("checkpoint_policy")
        serialization = manifest_payload.get("serialization")
        if not isinstance(storage, dict):
            raise ValueError("storage engine manifest must include a storage dictionary")
        if not isinstance(checkpoint_policy, dict):
            raise ValueError("storage engine manifest must include a checkpoint_policy dictionary")
        if not isinstance(serialization, dict):
            raise ValueError("storage engine manifest must include a serialization dictionary")

        return cls(
            checkpoint_path=storage.get("checkpoint_path"),
            wal_path=storage.get("wal_path"),
            wal_fsync=bool(storage.get("wal_fsync", True)),
            checkpoint_retain_last=storage.get("checkpoint_retain_last"),
            checkpoint_sign_key=checkpoint_policy.get("sign_key_path"),
            checkpoint_validity=checkpoint_policy.get("validity"),
            checkpoint_verify_key=checkpoint_policy.get("verify_key_path"),
            checkpoint_trusted_signers=checkpoint_policy.get("trusted_signers_path"),
            checkpoint_min_remaining_validity_seconds=checkpoint_policy.get("min_remaining_validity_seconds"),
            checkpoint_indent=serialization.get("indent", 2),
            checkpoint_sort_keys=bool(serialization.get("sort_keys", False)),
        )

    def save_manifest(self, path, *, sign_key=None, validity=None, indent=2, sort_keys=False):
        payload = self.export_manifest()
        document = _build_persisted_file_document(
            payload,
            compression="gzip" if _path_uses_gzip(path) else "none",
            sign_key=sign_key,
            validity=validity,
        )
        _write_json_file_atomic(path, document, indent=indent, sort_keys=sort_keys)
        return _coerce_filesystem_path(path)

    @classmethod
    def load_manifest(
        cls,
        path,
        *,
        verify_key=None,
        trusted_signers=None,
        min_remaining_validity_seconds=None,
    ):
        payload = _extract_payload_from_file_document(
            _read_json_file(path),
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        return cls.from_manifest(payload)

    def checkpoint(self, lqft_map, *, snapshots=None, retain_last=None):
        if not isinstance(lqft_map, LQFTMap):
            raise TypeError("lqft_map must be an LQFTMap")
        if self.checkpoint_path is None:
            raise ValueError("storage engine checkpoint_path is not configured")

        effective_retain_last = self.checkpoint_retain_last if retain_last is None else retain_last
        if effective_retain_last is not None and (type(effective_retain_last) is not int or effective_retain_last < 0):
            raise ValueError("retain_last must be a non-negative integer or null")
        if snapshots is not None and effective_retain_last is not None:
            raise ValueError("snapshots and retain_last cannot be used together")

        latest_snapshot = lqft_map.latest_snapshot()
        if effective_retain_last is not None:
            lqft_map.compact(retain_last=effective_retain_last)
        elif latest_snapshot is None or lqft_map.stats()["pending_mutations"]:
            lqft_map.snapshot()

        checkpoint_path = lqft_map.save_snapshot_bundle(
            self.checkpoint_path,
            snapshots=snapshots,
            indent=self.checkpoint_indent,
            sort_keys=self.checkpoint_sort_keys,
            sign_key=self._resolve_signing_key_material(self.checkpoint_sign_key),
            validity=self.checkpoint_validity,
        )

        if self.wal_path is not None:
            lqft_map.enable_write_ahead_log(self.wal_path, fsync=self.wal_fsync, truncate=True)

        return checkpoint_path

    def recover_map(self, *, migration_threshold=50000, truncate_incomplete_wal_tail=False):
        recovered = LQFTMap(migration_threshold=migration_threshold)
        try:
            if self.checkpoint_path is not None and os.path.exists(self.checkpoint_path) and os.path.getsize(self.checkpoint_path) > 0:
                recovered.load_snapshot_bundle_file(
                    self.checkpoint_path,
                    activate=True,
                    verify_key=self._resolve_verification_key_material(self.checkpoint_verify_key),
                    trusted_signers=self.checkpoint_trusted_signers,
                    min_remaining_validity_seconds=self.checkpoint_min_remaining_validity_seconds,
                )
            if self.wal_path is not None and os.path.exists(self.wal_path) and os.path.getsize(self.wal_path) > 0:
                recovered.replay_write_ahead_log(
                    self.wal_path,
                    truncate_incomplete_tail=truncate_incomplete_wal_tail,
                )
            self.attach(recovered, truncate_wal=False)
            return recovered
        except Exception:
            recovered.shutdown_background_worker(cancel_futures=True)
            raise


@dataclass(slots=True)
class LQFTShardedStorageEngine:
    shard_count: int
    checkpoint_dir: str | None = None
    wal_dir: str | None = None
    wal_fsync: bool = True
    checkpoint_retain_last: int | None = None
    checkpoint_sign_key: object = None
    checkpoint_validity: dict | None = None
    checkpoint_verify_key: object = None
    checkpoint_trusted_signers: object = None
    checkpoint_min_remaining_validity_seconds: int | None = None
    checkpoint_indent: int = 2
    checkpoint_sort_keys: bool = False

    def __post_init__(self):
        if type(self.shard_count) is not int or self.shard_count <= 0:
            raise ValueError("shard_count must be a positive integer")
        if self.checkpoint_dir is None and self.wal_dir is None:
            raise ValueError("sharded storage engine requires at least one of checkpoint_dir or wal_dir")
        if self.checkpoint_dir is not None:
            self.checkpoint_dir = _coerce_filesystem_path(self.checkpoint_dir)
        if self.wal_dir is not None:
            self.wal_dir = _coerce_filesystem_path(self.wal_dir)
        if self.checkpoint_retain_last is not None:
            if type(self.checkpoint_retain_last) is not int or self.checkpoint_retain_last < 0:
                raise ValueError("checkpoint_retain_last must be a non-negative integer or null")

    def _checkpoint_path_for_shard(self, index):
        if self.checkpoint_dir is None:
            return None
        return os.path.join(self.checkpoint_dir, f"shard-{index:04d}.checkpoint.json.gz")

    def _wal_path_for_shard(self, index):
        if self.wal_dir is None:
            return None
        return os.path.join(self.wal_dir, f"shard-{index:04d}.wal")

    def _engine_for_shard(self, index):
        return LQFTStorageEngine(
            checkpoint_path=self._checkpoint_path_for_shard(index),
            wal_path=self._wal_path_for_shard(index),
            wal_fsync=self.wal_fsync,
            checkpoint_retain_last=self.checkpoint_retain_last,
            checkpoint_sign_key=self.checkpoint_sign_key,
            checkpoint_validity=self.checkpoint_validity,
            checkpoint_verify_key=self.checkpoint_verify_key,
            checkpoint_trusted_signers=self.checkpoint_trusted_signers,
            checkpoint_min_remaining_validity_seconds=self.checkpoint_min_remaining_validity_seconds,
            checkpoint_indent=self.checkpoint_indent,
            checkpoint_sort_keys=self.checkpoint_sort_keys,
        )

    def attach(self, sharded_map, *, truncate_wal=False):
        if not isinstance(sharded_map, LQFTShardedMap):
            raise TypeError("sharded_map must be an LQFTShardedMap")
        if sharded_map.shard_count != self.shard_count:
            raise ValueError("shard_count mismatch between storage engine and map")
        for index in range(self.shard_count):
            self._engine_for_shard(index).attach(sharded_map.shard(index), truncate_wal=truncate_wal)
        return sharded_map

    def checkpoint(self, sharded_map, *, retain_last=None):
        if not isinstance(sharded_map, LQFTShardedMap):
            raise TypeError("sharded_map must be an LQFTShardedMap")
        if sharded_map.shard_count != self.shard_count:
            raise ValueError("shard_count mismatch between storage engine and map")

        checkpoint_paths = []
        for index in range(self.shard_count):
            checkpoint_paths.append(
                self._engine_for_shard(index).checkpoint(
                    sharded_map.shard(index),
                    retain_last=retain_last,
                )
            )
        return tuple(checkpoint_paths)

    def recover_map(self, *, migration_threshold=50000, truncate_incomplete_wal_tail=False):
        recovered = LQFTShardedMap(shard_count=self.shard_count, migration_threshold=migration_threshold)
        original_shards = [recovered.shard(index) for index in range(self.shard_count)]
        try:
            for index, original in enumerate(original_shards):
                replacement = self._engine_for_shard(index).recover_map(
                    migration_threshold=migration_threshold,
                    truncate_incomplete_wal_tail=truncate_incomplete_wal_tail,
                )
                recovered._shards[index] = replacement
                original.shutdown_background_worker(cancel_futures=True)
            return recovered
        except Exception:
            recovered.shutdown_background_worker(cancel_futures=True)
            raise

    def export_manifest(self):
        return {
            "format": "lqft-sharded-storage-engine-manifest-v1",
            "shard_count": self.shard_count,
            "storage": {
                "checkpoint_dir": self.checkpoint_dir,
                "wal_dir": self.wal_dir,
                "wal_fsync": bool(self.wal_fsync),
                "checkpoint_retain_last": self.checkpoint_retain_last,
            },
            "checkpoint_policy": {
                "sign_key_path": LQFTStorageEngine._manifest_path_field(
                    self.checkpoint_sign_key,
                    field_name="checkpoint_sign_key",
                ),
                "verify_key_path": LQFTStorageEngine._manifest_path_field(
                    self.checkpoint_verify_key,
                    field_name="checkpoint_verify_key",
                ),
                "trusted_signers_path": LQFTStorageEngine._manifest_path_field(
                    self.checkpoint_trusted_signers,
                    field_name="checkpoint_trusted_signers",
                ),
                "min_remaining_validity_seconds": self.checkpoint_min_remaining_validity_seconds,
                "validity": None if self.checkpoint_validity is None else dict(self.checkpoint_validity),
            },
            "serialization": {
                "indent": int(self.checkpoint_indent),
                "sort_keys": bool(self.checkpoint_sort_keys),
            },
            "shards": [
                self._engine_for_shard(index).export_manifest()
                for index in range(self.shard_count)
            ],
            "exported_at_ns": time.time_ns(),
        }

    @classmethod
    def from_manifest(cls, manifest_payload):
        if not isinstance(manifest_payload, dict):
            raise TypeError("manifest_payload must be a dictionary")
        if manifest_payload.get("format") != "lqft-sharded-storage-engine-manifest-v1":
            raise ValueError("unsupported sharded storage engine manifest format")

        shard_count = manifest_payload.get("shard_count")
        storage = manifest_payload.get("storage")
        checkpoint_policy = manifest_payload.get("checkpoint_policy")
        serialization = manifest_payload.get("serialization")
        if type(shard_count) is not int or shard_count <= 0:
            raise ValueError("sharded storage engine manifest must include a positive integer shard_count")
        if not isinstance(storage, dict):
            raise ValueError("sharded storage engine manifest must include a storage dictionary")
        if not isinstance(checkpoint_policy, dict):
            raise ValueError("sharded storage engine manifest must include a checkpoint_policy dictionary")
        if not isinstance(serialization, dict):
            raise ValueError("sharded storage engine manifest must include a serialization dictionary")

        shards = manifest_payload.get("shards")
        if not isinstance(shards, list) or len(shards) != shard_count:
            raise ValueError("sharded storage engine manifest must include one shard manifest per shard")

        return cls(
            shard_count=shard_count,
            checkpoint_dir=storage.get("checkpoint_dir"),
            wal_dir=storage.get("wal_dir"),
            wal_fsync=bool(storage.get("wal_fsync", True)),
            checkpoint_retain_last=storage.get("checkpoint_retain_last"),
            checkpoint_sign_key=checkpoint_policy.get("sign_key_path"),
            checkpoint_validity=checkpoint_policy.get("validity"),
            checkpoint_verify_key=checkpoint_policy.get("verify_key_path"),
            checkpoint_trusted_signers=checkpoint_policy.get("trusted_signers_path"),
            checkpoint_min_remaining_validity_seconds=checkpoint_policy.get("min_remaining_validity_seconds"),
            checkpoint_indent=serialization.get("indent", 2),
            checkpoint_sort_keys=bool(serialization.get("sort_keys", False)),
        )

    def save_manifest(self, path, *, sign_key=None, validity=None, indent=2, sort_keys=False):
        payload = self.export_manifest()
        document = _build_persisted_file_document(
            payload,
            compression="gzip" if _path_uses_gzip(path) else "none",
            sign_key=sign_key,
            validity=validity,
        )
        _write_json_file_atomic(path, document, indent=indent, sort_keys=sort_keys)
        return _coerce_filesystem_path(path)

    @classmethod
    def load_manifest(
        cls,
        path,
        *,
        verify_key=None,
        trusted_signers=None,
        min_remaining_validity_seconds=None,
    ):
        payload = _extract_payload_from_file_document(
            _read_json_file(path),
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        return cls.from_manifest(payload)


def _coerce_filesystem_path(path):
    try:
        resolved = os.fspath(path)
    except TypeError as exc:
        raise TypeError("path must be a string or os.PathLike object") from exc
    if not isinstance(resolved, str) or not resolved:
        raise ValueError("path must be a non-empty filesystem path")
    return resolved


def _write_bytes_file_atomic(path, payload):
    path = _coerce_filesystem_path(path)
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")

    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    temp_path = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    try:
        with open(temp_path, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def _read_bytes_file(path):
    path = _coerce_filesystem_path(path)
    with open(path, "rb") as handle:
        return handle.read()


def _path_uses_gzip(path):
    return _coerce_filesystem_path(path).lower().endswith(".gz")


def _canonical_json_bytes(payload):
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _coerce_binary_key_material(key_material, *, parameter_name):
    if isinstance(key_material, bytes):
        if not key_material:
            raise ValueError(f"{parameter_name} must not be empty")
        return key_material
    if isinstance(key_material, str):
        if not key_material:
            raise ValueError(f"{parameter_name} must not be empty")
        return key_material.encode("utf-8")
    raise TypeError(f"{parameter_name} must be bytes or str")


def _looks_like_pem_key(key_material):
    return key_material.lstrip().startswith(b"-----BEGIN ")


def _require_cryptography(feature_name):
    if serialization is None or ed25519 is None or InvalidSignature is None:
        raise ImportError(
            f"cryptography is required for {feature_name}. Install the 'cryptography' package."
        )


def _is_ed25519_private_key_instance(value):
    return ed25519 is not None and isinstance(value, ed25519.Ed25519PrivateKey)


def _is_ed25519_public_key_instance(value):
    return ed25519 is not None and isinstance(value, ed25519.Ed25519PublicKey)


def _ed25519_public_key_sha256(public_key):
    _require_cryptography("Ed25519 signing")
    raw_public_key = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return hashlib.sha256(raw_public_key).hexdigest()


def generate_ed25519_keypair():
    _require_cryptography("Ed25519 key generation")
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    public_key_sha256 = _ed25519_public_key_sha256(public_key)
    return {
        "algorithm": "ed25519",
        "key_id": f"sha256:{public_key_sha256}",
        "public_key_sha256": public_key_sha256,
        "private_key_pem": private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
        "public_key_pem": public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ),
    }


def write_ed25519_keypair(private_path, public_path, *, overwrite=False):
    keypair = generate_ed25519_keypair()
    private_path = _coerce_filesystem_path(private_path)
    public_path = _coerce_filesystem_path(public_path)
    if not overwrite:
        collisions = [path for path in (private_path, public_path) if os.path.exists(path)]
        if collisions:
            raise FileExistsError(f"refusing to overwrite existing key file: {collisions[0]}")

    _write_bytes_file_atomic(private_path, keypair["private_key_pem"])
    try:
        _write_bytes_file_atomic(public_path, keypair["public_key_pem"])
    except Exception:
        try:
            if os.path.exists(private_path):
                os.remove(private_path)
        except OSError:
            pass
        raise

    return {
        "algorithm": keypair["algorithm"],
        "key_id": keypair["key_id"],
        "public_key_sha256": keypair["public_key_sha256"],
        "private_path": private_path,
        "public_path": public_path,
    }


def build_signer_trust_store(trusted_signers):
    if trusted_signers is None:
        return None
    if isinstance(trusted_signers, (str, os.PathLike)):
        candidate_path = os.fspath(trusted_signers)
        if os.path.exists(candidate_path):
            return load_signer_trust_store(candidate_path)
        raise TypeError("trusted_signers string inputs must point to an existing trust-store path")
    if isinstance(trusted_signers, dict):
        items = trusted_signers.items()
    elif isinstance(trusted_signers, (list, tuple, set, frozenset)):
        items = ((None, value) for value in trusted_signers)
    else:
        raise TypeError("trusted_signers must be a mapping or iterable of Ed25519 verification keys")

    normalized_signers = {}
    for provided_key_id, verifier in items:
        if (
            isinstance(verifier, dict)
            and verifier.get("algorithm") == "ed25519"
            and "public_key" in verifier
            and "public_key_sha256" in verifier
            and "key_id" in verifier
        ):
            normalized = verifier
        else:
            normalized = _coerce_verification_key(verifier, parameter_name="trusted_signers")
        if normalized["algorithm"] != "ed25519":
            raise ValueError("trusted_signers only supports Ed25519 verification keys")

        normalized_key_id = normalized["key_id"]
        if provided_key_id is None:
            key_id = normalized_key_id
        else:
            if not isinstance(provided_key_id, str) or not provided_key_id:
                raise ValueError("trusted_signers keys must be non-empty strings")
            if provided_key_id != normalized_key_id:
                raise ValueError("trusted signer key_id does not match supplied verification key")
            key_id = provided_key_id

        if key_id in normalized_signers:
            raise ValueError(f"duplicate trusted signer key_id: {key_id}")
        normalized_signers[key_id] = normalized

    return normalized_signers


def _signer_store_entry_summary(key_id, signer_entry):
    return {
        "key_id": key_id,
        "algorithm": signer_entry.get("algorithm"),
        "public_key_sha256": signer_entry.get("public_key_sha256"),
        "metadata": dict(signer_entry.get("metadata") or {}),
    }


def _summarize_signer_store(signer_store):
    return [
        _signer_store_entry_summary(key_id, signer_store[key_id])
        for key_id in sorted(signer_store)
    ]


def _manifest_signature_summary(document):
    if not isinstance(document, dict) or document.get("format") != "lqft-persisted-file-v1":
        return False, None
    signature_block = document.get("signature")
    if not isinstance(signature_block, dict):
        return False, None
    signature = {
        key: value
        for key, value in signature_block.items()
        if key not in {"payload_ed25519", "payload_hmac_sha256"}
    }
    signature["algorithm"] = signature_block.get("algorithm")
    return True, signature


def _parse_utc_datetime_string(value, *, field_name):
    if not isinstance(value, str) or not value:
        raise ValueError(f"trust store manifest {field_name} must be a non-empty ISO-8601 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"trust store manifest {field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"trust store manifest {field_name} must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def _summarize_trust_store_validity(manifest_payload):
    return _summarize_validity_window(
        manifest_payload.get("validity"),
        subject_name="trust store manifest",
    )


def _invalid_validity_summary():
    return {
        "not_before": None,
        "expires_at": None,
        "warn_before_expiry_seconds": None,
        "seconds_until_expiry": None,
        "status": "invalid",
        "currently_valid": False,
    }


def _load_trust_store_manifest_payload(source, *, verify_key=None, trusted_manifest_signers=None):
    document = _read_json_file(source)
    signed, signature = _manifest_signature_summary(document)
    payload = document
    if isinstance(document, dict) and document.get("format") == "lqft-persisted-file-v1":
        payload = _extract_payload_from_file_document(
            document,
            verify_key=verify_key,
            trusted_signers=trusted_manifest_signers,
        )
    elif verify_key is not None or trusted_manifest_signers is not None:
        raise ValueError("trust store manifest is unsigned")
    if not isinstance(payload, dict):
        raise ValueError("trust store manifest must decode to a dictionary")
    return payload, signed, signature, _summarize_trust_store_validity(payload)


def _validate_minimum_remaining_validity_seconds(min_remaining_validity_seconds):
    if min_remaining_validity_seconds is None:
        return None
    if type(min_remaining_validity_seconds) is not int or min_remaining_validity_seconds < 0:
        raise ValueError("min_remaining_validity_seconds must be a non-negative integer or null")
    return min_remaining_validity_seconds


def _summarize_validity_window(validity, *, subject_name):
    if validity is None:
        return {
            "not_before": None,
            "expires_at": None,
            "warn_before_expiry_seconds": None,
            "seconds_until_expiry": None,
            "status": "unbounded",
            "currently_valid": True,
        }
    if not isinstance(validity, dict):
        raise ValueError(f"{subject_name} validity must be a dictionary")

    not_before = validity.get("not_before")
    expires_at = validity.get("expires_at")
    warn_before_expiry_seconds = validity.get("warn_before_expiry_seconds")
    parsed_not_before = None if not_before is None else _parse_utc_datetime_string(
        not_before,
        field_name="validity.not_before",
    )
    parsed_expires_at = None if expires_at is None else _parse_utc_datetime_string(
        expires_at,
        field_name="validity.expires_at",
    )
    if warn_before_expiry_seconds is not None:
        if type(warn_before_expiry_seconds) is not int or warn_before_expiry_seconds < 0:
            raise ValueError(f"{subject_name} validity.warn_before_expiry_seconds must be a non-negative integer or null")
    if parsed_not_before is not None and parsed_expires_at is not None and parsed_expires_at <= parsed_not_before:
        raise ValueError(f"{subject_name} validity.expires_at must be later than validity.not_before")
    if warn_before_expiry_seconds and parsed_expires_at is None:
        raise ValueError(f"{subject_name} validity.warn_before_expiry_seconds requires validity.expires_at")

    now = datetime.now(timezone.utc)
    seconds_until_expiry = None
    if parsed_expires_at is not None:
        seconds_until_expiry = int((parsed_expires_at - now).total_seconds())
    if parsed_not_before is not None and now < parsed_not_before:
        status = "not-yet-valid"
        currently_valid = False
    elif parsed_expires_at is not None and now >= parsed_expires_at:
        status = "expired"
        currently_valid = False
    elif (
        parsed_expires_at is not None
        and warn_before_expiry_seconds is not None
        and seconds_until_expiry is not None
        and seconds_until_expiry <= warn_before_expiry_seconds
    ):
        status = "expiring-soon"
        currently_valid = True
    else:
        status = "valid"
        currently_valid = True

    return {
        "not_before": None if parsed_not_before is None else parsed_not_before.isoformat(),
        "expires_at": None if parsed_expires_at is None else parsed_expires_at.isoformat(),
        "warn_before_expiry_seconds": warn_before_expiry_seconds,
        "seconds_until_expiry": seconds_until_expiry,
        "status": status,
        "currently_valid": currently_valid,
    }


def _enforce_minimum_remaining_validity(validity, *, subject_name, min_remaining_validity_seconds):
    min_remaining_validity_seconds = _validate_minimum_remaining_validity_seconds(
        min_remaining_validity_seconds
    )
    if min_remaining_validity_seconds is None:
        return
    if validity["seconds_until_expiry"] is None:
        raise ValueError(
            f"{subject_name} does not declare expires_at required by min_remaining_validity_seconds"
        )
    if validity["seconds_until_expiry"] < min_remaining_validity_seconds:
        raise ValueError(
            f"{subject_name} remaining validity is below min_remaining_validity_seconds: "
            f"{validity['seconds_until_expiry']} < {min_remaining_validity_seconds}"
        )


def _build_signer_store_from_manifest_payload(manifest_payload, manifest_dir, *, include_revoked):
    if isinstance(manifest_payload, dict) and "signers" in manifest_payload:
        signers = manifest_payload.get("signers")
        if not isinstance(signers, list) or not signers:
            raise ValueError("trust store manifest signers must be a non-empty list")
        signer_store = {}
        for signer in signers:
            if not isinstance(signer, dict):
                raise ValueError("trust store manifest signers must contain dictionaries")
            key_id = signer.get("key_id")
            path = signer.get("path")
            revoked = signer.get("revoked", False)
            metadata = signer.get("metadata")
            revocation_reason = signer.get("revocation_reason")
            if path is None:
                raise ValueError("trust store manifest signer entries must include a path")
            if not isinstance(path, str) or not path:
                raise ValueError("trust store manifest signer paths must be non-empty strings")
            if type(revoked) is not bool:
                raise ValueError("trust store manifest signer revoked must be a boolean")
            if metadata is not None and not isinstance(metadata, dict):
                raise ValueError("trust store manifest signer metadata must be a dictionary or null")
            if revocation_reason is not None and not isinstance(revocation_reason, str):
                raise ValueError("trust store manifest signer revocation_reason must be a string or null")
            if revoked and not include_revoked:
                continue
            resolved_path = path if os.path.isabs(path) else os.path.join(manifest_dir, path)
            normalized = _coerce_verification_key(_read_bytes_file(resolved_path), parameter_name="trusted_signers")
            resolved_key_id = normalized["key_id"]
            if key_id is not None:
                if not isinstance(key_id, str) or not key_id:
                    raise ValueError("trust store manifest signer key_id must be a non-empty string or null")
                if key_id != resolved_key_id:
                    raise ValueError("trust store manifest signer key_id does not match supplied verification key")
            if resolved_key_id in signer_store:
                raise ValueError(f"duplicate trusted signer key_id: {resolved_key_id}")
            normalized["metadata"] = {
                "source_path": resolved_path,
                "revoked": revoked,
                "revocation_reason": revocation_reason,
                "metadata": dict(metadata or {}),
            }
            signer_store[resolved_key_id] = normalized
        if not signer_store:
            raise ValueError("trust store manifest does not contain any active signers")
        return signer_store

    if isinstance(manifest_payload, dict) and manifest_payload:
        signer_entries = {}
        for key_id, path in manifest_payload.items():
            if key_id == "validity":
                continue
            if not isinstance(key_id, str) or not key_id:
                raise ValueError("trust store manifest keys must be non-empty signer key_id strings")
            if not isinstance(path, str) or not path:
                raise ValueError("trust store manifest values must be non-empty signer paths")
            resolved_path = path if os.path.isabs(path) else os.path.join(manifest_dir, path)
            signer_entries[key_id] = _read_bytes_file(resolved_path)
        if not signer_entries:
            raise ValueError("trust store manifest does not contain any active signers")
        return build_signer_trust_store(signer_entries)

    raise ValueError("trust store manifest must be a non-empty mapping or contain a non-empty signers list")


def inspect_signer_trust_store(source, *, verify_key=None, trusted_manifest_signers=None, min_remaining_validity_seconds=None):
    source = _coerce_filesystem_path(source)
    if os.path.isdir(source):
        load_error = None
        try:
            signer_store = load_signer_trust_store(
                source,
                verify_key=verify_key,
                trusted_manifest_signers=trusted_manifest_signers,
                min_remaining_validity_seconds=min_remaining_validity_seconds,
            )
        except (TypeError, ValueError) as exc:
            load_error = str(exc)
            try:
                signer_store = load_signer_trust_store(source)
            except (TypeError, ValueError):
                signer_store = {}
        classification = classify_policy_error(load_error)
        validity = {
            "not_before": None,
            "expires_at": None,
            "warn_before_expiry_seconds": None,
            "seconds_until_expiry": None,
            "status": "unbounded",
            "currently_valid": True,
        }
        return {
            "source": source,
            "source_type": "directory",
            "signed": False,
            "signature": None,
            "validity": validity,
            "active_signer_count": len(signer_store),
            "revoked_signer_count": 0,
            "active_signers": _summarize_signer_store(signer_store),
            "revoked_signers": [],
            "policy_check": {
                "verification_requested": verify_key is not None or trusted_manifest_signers is not None,
                "min_remaining_validity_seconds": min_remaining_validity_seconds,
                "loadable": load_error is None,
                "error": load_error,
                "error_code": None if classification is None else classification["code"],
                "error_code_is_fallback": False if classification is None else classification["is_fallback"],
            },
            "effective_policy": _build_effective_policy_summary(
                verification_mode=(
                    "trusted-manifest-signers"
                    if trusted_manifest_signers is not None
                    else ("direct-key" if verify_key is not None else "none")
                ),
                signer_trust_required=trusted_manifest_signers is not None,
                freshness_required=False,
                min_remaining_validity_seconds=min_remaining_validity_seconds,
                validity=validity,
                load_error=load_error,
            ),
        }

    document = _read_json_file(source)
    signed, signature = _manifest_signature_summary(document)
    validity = _invalid_validity_summary()
    active_signers = {}
    revoked_signers = {}
    load_error = None
    try:
        manifest_payload, signed, signature, validity = _load_trust_store_manifest_payload(
            source,
            verify_key=verify_key,
            trusted_manifest_signers=trusted_manifest_signers,
        )
        all_signers = _build_signer_store_from_manifest_payload(
            manifest_payload,
            os.path.dirname(source),
            include_revoked=True,
        )
        active_signers = {
            key_id: entry
            for key_id, entry in all_signers.items()
            if not bool((entry.get("metadata") or {}).get("revoked", False))
        }
        revoked_signers = {
            key_id: entry
            for key_id, entry in all_signers.items()
            if bool((entry.get("metadata") or {}).get("revoked", False))
        }
        try:
            load_signer_trust_store(
                source,
                verify_key=verify_key,
                trusted_manifest_signers=trusted_manifest_signers,
                min_remaining_validity_seconds=min_remaining_validity_seconds,
            )
        except (TypeError, ValueError) as exc:
            load_error = str(exc)
    except (TypeError, ValueError) as exc:
        load_error = str(exc)
        candidate_payload = document
        if isinstance(document, dict) and document.get("format") == "lqft-persisted-file-v1":
            candidate_payload = document.get("payload")
        if isinstance(candidate_payload, dict):
            try:
                validity = _summarize_trust_store_validity(candidate_payload)
            except ValueError:
                validity = _invalid_validity_summary()
            try:
                all_signers = _build_signer_store_from_manifest_payload(
                    candidate_payload,
                    os.path.dirname(source),
                    include_revoked=True,
                )
            except ValueError:
                all_signers = {}
            active_signers = {
                key_id: entry
                for key_id, entry in all_signers.items()
                if not bool((entry.get("metadata") or {}).get("revoked", False))
            }
            revoked_signers = {
                key_id: entry
                for key_id, entry in all_signers.items()
                if bool((entry.get("metadata") or {}).get("revoked", False))
            }
    classification = classify_policy_error(load_error)
    policy_check = {
        "verification_requested": verify_key is not None or trusted_manifest_signers is not None,
        "min_remaining_validity_seconds": min_remaining_validity_seconds,
        "loadable": load_error is None,
        "error": load_error,
        "error_code": None if classification is None else classification["code"],
        "error_code_is_fallback": False if classification is None else classification["is_fallback"],
    }
    return {
        "source": source,
        "source_type": "manifest",
        "signed": signed,
        "signature": signature,
        "validity": validity,
        "active_signer_count": len(active_signers),
        "revoked_signer_count": len(revoked_signers),
        "active_signers": _summarize_signer_store(active_signers),
        "revoked_signers": _summarize_signer_store(revoked_signers),
        "policy_check": policy_check,
        "effective_policy": _build_effective_policy_summary(
            verification_mode=(
                "trusted-manifest-signers"
                if trusted_manifest_signers is not None
                else ("direct-key" if verify_key is not None else "none")
            ),
            signer_trust_required=trusted_manifest_signers is not None,
            freshness_required=(
                min_remaining_validity_seconds is not None
                or validity["not_before"] is not None
                or validity["expires_at"] is not None
            ),
            min_remaining_validity_seconds=min_remaining_validity_seconds,
            validity=validity,
            load_error=load_error,
        ),
    }


def load_signer_trust_store(
    source,
    *,
    include_revoked=False,
    verify_key=None,
    trusted_manifest_signers=None,
    min_remaining_validity_seconds=None,
):
    source = _coerce_filesystem_path(source)

    try:
        min_remaining_validity_seconds = _validate_minimum_remaining_validity_seconds(
            min_remaining_validity_seconds
        )
        if os.path.isdir(source):
            if verify_key is not None or trusted_manifest_signers is not None:
                raise ValueError("directory trust stores do not support manifest signature verification")
            signer_store = {}
            for name in sorted(os.listdir(source)):
                path = os.path.join(source, name)
                if os.path.isfile(path) and name.lower().endswith(".pem"):
                    normalized = _coerce_verification_key(_read_bytes_file(path), parameter_name="trusted_signers")
                    key_id = normalized["key_id"]
                    if key_id in signer_store:
                        raise ValueError(f"duplicate trusted signer key_id: {key_id}")
                    normalized["metadata"] = {
                        "source_path": path,
                        "source_name": name,
                        "revoked": False,
                    }
                    signer_store[key_id] = normalized
            if not signer_store:
                raise ValueError("trust store directory does not contain any .pem public keys")
            return signer_store

        manifest_payload, _, _, validity = _load_trust_store_manifest_payload(
            source,
            verify_key=verify_key,
            trusted_manifest_signers=trusted_manifest_signers,
        )
        if not validity["currently_valid"]:
            raise ValueError(f"trust store manifest is not currently valid: {validity['status']}")
        _enforce_minimum_remaining_validity(
            validity,
            subject_name="trust store manifest",
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        return _build_signer_store_from_manifest_payload(
            manifest_payload,
            os.path.dirname(source),
            include_revoked=include_revoked,
        )
    except ValueError as exc:
        _reraise_policy_exception(exc)


def save_signer_trust_store_manifest(path, manifest_payload, *, sign_key=None, indent=2, sort_keys=False):
    if not isinstance(manifest_payload, dict) or not manifest_payload:
        raise ValueError("manifest_payload must be a non-empty dictionary")
    document = _build_persisted_file_document(
        manifest_payload,
        compression="gzip" if _path_uses_gzip(path) else "none",
        sign_key=sign_key,
    )
    _write_json_file_atomic(path, document, indent=indent, sort_keys=sort_keys)
    return _coerce_filesystem_path(path)


def _load_ed25519_private_key(key_material, *, parameter_name):
    _require_cryptography("Ed25519 signing")
    private_key = serialization.load_pem_private_key(key_material, password=None)
    if not isinstance(private_key, ed25519.Ed25519PrivateKey):
        raise TypeError(f"{parameter_name} PEM must contain an Ed25519 private key")
    return private_key


def _load_ed25519_public_key(key_material, *, parameter_name):
    _require_cryptography("Ed25519 verification")
    try:
        public_key = serialization.load_pem_public_key(key_material)
    except ValueError:
        private_key = _load_ed25519_private_key(key_material, parameter_name=parameter_name)
        return private_key.public_key()
    if not isinstance(public_key, ed25519.Ed25519PublicKey):
        raise TypeError(f"{parameter_name} PEM must contain an Ed25519 public key")
    return public_key


def _coerce_signing_key(sign_key, *, parameter_name):
    if sign_key is None:
        return None
    if _is_ed25519_private_key_instance(sign_key):
        public_key_sha256 = _ed25519_public_key_sha256(sign_key.public_key())
        return {
            "algorithm": "ed25519",
            "private_key": sign_key,
            "public_key_sha256": public_key_sha256,
            "key_id": f"sha256:{public_key_sha256}",
        }

    key_material = _coerce_binary_key_material(sign_key, parameter_name=parameter_name)
    if _looks_like_pem_key(key_material):
        private_key = _load_ed25519_private_key(key_material, parameter_name=parameter_name)
        public_key_sha256 = _ed25519_public_key_sha256(private_key.public_key())
        return {
            "algorithm": "ed25519",
            "private_key": private_key,
            "public_key_sha256": public_key_sha256,
            "key_id": f"sha256:{public_key_sha256}",
        }

    return {
        "algorithm": "hmac-sha256",
        "secret": key_material,
    }


def _coerce_verification_key(verify_key, *, parameter_name):
    if verify_key is None:
        return None
    if _is_ed25519_public_key_instance(verify_key):
        public_key_sha256 = _ed25519_public_key_sha256(verify_key)
        return {
            "algorithm": "ed25519",
            "public_key": verify_key,
            "public_key_sha256": public_key_sha256,
            "key_id": f"sha256:{public_key_sha256}",
        }
    if _is_ed25519_private_key_instance(verify_key):
        public_key = verify_key.public_key()
        public_key_sha256 = _ed25519_public_key_sha256(public_key)
        return {
            "algorithm": "ed25519",
            "public_key": public_key,
            "public_key_sha256": public_key_sha256,
            "key_id": f"sha256:{public_key_sha256}",
        }

    key_material = _coerce_binary_key_material(verify_key, parameter_name=parameter_name)
    if _looks_like_pem_key(key_material):
        public_key = _load_ed25519_public_key(key_material, parameter_name=parameter_name)
        public_key_sha256 = _ed25519_public_key_sha256(public_key)
        return {
            "algorithm": "ed25519",
            "public_key": public_key,
            "public_key_sha256": public_key_sha256,
            "key_id": f"sha256:{public_key_sha256}",
        }

    return {
        "algorithm": "hmac-sha256",
        "secret": key_material,
    }


def _build_persisted_file_document(payload, *, compression, sign_key=None, validity=None):
    canonical_payload = _canonical_json_bytes(payload)
    payload_sha256 = hashlib.sha256(canonical_payload).hexdigest()
    document = {
        "format": "lqft-persisted-file-v1",
        "compression": compression,
        "integrity": {
            "algorithm": "sha256",
            "payload_sha256": payload_sha256,
        },
        "payload": payload,
    }
    if validity is not None:
        _summarize_validity_window(validity, subject_name="persisted file")
        document["validity"] = dict(validity)
    signing_key = _coerce_signing_key(sign_key, parameter_name="sign_key")
    if signing_key is not None and signing_key["algorithm"] == "hmac-sha256":
        document["signature"] = {
            "algorithm": "hmac-sha256",
            "payload_hmac_sha256": hmac.new(
                signing_key["secret"],
                canonical_payload,
                hashlib.sha256,
            ).hexdigest(),
        }
    elif signing_key is not None:
        document["signature"] = {
            "algorithm": "ed25519",
            "key_id": signing_key["key_id"],
            "public_key_sha256": signing_key["public_key_sha256"],
            "payload_ed25519": base64.b64encode(
                signing_key["private_key"].sign(canonical_payload)
            ).decode("ascii"),
        }
    return document


def _summarize_persisted_file_payload(payload):
    if not isinstance(payload, dict):
        return {
            "payload_format": None,
            "payload_type": type(payload).__name__,
        }

    payload_format = payload.get("format")
    summary = {
        "payload_format": payload_format,
        "payload_type": "dict",
    }
    if payload_format == "lqft-snapshot-v1":
        metadata = payload.get("metadata")
        summary.update(
            {
                "snapshot_id": None if not isinstance(metadata, dict) else metadata.get("snapshot_id"),
                "generation": None if not isinstance(metadata, dict) else metadata.get("generation"),
                "size": None if not isinstance(metadata, dict) else metadata.get("size"),
                "item_count": len(payload.get("items", [])) if isinstance(payload.get("items"), list) else None,
            }
        )
    elif payload_format == "lqft-snapshot-bundle-v1":
        metadata = payload.get("metadata")
        snapshots = payload.get("snapshots")
        summary.update(
            {
                "snapshot_count": None if not isinstance(snapshots, list) else len(snapshots),
                "current_snapshot_id": None if not isinstance(metadata, dict) else metadata.get("current_snapshot_id"),
                "exported_at_ns": None if not isinstance(metadata, dict) else metadata.get("exported_at_ns"),
            }
        )
    elif payload_format == "lqft-storage-engine-manifest-v1":
        storage = payload.get("storage")
        checkpoint_policy = payload.get("checkpoint_policy")
        summary.update(
            {
                "checkpoint_path": None if not isinstance(storage, dict) else storage.get("checkpoint_path"),
                "wal_path": None if not isinstance(storage, dict) else storage.get("wal_path"),
                "checkpoint_retain_last": None if not isinstance(storage, dict) else storage.get("checkpoint_retain_last"),
                "verify_key_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("verify_key_path"),
                "trusted_signers_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("trusted_signers_path"),
            }
        )
    elif payload_format == "lqft-sharded-storage-engine-manifest-v1":
        storage = payload.get("storage")
        checkpoint_policy = payload.get("checkpoint_policy")
        summary.update(
            {
                "shard_count": payload.get("shard_count"),
                "checkpoint_dir": None if not isinstance(storage, dict) else storage.get("checkpoint_dir"),
                "wal_dir": None if not isinstance(storage, dict) else storage.get("wal_dir"),
                "checkpoint_retain_last": None if not isinstance(storage, dict) else storage.get("checkpoint_retain_last"),
                "verify_key_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("verify_key_path"),
                "trusted_signers_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("trusted_signers_path"),
            }
        )
    return summary


def _summarize_storage_engine_manifest(payload):
    if not isinstance(payload, dict):
        return {
            "manifest_format": None,
            "manifest_type": type(payload).__name__,
        }

    storage = payload.get("storage")
    checkpoint_policy = payload.get("checkpoint_policy")
    serialization = payload.get("serialization")
    return {
        "manifest_format": payload.get("format"),
        "checkpoint_path": None if not isinstance(storage, dict) else storage.get("checkpoint_path"),
        "wal_path": None if not isinstance(storage, dict) else storage.get("wal_path"),
        "wal_fsync": None if not isinstance(storage, dict) else storage.get("wal_fsync"),
        "checkpoint_retain_last": None if not isinstance(storage, dict) else storage.get("checkpoint_retain_last"),
        "sign_key_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("sign_key_path"),
        "verify_key_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("verify_key_path"),
        "trusted_signers_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("trusted_signers_path"),
        "min_remaining_validity_seconds": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("min_remaining_validity_seconds"),
        "has_validity_policy": bool(isinstance(checkpoint_policy, dict) and checkpoint_policy.get("validity") is not None),
        "indent": None if not isinstance(serialization, dict) else serialization.get("indent"),
        "sort_keys": None if not isinstance(serialization, dict) else serialization.get("sort_keys"),
    }


def _summarize_sharded_storage_engine_manifest(payload):
    if not isinstance(payload, dict):
        return {
            "manifest_format": None,
            "manifest_type": type(payload).__name__,
        }

    storage = payload.get("storage")
    checkpoint_policy = payload.get("checkpoint_policy")
    serialization = payload.get("serialization")
    shards = payload.get("shards")
    return {
        "manifest_format": payload.get("format"),
        "shard_count": payload.get("shard_count"),
        "checkpoint_dir": None if not isinstance(storage, dict) else storage.get("checkpoint_dir"),
        "wal_dir": None if not isinstance(storage, dict) else storage.get("wal_dir"),
        "wal_fsync": None if not isinstance(storage, dict) else storage.get("wal_fsync"),
        "checkpoint_retain_last": None if not isinstance(storage, dict) else storage.get("checkpoint_retain_last"),
        "sign_key_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("sign_key_path"),
        "verify_key_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("verify_key_path"),
        "trusted_signers_path": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("trusted_signers_path"),
        "min_remaining_validity_seconds": None if not isinstance(checkpoint_policy, dict) else checkpoint_policy.get("min_remaining_validity_seconds"),
        "has_validity_policy": bool(isinstance(checkpoint_policy, dict) and checkpoint_policy.get("validity") is not None),
        "indent": None if not isinstance(serialization, dict) else serialization.get("indent"),
        "sort_keys": None if not isinstance(serialization, dict) else serialization.get("sort_keys"),
        "shard_manifest_count": None if not isinstance(shards, list) else len(shards),
    }


def _summarize_persisted_file_signature(document):
    signature = document.get("signature") if isinstance(document, dict) else None
    if not isinstance(signature, dict):
        return False, None
    summary = {
        key: value
        for key, value in signature.items()
        if key not in {"payload_hmac_sha256", "payload_ed25519"}
    }
    summary["algorithm"] = signature.get("algorithm")
    return True, summary


def _classify_policy_error(load_error):
    if load_error is None:
        return None
    if "checksum mismatch" in load_error:
        return {"code": "integrity_checksum_mismatch", "component": "integrity"}
    if "trusted_signers string inputs must point to an existing trust-store path" in load_error:
        return {"code": "source_missing_trusted_signers_path", "component": "source"}
    if "trust store directory does not contain any .pem public keys" in load_error:
        return {"code": "source_empty_trust_store_directory", "component": "source"}
    if "trust store manifest validity" in load_error:
        return {"code": "schema_invalid_trust_store_validity", "component": "schema"}
    if "trust store manifest must decode to a dictionary" in load_error:
        return {"code": "schema_invalid_trust_store_manifest", "component": "schema"}
    if "trust store manifest does not contain any active signers" in load_error:
        return {"code": "schema_empty_trust_store_manifest", "component": "schema"}
    if "trusted_signers must be a mapping or iterable of Ed25519 verification keys" in load_error:
        return {"code": "schema_invalid_trusted_signers", "component": "schema"}
    if "trusted_signers keys must be non-empty strings" in load_error:
        return {"code": "schema_invalid_trusted_signer_key_id", "component": "schema"}
    if "duplicate trusted signer key_id" in load_error:
        return {"code": "schema_duplicate_trusted_signer_key_id", "component": "schema"}
    if "trusted_signers only supports Ed25519 verification keys" in load_error:
        return {"code": "verification_trusted_signers_non_ed25519", "component": "verification"}
    if "trusted signer key_id does not match supplied verification key" in load_error:
        return {"code": "verification_trusted_signer_identity_mismatch", "component": "verification"}
    if "min_remaining_validity_seconds must be a non-negative integer or null" in load_error:
        return {"code": "freshness_invalid_min_remaining_validity", "component": "freshness"}
    if (
        "trust store manifest signers" in load_error
        or "trust store manifest signer" in load_error
        or "trust store manifest keys must" in load_error
        or "trust store manifest values must" in load_error
        or "trust store manifest must be a non-empty mapping" in load_error
    ):
        return {"code": "schema_invalid_trust_store_manifest", "component": "schema"}
    if "storage engine manifest" in load_error:
        return {"code": "schema_invalid_storage_engine_manifest", "component": "schema"}
    if "must decode to a dictionary payload" in load_error:
        return {"code": "schema_invalid_document", "component": "schema"}
    if "persisted file validity" in load_error:
        return {"code": "schema_invalid_validity_block", "component": "schema"}
    if "payload must be a dictionary" in load_error:
        return {"code": "schema_invalid_payload", "component": "schema"}
    if "integrity block must be a dictionary" in load_error or "integrity block is invalid" in load_error:
        return {"code": "schema_invalid_integrity_block", "component": "schema"}
    if "signature block must be a dictionary" in load_error or "signature block is invalid" in load_error:
        return {"code": "schema_invalid_signature_block", "component": "schema"}
    if "do not support manifest signature verification" in load_error:
        return {"code": "verification_unsupported_for_directory_trust_store", "component": "verification"}
    if "does not support trusted_signers" in load_error:
        return {"code": "verification_trusted_signers_unsupported", "component": "verification"}
    if "signer is untrusted" in load_error or "untrusted" in load_error:
        return {"code": "signer_untrusted", "component": "signer_trust"}
    if "identity mismatch" in load_error:
        return {"code": "verification_signer_identity_mismatch", "component": "verification"}
    if "signature mismatch" in load_error:
        return {"code": "verification_signature_mismatch", "component": "verification"}
    if "signature algorithm does not match verify_key" in load_error:
        return {"code": "verification_algorithm_mismatch", "component": "verification"}
    if "unsigned" in load_error:
        return {"code": "verification_unsigned", "component": "verification"}
    if "does not declare expires_at required by min_remaining_validity_seconds" in load_error:
        return {"code": "freshness_missing_expires_at", "component": "freshness"}
    if "remaining validity is below min_remaining_validity_seconds" in load_error:
        return {"code": "freshness_below_min_remaining_validity", "component": "freshness"}
    if "not currently valid" in load_error:
        return {"code": "freshness_not_currently_valid", "component": "freshness"}
    if "verify_key" in load_error:
        return {"code": "verification_configuration_error", "component": "verification"}
    return {"code": "policy_error", "component": "policy"}


def classify_policy_error(load_error):
    classification = _classify_policy_error(load_error)
    if classification is None:
        return None
    result = dict(classification)
    result["is_fallback"] = result["code"] == "policy_error"
    return result


def _reraise_policy_exception(error):
    if isinstance(error, LQFTPolicyError):
        raise error
    classification = classify_policy_error(str(error))
    if classification is None:
        raise error
    raise LQFTPolicyError(
        str(error),
        code=classification["code"],
        component=classification["component"],
        is_fallback=classification["is_fallback"],
    ) from error


def _build_effective_policy_summary(
    *,
    verification_mode,
    signer_trust_required,
    freshness_required,
    min_remaining_validity_seconds,
    validity,
    load_error,
):
    verification_required = verification_mode != "none"
    classification = classify_policy_error(load_error)
    error_code = None if classification is None else classification["code"]
    component = None if classification is None else classification["component"]
    is_fallback = False if classification is None else classification["is_fallback"]
    if load_error is None:
        verification_passed = True if verification_required else None
        signer_trust_passed = True if signer_trust_required else None
        freshness_passed = True if freshness_required else None
        verification_code = None
        signer_trust_code = None
        freshness_code = None
    else:
        freshness_error = component == "freshness"
        signer_trust_error = component == "signer_trust"
        verification_error = component in {"verification", "signer_trust"}
        verification_passed = None if not verification_required else not verification_error
        signer_trust_passed = None if not signer_trust_required else not signer_trust_error
        freshness_passed = None if not freshness_required else not freshness_error
        verification_code = error_code if verification_error and verification_required else None
        signer_trust_code = error_code if signer_trust_error and signer_trust_required else None
        freshness_code = error_code if freshness_error and freshness_required else None

    return {
        "status": "pass" if load_error is None else "fail",
        "allowed": load_error is None,
        "reason": load_error,
        "reason_code": error_code,
        "reason_code_is_fallback": is_fallback,
        "verification": {
            "required": verification_required,
            "mode": verification_mode,
            "passed": verification_passed,
            "code": verification_code,
        },
        "signer_trust": {
            "required": signer_trust_required,
            "passed": signer_trust_passed,
            "code": signer_trust_code,
        },
        "freshness": {
            "required": freshness_required,
            "passed": freshness_passed,
            "min_remaining_validity_seconds": min_remaining_validity_seconds,
            "current_status": validity.get("status"),
            "code": freshness_code,
        },
    }


def inspect_persisted_file(path, *, verify_key=None, trusted_signers=None, min_remaining_validity_seconds=None):
    path = _coerce_filesystem_path(path)
    document = _read_json_file(path)
    signed, signature = _summarize_persisted_file_signature(document)

    if isinstance(document, dict) and document.get("format") == "lqft-persisted-file-v1":
        payload = document.get("payload")
        try:
            validity = _summarize_validity_window(
                document.get("validity"),
                subject_name="persisted file",
            )
        except ValueError:
            validity = {
                "not_before": None,
                "expires_at": None,
                "warn_before_expiry_seconds": None,
                "seconds_until_expiry": None,
                "status": "invalid",
                "currently_valid": False,
            }
        integrity = document.get("integrity")
        payload_sha256 = None if not isinstance(integrity, dict) else integrity.get("payload_sha256")
        integrity_verified = False
        if isinstance(payload, dict) and isinstance(payload_sha256, str) and payload_sha256:
            integrity_verified = hashlib.sha256(_canonical_json_bytes(payload)).hexdigest() == payload_sha256

        inspection = {
            "source": path,
            "source_type": "persisted-file",
            "compression": document.get("compression"),
            "signed": signed,
            "signature": signature,
            "integrity": {
                "algorithm": None if not isinstance(integrity, dict) else integrity.get("algorithm"),
                "payload_sha256": payload_sha256,
                "verified": integrity_verified,
            },
            "validity": validity,
            "payload": _summarize_persisted_file_payload(payload),
        }
    else:
        inspection = {
            "source": path,
            "source_type": "raw-payload",
            "compression": "none",
            "signed": False,
            "signature": None,
            "integrity": {
                "algorithm": None,
                "payload_sha256": None,
                "verified": False,
            },
            "validity": _summarize_validity_window(None, subject_name="persisted file"),
            "payload": _summarize_persisted_file_payload(document),
        }

    load_error = None
    try:
        _extract_payload_from_file_document(
            document,
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
    except (TypeError, ValueError) as exc:
        load_error = str(exc)
    classification = classify_policy_error(load_error)

    inspection["policy_check"] = {
        "verification_requested": verify_key is not None or trusted_signers is not None,
        "min_remaining_validity_seconds": min_remaining_validity_seconds,
        "loadable": load_error is None,
        "error": load_error,
        "error_code": None if classification is None else classification["code"],
        "error_code_is_fallback": False if classification is None else classification["is_fallback"],
    }
    inspection["effective_policy"] = _build_effective_policy_summary(
        verification_mode=(
            "trusted-signers"
            if trusted_signers is not None
            else ("direct-key" if verify_key is not None else "none")
        ),
        signer_trust_required=trusted_signers is not None,
        freshness_required=(
            min_remaining_validity_seconds is not None
            or inspection["validity"]["not_before"] is not None
            or inspection["validity"]["expires_at"] is not None
        ),
        min_remaining_validity_seconds=min_remaining_validity_seconds,
        validity=inspection["validity"],
        load_error=load_error,
    )
    return inspection


def inspect_storage_engine_manifest(path, *, verify_key=None, trusted_signers=None, min_remaining_validity_seconds=None):
    path = _coerce_filesystem_path(path)
    document = _read_json_file(path)
    signed, signature = _summarize_persisted_file_signature(document)

    payload = None
    if isinstance(document, dict) and document.get("format") == "lqft-persisted-file-v1":
        payload = document.get("payload")
        try:
            validity = _summarize_validity_window(
                document.get("validity"),
                subject_name="persisted file",
            )
        except ValueError:
            validity = _invalid_validity_summary()
    else:
        validity = _summarize_validity_window(None, subject_name="persisted file")

    load_error = None
    try:
        extracted_payload = _extract_payload_from_file_document(
            document,
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        LQFTStorageEngine.from_manifest(extracted_payload)
        payload = extracted_payload
    except (TypeError, ValueError, LQFTPolicyError) as exc:
        load_error = str(exc)

    classification = classify_policy_error(load_error)
    policy_check = {
        "verification_requested": verify_key is not None or trusted_signers is not None,
        "min_remaining_validity_seconds": min_remaining_validity_seconds,
        "loadable": load_error is None,
        "error": load_error,
        "error_code": None if classification is None else classification["code"],
        "error_code_is_fallback": False if classification is None else classification["is_fallback"],
    }
    return {
        "path": path,
        "source_type": "storage-engine-manifest",
        "signed": signed,
        "signature": signature,
        "validity": validity,
        "manifest": _summarize_storage_engine_manifest(payload),
        "payload": _summarize_persisted_file_payload(payload),
        "policy_check": policy_check,
        "effective_policy": _build_effective_policy_summary(
            verification_mode=(
                "trusted-signers"
                if trusted_signers is not None
                else ("direct-key" if verify_key is not None else "none")
            ),
            signer_trust_required=trusted_signers is not None,
            freshness_required=(
                min_remaining_validity_seconds is not None
                or validity["not_before"] is not None
                or validity["expires_at"] is not None
            ),
            min_remaining_validity_seconds=min_remaining_validity_seconds,
            validity=validity,
            load_error=load_error,
        ),
    }


def inspect_sharded_storage_engine_manifest(path, *, verify_key=None, trusted_signers=None, min_remaining_validity_seconds=None):
    path = _coerce_filesystem_path(path)
    document = _read_json_file(path)
    signed, signature = _summarize_persisted_file_signature(document)

    payload = None
    if isinstance(document, dict) and document.get("format") == "lqft-persisted-file-v1":
        payload = document.get("payload")
        try:
            validity = _summarize_validity_window(
                document.get("validity"),
                subject_name="persisted file",
            )
        except ValueError:
            validity = _invalid_validity_summary()
    else:
        validity = _summarize_validity_window(None, subject_name="persisted file")

    load_error = None
    try:
        extracted_payload = _extract_payload_from_file_document(
            document,
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        LQFTShardedStorageEngine.from_manifest(extracted_payload)
        payload = extracted_payload
    except (TypeError, ValueError, LQFTPolicyError) as exc:
        load_error = str(exc)

    classification = classify_policy_error(load_error)
    policy_check = {
        "verification_requested": verify_key is not None or trusted_signers is not None,
        "min_remaining_validity_seconds": min_remaining_validity_seconds,
        "loadable": load_error is None,
        "error": load_error,
        "error_code": None if classification is None else classification["code"],
        "error_code_is_fallback": False if classification is None else classification["is_fallback"],
    }
    return {
        "path": path,
        "source_type": "sharded-storage-engine-manifest",
        "signed": signed,
        "signature": signature,
        "validity": validity,
        "manifest": _summarize_sharded_storage_engine_manifest(payload),
        "payload": _summarize_persisted_file_payload(payload),
        "policy_check": policy_check,
        "effective_policy": _build_effective_policy_summary(
            verification_mode=(
                "trusted-signers"
                if trusted_signers is not None
                else ("direct-key" if verify_key is not None else "none")
            ),
            signer_trust_required=trusted_signers is not None,
            freshness_required=(
                min_remaining_validity_seconds is not None
                or validity["not_before"] is not None
                or validity["expires_at"] is not None
            ),
            min_remaining_validity_seconds=min_remaining_validity_seconds,
            validity=validity,
            load_error=load_error,
        ),
    }


def _extract_payload_from_file_document(document, *, verify_key=None, trusted_signers=None, min_remaining_validity_seconds=None):
    if not isinstance(document, dict):
        raise TypeError("persisted file must decode to a dictionary payload")

    try:
        min_remaining_validity_seconds = _validate_minimum_remaining_validity_seconds(
            min_remaining_validity_seconds
        )
        if document.get("format") == "lqft-persisted-file-v1":
            payload = document.get("payload")
            integrity = document.get("integrity")
            validity = _summarize_validity_window(
                document.get("validity"),
                subject_name="persisted file",
            )
            if not isinstance(payload, dict):
                raise ValueError("persisted file payload must be a dictionary")
            if not isinstance(integrity, dict):
                raise ValueError("persisted file integrity block must be a dictionary")

            algorithm = integrity.get("algorithm")
            payload_sha256 = integrity.get("payload_sha256")
            if algorithm != "sha256" or not isinstance(payload_sha256, str) or not payload_sha256:
                raise ValueError("persisted file integrity block is invalid")

            canonical_payload = _canonical_json_bytes(payload)
            actual_sha256 = hashlib.sha256(canonical_payload).hexdigest()
            if actual_sha256 != payload_sha256:
                raise ValueError("persisted file checksum mismatch")

            signature = document.get("signature")
            verification_key = _coerce_verification_key(verify_key, parameter_name="verify_key")
            trusted_signer_store = build_signer_trust_store(trusted_signers)
            if signature is not None:
                if not isinstance(signature, dict):
                    raise ValueError("persisted file signature block must be a dictionary")
                algorithm = signature.get("algorithm")
                if algorithm == "hmac-sha256":
                    payload_hmac_sha256 = signature.get("payload_hmac_sha256")
                    if not isinstance(payload_hmac_sha256, str) or not payload_hmac_sha256:
                        raise ValueError("persisted file signature block is invalid")
                    if verification_key is not None:
                        if verification_key["algorithm"] != "hmac-sha256":
                            raise ValueError("persisted file signature algorithm does not match verify_key")
                        expected_hmac = hmac.new(
                            verification_key["secret"],
                            canonical_payload,
                            hashlib.sha256,
                        ).hexdigest()
                        if not hmac.compare_digest(expected_hmac, payload_hmac_sha256):
                            raise ValueError("persisted file signature mismatch")
                    elif trusted_signer_store is not None:
                        raise ValueError("persisted file signature algorithm does not support trusted_signers")
                elif algorithm == "ed25519":
                    key_id = signature.get("key_id")
                    public_key_sha256 = signature.get("public_key_sha256")
                    payload_ed25519 = signature.get("payload_ed25519")
                    if (
                        not isinstance(key_id, str)
                        or not key_id
                        or not isinstance(public_key_sha256, str)
                        or not public_key_sha256
                        or not isinstance(payload_ed25519, str)
                        or not payload_ed25519
                        or key_id != f"sha256:{public_key_sha256}"
                    ):
                        raise ValueError("persisted file signature block is invalid")
                    selected_verifier = verification_key
                    if selected_verifier is None and trusted_signer_store is not None:
                        selected_verifier = trusted_signer_store.get(key_id)
                        if selected_verifier is None:
                            raise ValueError("persisted file signer is untrusted")

                    if selected_verifier is not None:
                        if selected_verifier["algorithm"] != "ed25519":
                            raise ValueError("persisted file signature algorithm does not match verify_key")
                        if selected_verifier["public_key_sha256"] != public_key_sha256:
                            raise ValueError("persisted file signer identity mismatch")
                        try:
                            signature_bytes = base64.b64decode(payload_ed25519.encode("ascii"), validate=True)
                        except (ValueError, TypeError) as exc:
                            raise ValueError("persisted file signature block is invalid") from exc
                        try:
                            selected_verifier["public_key"].verify(signature_bytes, canonical_payload)
                        except InvalidSignature as exc:
                            raise ValueError("persisted file signature mismatch") from exc
                else:
                    raise ValueError("persisted file signature block is invalid")
            elif verification_key is not None or trusted_signer_store is not None:
                raise ValueError("persisted file is unsigned")

            if not validity["currently_valid"]:
                raise ValueError(f"persisted file is not currently valid: {validity['status']}")
            _enforce_minimum_remaining_validity(
                validity,
                subject_name="persisted file",
                min_remaining_validity_seconds=min_remaining_validity_seconds,
            )

            return payload

        _enforce_minimum_remaining_validity(
            _summarize_validity_window(None, subject_name="persisted file"),
            subject_name="persisted file",
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        return document
    except ValueError as exc:
        _reraise_policy_exception(exc)
    return document


def _build_snapshot_bundle_payload(snapshots, current_snapshot_id=None):
    return {
        "format": "lqft-snapshot-bundle-v1",
        "metadata": {
            "snapshot_count": len(snapshots),
            "current_snapshot_id": current_snapshot_id,
            "exported_at_ns": time.time_ns(),
        },
        "snapshots": snapshots,
    }


def _write_json_file_atomic(path, payload, *, indent=2, sort_keys=False):
    path = _coerce_filesystem_path(path)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    temp_path = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    serialized_payload = json.dumps(payload, indent=indent, sort_keys=sort_keys)
    try:
        if _path_uses_gzip(path):
            compressed_payload = gzip.compress((serialized_payload + "\n").encode("utf-8"))
            with open(temp_path, "wb") as handle:
                handle.write(compressed_payload)
                handle.flush()
                os.fsync(handle.fileno())
        else:
            with open(temp_path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(serialized_payload)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def _read_json_file(path):
    path = _coerce_filesystem_path(path)
    if _path_uses_gzip(path):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _truncate_file(path, size):
    path = _coerce_filesystem_path(path)
    with open(path, "r+b") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())


def _append_jsonl_record(path, payload, *, fsync=True):
    path = _coerce_filesystem_path(path)
    if _path_uses_gzip(path):
        raise ValueError("write-ahead log paths must not use gzip")

    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    with open(path, "ab") as handle:
        handle.write(encoded)
        handle.flush()
        if fsync:
            os.fsync(handle.fileno())


def _read_jsonl_records(path, *, truncate_incomplete_tail=False):
    path = _coerce_filesystem_path(path)
    with open(path, "rb") as handle:
        payload = handle.read()

    records = []
    good_end = 0
    offset = 0
    lines = payload.splitlines(keepends=True)
    for index, line in enumerate(lines):
        offset += len(line)
        if not line.strip():
            good_end = offset
            continue

        if not line.endswith((b"\n", b"\r")):
            if truncate_incomplete_tail and index == len(lines) - 1:
                _truncate_file(path, good_end)
                return records
            raise ValueError("write-ahead log ends with a partial record")

        try:
            record = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            if truncate_incomplete_tail and index == len(lines) - 1:
                _truncate_file(path, good_end)
                return records
            raise ValueError("write-ahead log contains an invalid record") from exc

        if not isinstance(record, dict) or record.get("format") != "lqft-wal-v1":
            raise ValueError("write-ahead log contains an unsupported record format")

        records.append(record)
        good_end = offset

    return records


def _build_keygen_arg_parser():
    parser = argparse.ArgumentParser(description="LQFT utility commands")
    subparsers = parser.add_subparsers(dest="command")

    keygen_parser = subparsers.add_parser(
        "generate-ed25519-keypair",
        help="Generate an Ed25519 keypair for persisted snapshot signing",
    )
    keygen_parser.add_argument("--private-out", required=True, help="Path to write the private PEM key")
    keygen_parser.add_argument("--public-out", required=True, help="Path to write the public PEM key")
    keygen_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output files if they already exist",
    )

    save_storage_engine_manifest_parser = subparsers.add_parser(
        "save-storage-engine-manifest",
        help="Write a storage engine manifest from explicit checkpoint and WAL configuration",
    )
    save_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to write the storage engine manifest file",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-path",
        help="Path to the persisted checkpoint bundle used by the storage engine",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--wal-path",
        help="Path to the write-ahead log used by the storage engine",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--wal-no-fsync",
        action="store_true",
        help="Disable fsync on WAL appends in the stored engine configuration",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-retain-last",
        type=int,
        help="Retain at most this many snapshots during checkpoint rotation",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-sign-key-file",
        help="Path to the checkpoint signing key file to store as a path-based preset",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-verify-key-file",
        help="Path to the checkpoint verification key file to store as a path-based preset",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-trusted-signers",
        help="Path to a trusted-signers store used to verify checkpoint bundles",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining checkpoint validity during recovery",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-expires-at",
        help="Persist this expires_at value in the checkpoint validity policy",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-not-before",
        help="Persist this not_before value in the checkpoint validity policy",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-warn-before-expiry-seconds",
        type=int,
        help="Persist this warn_before_expiry_seconds value in the checkpoint validity policy",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-indent",
        type=int,
        default=2,
        help="Indent level to use when checkpoint bundles are written",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--checkpoint-sort-keys",
        action="store_true",
        help="Sort JSON keys when checkpoint bundles are written",
    )
    manifest_sign_group = save_storage_engine_manifest_parser.add_mutually_exclusive_group()
    manifest_sign_group.add_argument(
        "--manifest-sign-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM key used to sign the storage engine manifest",
    )
    manifest_sign_group.add_argument(
        "--manifest-sign-key-text",
        help="Literal HMAC secret text used to sign the storage engine manifest",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--manifest-expires-at",
        help="Persist this expires_at value in the manifest validity window",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--manifest-not-before",
        help="Persist this not_before value in the manifest validity window",
    )
    save_storage_engine_manifest_parser.add_argument(
        "--manifest-warn-before-expiry-seconds",
        type=int,
        help="Persist this warn_before_expiry_seconds value in the manifest validity window",
    )

    save_sharded_storage_engine_manifest_parser = subparsers.add_parser(
        "save-sharded-storage-engine-manifest",
        help="Write a sharded storage engine manifest from explicit checkpoint and WAL directory configuration",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to write the sharded storage engine manifest file",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--shard-count",
        type=int,
        required=True,
        help="Shard count for the sharded storage engine",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-dir",
        help="Directory containing per-shard checkpoint bundles",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--wal-dir",
        help="Directory containing per-shard write-ahead logs",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--wal-no-fsync",
        action="store_true",
        help="Disable fsync on WAL appends in the stored sharded engine configuration",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-retain-last",
        type=int,
        help="Retain at most this many snapshots during checkpoint rotation",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-sign-key-file",
        help="Path to the checkpoint signing key file to store as a path-based preset",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-verify-key-file",
        help="Path to the checkpoint verification key file to store as a path-based preset",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-trusted-signers",
        help="Path to a trusted-signers store used to verify checkpoint bundles",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining checkpoint validity during recovery",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-expires-at",
        help="Persist this expires_at value in the checkpoint validity policy",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-not-before",
        help="Persist this not_before value in the checkpoint validity policy",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-warn-before-expiry-seconds",
        type=int,
        help="Persist this warn_before_expiry_seconds value in the checkpoint validity policy",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-indent",
        type=int,
        default=2,
        help="Indent level to use when checkpoint bundles are written",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--checkpoint-sort-keys",
        action="store_true",
        help="Sort JSON keys when checkpoint bundles are written",
    )
    sharded_manifest_sign_group = save_sharded_storage_engine_manifest_parser.add_mutually_exclusive_group()
    sharded_manifest_sign_group.add_argument(
        "--manifest-sign-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM key used to sign the sharded storage engine manifest",
    )
    sharded_manifest_sign_group.add_argument(
        "--manifest-sign-key-text",
        help="Literal HMAC secret text used to sign the sharded storage engine manifest",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--manifest-expires-at",
        help="Persist this expires_at value in the manifest validity window",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--manifest-not-before",
        help="Persist this not_before value in the manifest validity window",
    )
    save_sharded_storage_engine_manifest_parser.add_argument(
        "--manifest-warn-before-expiry-seconds",
        type=int,
        help="Persist this warn_before_expiry_seconds value in the manifest validity window",
    )

    recover_storage_engine_manifest_parser = subparsers.add_parser(
        "recover-storage-engine-manifest",
        help="Recover map state from a storage engine manifest",
    )
    recover_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to the storage engine manifest file",
    )
    recover_manifest_verify_group = recover_storage_engine_manifest_parser.add_mutually_exclusive_group()
    recover_manifest_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the signed storage engine manifest",
    )
    recover_manifest_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the signed storage engine manifest",
    )
    recover_storage_engine_manifest_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 storage-engine manifest verification",
    )
    recover_storage_engine_manifest_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the storage engine manifest to load",
    )
    recover_storage_engine_manifest_parser.add_argument(
        "--truncate-incomplete-wal-tail",
        action="store_true",
        help="Truncate an incomplete final WAL record during recovery instead of failing",
    )
    recover_storage_engine_manifest_parser.add_argument(
        "--migration-threshold",
        type=int,
        default=50000,
        help="Migration threshold to use for the recovered map instance",
    )
    recover_storage_engine_manifest_parser.add_argument(
        "--include-current-state-payload",
        action="store_true",
        help="Include a non-mutating exported current-state payload in the recovery JSON output",
    )

    recover_sharded_storage_engine_manifest_parser = subparsers.add_parser(
        "recover-sharded-storage-engine-manifest",
        help="Recover sharded map state from a sharded storage engine manifest",
    )
    recover_sharded_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to the sharded storage engine manifest file",
    )
    recover_sharded_manifest_verify_group = recover_sharded_storage_engine_manifest_parser.add_mutually_exclusive_group()
    recover_sharded_manifest_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the signed sharded storage engine manifest",
    )
    recover_sharded_manifest_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the signed sharded storage engine manifest",
    )
    recover_sharded_storage_engine_manifest_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 sharded storage-engine manifest verification",
    )
    recover_sharded_storage_engine_manifest_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the sharded storage engine manifest to load",
    )
    recover_sharded_storage_engine_manifest_parser.add_argument(
        "--truncate-incomplete-wal-tail",
        action="store_true",
        help="Truncate an incomplete final WAL record during recovery instead of failing",
    )
    recover_sharded_storage_engine_manifest_parser.add_argument(
        "--migration-threshold",
        type=int,
        default=50000,
        help="Migration threshold to use for recovered shard map instances",
    )
    recover_sharded_storage_engine_manifest_parser.add_argument(
        "--include-current-state-payload",
        action="store_true",
        help="Include a non-mutating exported current-state payload in the recovery JSON output",
    )

    checkpoint_storage_engine_manifest_parser = subparsers.add_parser(
        "checkpoint-storage-engine-manifest",
        help="Recover from a storage engine manifest and run checkpoint rotation",
    )
    checkpoint_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to the storage engine manifest file",
    )
    checkpoint_manifest_verify_group = checkpoint_storage_engine_manifest_parser.add_mutually_exclusive_group()
    checkpoint_manifest_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the signed storage engine manifest",
    )
    checkpoint_manifest_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the signed storage engine manifest",
    )
    checkpoint_storage_engine_manifest_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 storage-engine manifest verification",
    )
    checkpoint_storage_engine_manifest_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the storage engine manifest to load",
    )
    checkpoint_storage_engine_manifest_parser.add_argument(
        "--truncate-incomplete-wal-tail",
        action="store_true",
        help="Truncate an incomplete final WAL record during recovery instead of failing",
    )
    checkpoint_storage_engine_manifest_parser.add_argument(
        "--migration-threshold",
        type=int,
        default=50000,
        help="Migration threshold to use for the recovered map instance",
    )
    checkpoint_storage_engine_manifest_parser.add_argument(
        "--retain-last",
        type=int,
        help="Override the manifest checkpoint retention policy for this checkpoint rotation",
    )

    checkpoint_sharded_storage_engine_manifest_parser = subparsers.add_parser(
        "checkpoint-sharded-storage-engine-manifest",
        help="Recover from a sharded storage engine manifest and run per-shard checkpoint rotation",
    )
    checkpoint_sharded_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to the sharded storage engine manifest file",
    )
    checkpoint_sharded_manifest_verify_group = checkpoint_sharded_storage_engine_manifest_parser.add_mutually_exclusive_group()
    checkpoint_sharded_manifest_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the signed sharded storage engine manifest",
    )
    checkpoint_sharded_manifest_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the signed sharded storage engine manifest",
    )
    checkpoint_sharded_storage_engine_manifest_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 sharded storage-engine manifest verification",
    )
    checkpoint_sharded_storage_engine_manifest_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the sharded storage engine manifest to load",
    )
    checkpoint_sharded_storage_engine_manifest_parser.add_argument(
        "--truncate-incomplete-wal-tail",
        action="store_true",
        help="Truncate an incomplete final WAL record during recovery instead of failing",
    )
    checkpoint_sharded_storage_engine_manifest_parser.add_argument(
        "--migration-threshold",
        type=int,
        default=50000,
        help="Migration threshold to use for recovered shard map instances",
    )
    checkpoint_sharded_storage_engine_manifest_parser.add_argument(
        "--retain-last",
        type=int,
        help="Override the manifest checkpoint retention policy for this checkpoint rotation",
    )

    inspect_persisted_parser = subparsers.add_parser(
        "inspect-persisted-file",
        help="Inspect a persisted snapshot or bundle file without loading it into a map",
    )
    inspect_persisted_parser.add_argument("path", help="Path to the persisted snapshot or bundle file")
    persisted_verify_group = inspect_persisted_parser.add_mutually_exclusive_group()
    persisted_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the persisted file signature",
    )
    persisted_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the persisted file signature",
    )
    inspect_persisted_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 producer allowlist verification",
    )
    inspect_persisted_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the persisted file to pass policy_check",
    )
    inspect_persisted_parser.add_argument(
        "--fail-on-policy-fail",
        action="store_true",
        help="Exit with a category-specific non-zero status when effective_policy would reject the persisted file",
    )

    inspect_trust_store_parser = subparsers.add_parser(
        "inspect-signer-trust-store",
        help="Inspect a signer trust store directory or manifest without loading it for production use",
    )
    inspect_trust_store_parser.add_argument("path", help="Path to the signer trust store directory or manifest")
    trust_verify_group = inspect_trust_store_parser.add_mutually_exclusive_group()
    trust_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify a signed trust-store manifest",
    )
    trust_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify a signed trust-store manifest",
    )
    inspect_trust_store_parser.add_argument(
        "--trusted-manifest-signers",
        help="Path to a trust store used to verify signed trust-store manifests by signer allowlist",
    )
    inspect_trust_store_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the manifest to pass policy_check",
    )
    inspect_trust_store_parser.add_argument(
        "--fail-on-policy-fail",
        action="store_true",
        help="Exit with a category-specific non-zero status when effective_policy would reject the trust store policy",
    )

    inspect_storage_engine_manifest_parser = subparsers.add_parser(
        "inspect-storage-engine-manifest",
        help="Inspect a storage engine manifest without performing checkpoint recovery",
    )
    inspect_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to the storage engine manifest file",
    )
    storage_manifest_verify_group = inspect_storage_engine_manifest_parser.add_mutually_exclusive_group()
    storage_manifest_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the signed storage engine manifest",
    )
    storage_manifest_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the signed storage engine manifest",
    )
    inspect_storage_engine_manifest_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 storage-engine manifest verification",
    )
    inspect_storage_engine_manifest_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the storage engine manifest to pass policy_check",
    )
    inspect_storage_engine_manifest_parser.add_argument(
        "--fail-on-policy-fail",
        action="store_true",
        help="Exit with a category-specific non-zero status when effective_policy would reject the storage engine manifest",
    )

    inspect_sharded_storage_engine_manifest_parser = subparsers.add_parser(
        "inspect-sharded-storage-engine-manifest",
        help="Inspect a sharded storage engine manifest without performing checkpoint recovery",
    )
    inspect_sharded_storage_engine_manifest_parser.add_argument(
        "path",
        help="Path to the sharded storage engine manifest file",
    )
    sharded_storage_manifest_verify_group = inspect_sharded_storage_engine_manifest_parser.add_mutually_exclusive_group()
    sharded_storage_manifest_verify_group.add_argument(
        "--verify-key-file",
        help="Path to an HMAC secret file or Ed25519 PEM file used to verify the signed sharded storage engine manifest",
    )
    sharded_storage_manifest_verify_group.add_argument(
        "--verify-key-text",
        help="Literal HMAC secret text used to verify the signed sharded storage engine manifest",
    )
    inspect_sharded_storage_engine_manifest_parser.add_argument(
        "--trusted-signers",
        help="Path to a signer trust store for Ed25519 sharded storage-engine manifest verification",
    )
    inspect_sharded_storage_engine_manifest_parser.add_argument(
        "--min-remaining-validity-seconds",
        type=int,
        help="Require at least this many seconds of remaining validity for the sharded storage engine manifest to pass policy_check",
    )
    inspect_sharded_storage_engine_manifest_parser.add_argument(
        "--fail-on-policy-fail",
        action="store_true",
        help="Exit with a category-specific non-zero status when effective_policy would reject the sharded storage engine manifest",
    )

    return parser


def _resolve_cli_key_argument(file_value, text_value):
    if file_value is not None:
        return _read_bytes_file(file_value)
    return text_value


def _build_cli_validity_argument(*, not_before, expires_at, warn_before_expiry_seconds):
    validity = {}
    if not_before is not None:
        validity["not_before"] = not_before
    if expires_at is not None:
        validity["expires_at"] = expires_at
    if warn_before_expiry_seconds is not None:
        validity["warn_before_expiry_seconds"] = int(warn_before_expiry_seconds)
    return validity or None


def _policy_failure_exit_code(reason_code):
    if isinstance(reason_code, str) and reason_code.startswith("source_"):
        return 17
    if isinstance(reason_code, str) and reason_code.startswith("integrity_"):
        return 15
    if isinstance(reason_code, str) and reason_code.startswith("schema_"):
        return 16
    if reason_code in {
        "verification_unsupported_for_directory_trust_store",
        "verification_trusted_signers_unsupported",
        "verification_configuration_error",
    }:
        return 13
    if isinstance(reason_code, str) and reason_code.startswith("freshness_"):
        return 10
    if reason_code == "signer_untrusted":
        return 11
    if isinstance(reason_code, str) and reason_code.startswith("verification_"):
        return 12
    return 14


def _effective_policy_exit_code(result):
    effective_policy = result.get("effective_policy", {})
    if bool(effective_policy.get("allowed", False)):
        return 0
    reason_code = effective_policy.get("reason_code")
    if reason_code is None:
        reason_code = result.get("policy_check", {}).get("error_code")
    return _policy_failure_exit_code(reason_code)


def _main(argv=None):
    parser = _build_keygen_arg_parser()
    args = parser.parse_args(argv)
    if args.command == "generate-ed25519-keypair":
        result = write_ed25519_keypair(
            args.private_out,
            args.public_out,
            overwrite=args.force,
        )
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "save-storage-engine-manifest":
        storage = LQFTStorageEngine(
            checkpoint_path=args.checkpoint_path,
            wal_path=args.wal_path,
            wal_fsync=not args.wal_no_fsync,
            checkpoint_retain_last=args.checkpoint_retain_last,
            checkpoint_sign_key=args.checkpoint_sign_key_file,
            checkpoint_validity=_build_cli_validity_argument(
                not_before=args.checkpoint_not_before,
                expires_at=args.checkpoint_expires_at,
                warn_before_expiry_seconds=args.checkpoint_warn_before_expiry_seconds,
            ),
            checkpoint_verify_key=args.checkpoint_verify_key_file,
            checkpoint_trusted_signers=args.checkpoint_trusted_signers,
            checkpoint_min_remaining_validity_seconds=args.checkpoint_min_remaining_validity_seconds,
            checkpoint_indent=args.checkpoint_indent,
            checkpoint_sort_keys=args.checkpoint_sort_keys,
        )
        saved_path = storage.save_manifest(
            args.path,
            sign_key=_resolve_cli_key_argument(args.manifest_sign_key_file, args.manifest_sign_key_text),
            validity=_build_cli_validity_argument(
                not_before=args.manifest_not_before,
                expires_at=args.manifest_expires_at,
                warn_before_expiry_seconds=args.manifest_warn_before_expiry_seconds,
            ),
        )
        result = {
            "path": saved_path,
            "manifest": storage.export_manifest(),
        }
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "save-sharded-storage-engine-manifest":
        storage = LQFTShardedStorageEngine(
            shard_count=args.shard_count,
            checkpoint_dir=args.checkpoint_dir,
            wal_dir=args.wal_dir,
            wal_fsync=not args.wal_no_fsync,
            checkpoint_retain_last=args.checkpoint_retain_last,
            checkpoint_sign_key=args.checkpoint_sign_key_file,
            checkpoint_validity=_build_cli_validity_argument(
                not_before=args.checkpoint_not_before,
                expires_at=args.checkpoint_expires_at,
                warn_before_expiry_seconds=args.checkpoint_warn_before_expiry_seconds,
            ),
            checkpoint_verify_key=args.checkpoint_verify_key_file,
            checkpoint_trusted_signers=args.checkpoint_trusted_signers,
            checkpoint_min_remaining_validity_seconds=args.checkpoint_min_remaining_validity_seconds,
            checkpoint_indent=args.checkpoint_indent,
            checkpoint_sort_keys=args.checkpoint_sort_keys,
        )
        saved_path = storage.save_manifest(
            args.path,
            sign_key=_resolve_cli_key_argument(args.manifest_sign_key_file, args.manifest_sign_key_text),
            validity=_build_cli_validity_argument(
                not_before=args.manifest_not_before,
                expires_at=args.manifest_expires_at,
                warn_before_expiry_seconds=args.manifest_warn_before_expiry_seconds,
            ),
        )
        result = {
            "path": saved_path,
            "manifest": storage.export_manifest(),
        }
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "recover-storage-engine-manifest":
        storage = LQFTStorageEngine.load_manifest(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        recovered_map = storage.recover_map(
            migration_threshold=args.migration_threshold,
            truncate_incomplete_wal_tail=args.truncate_incomplete_wal_tail,
        )
        try:
            latest_snapshot = recovered_map.latest_snapshot()
            result = {
                "manifest_path": _coerce_filesystem_path(args.path),
                "storage": storage.export_manifest(),
                "stats": recovered_map.stats(),
                "latest_snapshot": (
                    None
                    if latest_snapshot is None
                    else {
                        "snapshot_id": latest_snapshot.snapshot_id,
                        "generation": latest_snapshot.generation,
                        "parent_snapshot_id": latest_snapshot.parent_snapshot_id,
                        "size": latest_snapshot.size,
                    }
                ),
            }
            if args.include_current_state_payload:
                result["current_state"] = recovered_map.export_current_state_payload()
        finally:
            recovered_map.shutdown_background_worker(cancel_futures=True)
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "recover-sharded-storage-engine-manifest":
        storage = LQFTShardedStorageEngine.load_manifest(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        recovered_map = storage.recover_map(
            migration_threshold=args.migration_threshold,
            truncate_incomplete_wal_tail=args.truncate_incomplete_wal_tail,
        )
        try:
            result = {
                "manifest_path": _coerce_filesystem_path(args.path),
                "storage": storage.export_manifest(),
                "stats": recovered_map.stats(),
            }
            if args.include_current_state_payload:
                result["current_state"] = recovered_map.export_current_state_payload()
        finally:
            recovered_map.shutdown_background_worker(cancel_futures=True)
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "checkpoint-storage-engine-manifest":
        storage = LQFTStorageEngine.load_manifest(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        recovered_map = storage.recover_map(
            migration_threshold=args.migration_threshold,
            truncate_incomplete_wal_tail=args.truncate_incomplete_wal_tail,
        )
        try:
            checkpoint_path = storage.checkpoint(recovered_map, retain_last=args.retain_last)
            latest_snapshot = recovered_map.latest_snapshot()
            result = {
                "manifest_path": _coerce_filesystem_path(args.path),
                "checkpoint_path": checkpoint_path,
                "storage": storage.export_manifest(),
                "stats": recovered_map.stats(),
                "latest_snapshot": (
                    None
                    if latest_snapshot is None
                    else {
                        "snapshot_id": latest_snapshot.snapshot_id,
                        "generation": latest_snapshot.generation,
                        "parent_snapshot_id": latest_snapshot.parent_snapshot_id,
                        "size": latest_snapshot.size,
                    }
                ),
            }
        finally:
            recovered_map.shutdown_background_worker(cancel_futures=True)
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "checkpoint-sharded-storage-engine-manifest":
        storage = LQFTShardedStorageEngine.load_manifest(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        recovered_map = storage.recover_map(
            migration_threshold=args.migration_threshold,
            truncate_incomplete_wal_tail=args.truncate_incomplete_wal_tail,
        )
        try:
            checkpoint_paths = storage.checkpoint(recovered_map, retain_last=args.retain_last)
            result = {
                "manifest_path": _coerce_filesystem_path(args.path),
                "checkpoint_paths": checkpoint_paths,
                "storage": storage.export_manifest(),
                "stats": recovered_map.stats(),
            }
        finally:
            recovered_map.shutdown_background_worker(cancel_futures=True)
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        return 0
    if args.command == "inspect-persisted-file":
        result = inspect_persisted_file(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        if args.fail_on_policy_fail and not bool(result.get("effective_policy", {}).get("allowed", False)):
            return _effective_policy_exit_code(result)
        return 0
    if args.command == "inspect-signer-trust-store":
        result = inspect_signer_trust_store(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_manifest_signers=args.trusted_manifest_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        if args.fail_on_policy_fail and not bool(result.get("effective_policy", {}).get("allowed", False)):
            return _effective_policy_exit_code(result)
        return 0
    if args.command == "inspect-storage-engine-manifest":
        result = inspect_storage_engine_manifest(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        if args.fail_on_policy_fail and not bool(result.get("effective_policy", {}).get("allowed", False)):
            return _effective_policy_exit_code(result)
        return 0
    if args.command == "inspect-sharded-storage-engine-manifest":
        result = inspect_sharded_storage_engine_manifest(
            args.path,
            verify_key=_resolve_cli_key_argument(args.verify_key_file, args.verify_key_text),
            trusted_signers=args.trusted_signers,
            min_remaining_validity_seconds=args.min_remaining_validity_seconds,
        )
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
        sys.stdout.write("\n")
        if args.fail_on_policy_fail and not bool(result.get("effective_policy", {}).get("allowed", False)):
            return _effective_policy_exit_code(result)
        return 0

    parser.print_help()
    return 1


def _safe_ratio(numerator, denominator):
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _read_cache_fetch(cache, key):
    if cache is None:
        return False, None
    try:
        value = cache.pop(key)
    except KeyError:
        return False, None
    cache[key] = value
    return True, value


def _read_cache_store(cache, key, value, max_entries):
    if cache is None or max_entries <= 0:
        return 0
    if key in cache:
        cache.pop(key)
    cache[key] = value
    evictions = 0
    while len(cache) > max_entries:
        cache.popitem(last=False)
        evictions += 1
    return evictions


def _annotate_persistent_stats(
    raw_stats,
    pending_buffered_writes=0,
    reads_sealed=False,
    engine_scope="global-native-state",
):
    stats = dict(raw_stats)
    logical_inserts = int(stats.get("logical_inserts", 0) or 0)
    physical_nodes = int(stats.get("physical_nodes", 0) or 0)
    estimated_native_bytes = int(stats.get("estimated_native_bytes", 0) or 0)
    active_child_bytes = int(stats.get("active_child_bytes", 0) or 0)
    value_pool_bytes = int(stats.get("value_pool_bytes", 0) or 0)
    nodes_with_values = int(stats.get("nodes_with_values", stats.get("live_items", 0)) or 0)
    nodes_with_children = int(stats.get("nodes_with_children", 0) or 0)
    hybrid_nodes = int(stats.get("hybrid_nodes", 0) or 0)
    value_dedup_unique_values = int(
        stats.get("value_dedup_unique_values", stats.get("value_pool_entries", 0)) or 0
    )
    value_dedup_saved_values = int(
        stats.get("value_dedup_saved_values", max(0, nodes_with_values - value_dedup_unique_values)) or 0
    )
    stats["model"] = "persistent-native"
    stats["engine_scope"] = engine_scope
    stats["pending_buffered_writes"] = int(pending_buffered_writes)
    stats["reads_sealed"] = bool(reads_sealed)
    stats.setdefault("nodes_with_values", nodes_with_values)
    stats.setdefault("nodes_with_children", nodes_with_children)
    stats.setdefault("hybrid_nodes", hybrid_nodes)
    stats.setdefault("value_dedup_unique_values", value_dedup_unique_values)
    stats.setdefault("value_dedup_saved_values", value_dedup_saved_values)
    stats.setdefault(
        "value_dedup_ratio",
        _safe_ratio(value_dedup_saved_values, nodes_with_values),
    )
    stats.setdefault(
        "node_density_ratio",
        stats.get("deduplication_ratio", _safe_ratio(nodes_with_values, physical_nodes)),
    )
    stats.setdefault("deduplication_ratio", stats["node_density_ratio"])
    stats["estimated_bytes_per_logical_insert"] = _safe_ratio(
        estimated_native_bytes,
        logical_inserts,
    )
    stats["active_child_bytes_per_physical_node"] = _safe_ratio(
        active_child_bytes,
        physical_nodes,
    )
    stats["value_node_share_of_physical_nodes"] = _safe_ratio(
        nodes_with_values,
        physical_nodes,
    )
    stats["internal_node_share_of_physical_nodes"] = _safe_ratio(
        nodes_with_children,
        physical_nodes,
    )
    stats["hybrid_node_share_of_physical_nodes"] = _safe_ratio(
        hybrid_nodes,
        physical_nodes,
    )
    stats["value_pool_share_of_native_bytes"] = _safe_ratio(
        value_pool_bytes,
        estimated_native_bytes,
    )
    return stats


def _annotate_mutable_stats(raw_stats, model, engine_scope):
    stats = dict(raw_stats)
    logical_inserts = int(stats.get("logical_inserts", 0) or 0)
    mutable_capacity = int(stats.get("mutable_capacity", logical_inserts) or logical_inserts)
    stats["model"] = model
    stats["engine_scope"] = engine_scope
    stats["mutable_load_factor"] = _safe_ratio(logical_inserts, mutable_capacity)
    return stats


def _load_items_into_persistent(target, keys, values):
    if not keys:
        return target

    state_bulk_insert_pairs = getattr(target, "_native_state_bulk_insert_key_values", None)
    state_insert = getattr(target, "_native_state_insert_kv", None)
    persistent_state = getattr(target, "_persistent_state", None)

    if state_bulk_insert_pairs is not None and persistent_state is not None:
        state_bulk_insert_pairs(persistent_state, keys, values)
        return target
    if state_insert is not None and persistent_state is not None:
        for key, value in zip(keys, values):
            state_insert(persistent_state, key, value)
        return target
    for key, value in zip(keys, values):
        target.insert(key, value)
    return target


def _materialize_mutable_into_persistent(source, target):
    persistent_state = getattr(target, "_persistent_state", None)
    if persistent_state is None:
        return False

    materialize_method = getattr(source, "materialize_persistent", None)
    if callable(materialize_method):
        materialize_method(persistent_state)
        return True

    native_state = getattr(source, "_native_state", None)
    module_materialize = getattr(lqft_c_engine, "mutable_materialize_persistent", None)
    if native_state is not None and module_materialize is not None:
        module_materialize(native_state, persistent_state)
        return True

    return False


def _export_mutable_tombstones(source):
    export_tombstones = getattr(source, "export_tombstones", None)
    if export_tombstones is not None:
        return list(export_tombstones())

    tombstones = getattr(source, "_tombstones", None)
    if tombstones is not None:
        return list(tombstones)

    return []


def _clone_mutable_frontend(source):
    clone_method = getattr(source, "clone", None)
    if callable(clone_method):
        return clone_method()

    data = getattr(source, "_data", None)
    tombstones = getattr(source, "_tombstones", None)
    if data is None or tombstones is None:
        return None

    cloned = type(source)(migration_threshold=getattr(source, "migration_threshold", 50000))
    cloned._data.update(data)
    cloned._tombstones.update(tombstones)
    return cloned


def _logical_snapshot_size(committed_snapshot, mutable_source, pending_clear=False):
    if pending_clear or committed_snapshot is None:
        return len(mutable_source)

    committed_size = committed_snapshot.size
    mutable_size = len(mutable_source)

    export_tombstones = getattr(mutable_source, "export_tombstones", None)
    if callable(export_tombstones):
        tombstone_keys = list(export_tombstones())
    else:
        tombstone_keys = _export_mutable_tombstones(mutable_source)

    if mutable_size == 0 and not tombstone_keys:
        return committed_size

    overlap_count = 0
    committed_root = committed_snapshot.root
    if mutable_size:
        export_items = getattr(mutable_source, "export_items", None)
        if callable(export_items):
            mutable_keys, _mutable_values = export_items()
            mutable_keys = list(mutable_keys)
        else:
            mutable_keys, _mutable_values = _export_mutable_items(mutable_source)
        for key in mutable_keys:
            if committed_root.contains(key):
                overlap_count += 1

    tombstone_overlap_count = 0
    for key in tombstone_keys:
        if committed_root.contains(key):
            tombstone_overlap_count += 1

    logical_size = committed_size - tombstone_overlap_count + mutable_size - overlap_count
    return max(0, logical_size)


def _apply_mutable_delta_into_persistent(source, target):
    if _materialize_mutable_into_persistent(source, target):
        return target

    for key in _export_mutable_tombstones(source):
        target.delete(key)

    keys, values = _export_mutable_items(source)
    _load_items_into_persistent(target, keys, values)
    return target


def _export_mutable_items(source):
    export_items = getattr(source, "export_items", None)
    if export_items is not None:
        keys, values = export_items()
        return list(keys), list(values)

    data = getattr(source, "_data", None)
    if data is not None:
        keys = list(data.keys())
        return keys, [data[key] for key in keys]

    raise TypeError("Mutable LQFT frontend does not expose export_items()")


def _diff_native_persistent_roots(left_root, right_root):
    left_state = getattr(left_root, "_persistent_state", None)
    right_state = getattr(right_root, "_persistent_state", None)
    native_diff = getattr(lqft_c_engine, "persistent_diff_states", None)
    if left_state is None or right_state is None or native_diff is None:
        return None
    return native_diff(left_state, right_state)


def _export_persistent_items(source):
    export_items = getattr(source, "export_items", None)
    if export_items is None:
        raise TypeError("Persistent LQFT root does not expose export_items()")
    keys, values = export_items()
    return list(keys), list(values)


def _load_items_into_mutable(target, keys, values):
    if not keys:
        return target
    for key, value in zip(keys, values):
        target.insert(key, value)
    return target


def _restore_persistent_into_mutable(target, source):
    persistent_state = getattr(source, "_persistent_state", None)
    if persistent_state is None:
        return False

    restore_method = getattr(target, "restore_persistent", None)
    if callable(restore_method):
        restore_method(persistent_state)
        return True

    native_state = getattr(target, "_native_state", None)
    module_restore = getattr(lqft_c_engine, "mutable_restore_persistent", None)
    if native_state is not None and module_restore is not None:
        module_restore(native_state, persistent_state)
        return True

    return False


def _committed_root_stats(snapshot):
    if snapshot is None:
        return {}

    root = snapshot.root
    get_stats = getattr(root, "get_stats", None)
    if not callable(get_stats):
        return {}

    return dict(get_stats())


_COMMITTED_PHASE5_STAT_DEFAULTS = {
    "live_items": 0,
    "nodes_with_values": 0,
    "nodes_with_children": 0,
    "leaf_only_nodes": 0,
    "branch_only_nodes": 0,
    "hybrid_nodes": 0,
    "node_density_ratio": 0.0,
    "deduplication_ratio": 0.0,
    "value_dedup_unique_values": 0,
    "value_dedup_saved_values": 0,
    "value_dedup_ratio": 0.0,
    "canonical_registry_hits": 0,
    "canonical_registry_misses": 0,
    "canonical_registry_lookups": 0,
    "canonical_registry_hit_rate": 0.0,
    "value_pool_hits": 0,
    "value_pool_misses": 0,
    "value_pool_lookups": 0,
    "value_pool_hit_rate": 0.0,
    "value_node_share_of_physical_nodes": 0.0,
    "internal_node_share_of_physical_nodes": 0.0,
    "hybrid_node_share_of_physical_nodes": 0.0,
}

_RETAINED_SUBTREE_METRIC_DEFAULTS = {
    "retained_snapshot_pair_count": 0,
    "retained_snapshot_total_pair_items": 0,
    "retained_snapshot_unchanged_items": 0,
    "retained_snapshot_changed_items": 0,
    "retained_snapshot_item_reuse_ratio": 0.0,
    "retained_snapshot_total_pair_physical_nodes": 0,
    "retained_snapshot_estimated_shared_subtree_nodes": 0,
    "retained_snapshot_estimated_shared_internal_nodes": 0,
    "retained_snapshot_subtree_reuse_ratio": 0.0,
    "retained_snapshot_internal_subtree_reuse_ratio": 0.0,
}


def _snapshot_pair_subtree_metrics(left_snapshot, right_snapshot):
    left_size = int(left_snapshot.size or 0)
    right_size = int(right_snapshot.size or 0)
    left_stats = left_snapshot.stats()
    right_stats = right_snapshot.stats()
    left_nodes = int(left_stats.get("physical_nodes", 0) or 0)
    right_nodes = int(right_stats.get("physical_nodes", 0) or 0)
    left_internal = int(left_stats.get("nodes_with_children", 0) or 0)
    right_internal = int(right_stats.get("nodes_with_children", 0) or 0)

    diff = _diff_native_persistent_roots(left_snapshot.root, right_snapshot.root)
    if diff is None:
        left_data = left_snapshot.data
        right_data = right_snapshot.data
        left_keys = set(left_data)
        right_keys = set(right_data)
        changed_count = sum(
            1
            for key in (left_keys & right_keys)
            if left_data[key] != right_data[key]
        )
        added_count = len(right_keys - left_keys)
        removed_count = len(left_keys - right_keys)
    else:
        added_count = len(diff.get("added", {}))
        removed_count = len(diff.get("removed", {}))
        changed_count = len(diff.get("changed", {}))

    unchanged_left = max(0, left_size - removed_count - changed_count)
    unchanged_right = max(0, right_size - added_count - changed_count)
    unchanged_items = min(unchanged_left, unchanged_right)
    pair_items = max(left_size, right_size)
    pair_physical_nodes = max(left_nodes, right_nodes)
    pair_internal_nodes = max(left_internal, right_internal)
    item_reuse_ratio = _safe_ratio(unchanged_items, pair_items)
    shared_subtree_nodes = min(left_nodes, right_nodes, int(round(pair_physical_nodes * item_reuse_ratio)))
    shared_internal_nodes = min(left_internal, right_internal, int(round(pair_internal_nodes * item_reuse_ratio)))

    return {
        "pair_items": pair_items,
        "unchanged_items": unchanged_items,
        "changed_items": changed_count + added_count + removed_count,
        "pair_physical_nodes": pair_physical_nodes,
        "pair_internal_nodes": pair_internal_nodes,
        "estimated_shared_subtree_nodes": shared_subtree_nodes,
        "estimated_shared_internal_nodes": shared_internal_nodes,
    }


def _compute_retained_snapshot_subtree_metrics(snapshot_handles):
    if len(snapshot_handles) < 2:
        return dict(_RETAINED_SUBTREE_METRIC_DEFAULTS)

    totals = {
        "retained_snapshot_pair_count": 0,
        "retained_snapshot_total_pair_items": 0,
        "retained_snapshot_unchanged_items": 0,
        "retained_snapshot_changed_items": 0,
        "retained_snapshot_total_pair_physical_nodes": 0,
        "retained_snapshot_total_pair_internal_nodes": 0,
        "retained_snapshot_estimated_shared_subtree_nodes": 0,
        "retained_snapshot_estimated_shared_internal_nodes": 0,
    }

    for previous, current in zip(snapshot_handles, snapshot_handles[1:]):
        pair_metrics = _snapshot_pair_subtree_metrics(previous, current)
        totals["retained_snapshot_pair_count"] += 1
        totals["retained_snapshot_total_pair_items"] += pair_metrics["pair_items"]
        totals["retained_snapshot_unchanged_items"] += pair_metrics["unchanged_items"]
        totals["retained_snapshot_changed_items"] += pair_metrics["changed_items"]
        totals["retained_snapshot_total_pair_physical_nodes"] += pair_metrics["pair_physical_nodes"]
        totals["retained_snapshot_total_pair_internal_nodes"] += pair_metrics["pair_internal_nodes"]
        totals["retained_snapshot_estimated_shared_subtree_nodes"] += pair_metrics[
            "estimated_shared_subtree_nodes"
        ]
        totals["retained_snapshot_estimated_shared_internal_nodes"] += pair_metrics[
            "estimated_shared_internal_nodes"
        ]

    totals["retained_snapshot_item_reuse_ratio"] = _safe_ratio(
        totals["retained_snapshot_unchanged_items"],
        totals["retained_snapshot_total_pair_items"],
    )
    totals["retained_snapshot_subtree_reuse_ratio"] = _safe_ratio(
        totals["retained_snapshot_estimated_shared_subtree_nodes"],
        totals["retained_snapshot_total_pair_physical_nodes"],
    )
    totals["retained_snapshot_internal_subtree_reuse_ratio"] = _safe_ratio(
        totals["retained_snapshot_estimated_shared_internal_nodes"],
        totals["retained_snapshot_total_pair_internal_nodes"],
    )
    totals.pop("retained_snapshot_total_pair_internal_nodes", None)
    return totals


def _new_snapshot_registry():
    registry_type = getattr(lqft_c_engine, "NativeSnapshotRegistry", None)
    if registry_type is not None:
        return registry_type()

    create_registry = getattr(lqft_c_engine, "create_snapshot_registry", None)
    if create_registry is not None:
        return create_registry()

    return []


def _snapshot_registry_append(registry, metadata, root):
    append = getattr(registry, "append", None)
    if append is not None:
        append(metadata, root)
        return
    registry.append((int(metadata.snapshot_id), metadata, root))


def _snapshot_registry_get_metadata(registry, snapshot_id):
    get_method = getattr(registry, "get_metadata", None)
    if get_method is None:
        get_method = getattr(registry, "get", None)
    if get_method is not None:
        return get_method(snapshot_id)

    for current_snapshot_id, metadata, _root in registry:
        if current_snapshot_id == snapshot_id:
            return metadata
    return None


def _snapshot_registry_get_root(registry, snapshot_id):
    get_method = getattr(registry, "get_root", None)
    if get_method is not None:
        return get_method(snapshot_id)

    for current_snapshot_id, _metadata, root in registry:
        if current_snapshot_id == snapshot_id:
            return root
    return None


def _snapshot_registry_values(registry):
    values = getattr(registry, "values", None)
    if values is not None:
        return values()
    return [metadata for _snapshot_id, metadata, _root in registry]


def _snapshot_registry_prune(registry, retained_snapshot_ids):
    prune = getattr(registry, "prune", None)
    if prune is not None:
        prune(retained_snapshot_ids)
        return

    registry[:] = [
        entry for entry in registry if entry[0] in retained_snapshot_ids
    ]


def _snapshot_metadata_dict(snapshot):
    return {
        "snapshot_id": snapshot.snapshot_id,
        "generation": snapshot.generation,
        "parent_snapshot_id": snapshot.parent_snapshot_id,
        "created_at_ns": int(snapshot._meta.created_at_ns),
        "created_at": snapshot.created_at,
        "mutation_count": snapshot.mutation_count,
        "delta_size": snapshot.delta_size,
        "size": snapshot.size,
    }


def _normalize_snapshot_items(items):
    normalized = []
    for entry in items:
        if isinstance(entry, dict):
            key = entry.get("key")
            value = entry.get("value")
        else:
            try:
                key, value = entry
            except (TypeError, ValueError) as exc:
                raise ValueError("snapshot items must be {'key', 'value'} mappings or key/value pairs") from exc

        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("snapshot items must contain string keys and string values")
        normalized.append((key, value))

    normalized.sort(key=lambda item: item[0])
    return normalized


def _snapshot_payload_to_root(snapshot_payload, migration_threshold):
    items = _normalize_snapshot_items(snapshot_payload.get("items", ()))
    root = _new_native_lqft(migration_threshold)
    if items:
        keys = [key for key, _value in items]
        values = [value for _key, value in items]
        _load_items_into_persistent(root, keys, values)
    return root, items


@dataclass(frozen=True, slots=True)
class LQFTSnapshot:
    _registry: object
    _snapshot_id: int
    _created_at_cache: str | None = None
    _data_cache: MappingProxyType | None = None

    @property
    def _meta(self):
        meta = _snapshot_registry_get_metadata(self._registry, self._snapshot_id)
        if meta is None:
            raise KeyError(f"Unknown snapshot id: {self._snapshot_id}")
        return meta

    @property
    def root(self):
        root = _snapshot_registry_get_root(self._registry, self._snapshot_id)
        if root is None:
            raise KeyError(f"Unknown snapshot id: {self._snapshot_id}")
        return root

    @property
    def snapshot_id(self):
        return int(self._snapshot_id)

    @property
    def generation(self):
        return int(self._meta.generation)

    @property
    def parent_snapshot_id(self):
        parent_snapshot_id = self._meta.parent_snapshot_id
        return None if parent_snapshot_id is None else int(parent_snapshot_id)

    @property
    def mutation_count(self):
        return int(self._meta.mutation_count)

    @property
    def delta_size(self):
        return int(self._meta.delta_size)

    @property
    def size(self):
        return int(self._meta.size)

    @property
    def created_at(self):
        created_at = self._created_at_cache
        if created_at is not None:
            return created_at

        created_at = datetime.fromtimestamp(
            int(self._meta.created_at_ns) / 1_000_000_000,
            tz=timezone.utc,
        ).isoformat()
        object.__setattr__(self, "_created_at_cache", created_at)
        return created_at

    @property
    def data(self):
        data = self._data_cache
        if data is not None:
            return data

        keys, values = _export_persistent_items(self.root)
        data = MappingProxyType(dict(zip(keys, values)))
        object.__setattr__(self, "_data_cache", data)
        return data

    def get(self, key, default=None):
        return self.data.get(key, default)

    def contains(self, key):
        return key in self.data

    def items(self):
        return tuple(self.data.items())

    def export(self):
        items = [
            {"key": key, "value": value}
            for key, value in sorted(self.items())
        ]
        return {
            "format": "lqft-snapshot-v1",
            "metadata": _snapshot_metadata_dict(self),
            "items": items,
        }

    def save(self, path, *, indent=2, sort_keys=False, sign_key=None, validity=None):
        payload = self.export()
        document = _build_persisted_file_document(
            payload,
            compression="gzip" if _path_uses_gzip(path) else "none",
            sign_key=sign_key,
            validity=validity,
        )
        _write_json_file_atomic(path, document, indent=indent, sort_keys=sort_keys)
        return _coerce_filesystem_path(path)

    def stats(self):
        root_stats = self.root.get_stats() if hasattr(self.root, "get_stats") else {}
        stats = dict(root_stats)
        stats.update(
            {
                "snapshot_id": self.snapshot_id,
                "generation": self.generation,
                "parent_snapshot_id": self.parent_snapshot_id,
                "created_at": self.created_at,
                "mutation_count": self.mutation_count,
                "delta_size": self.delta_size,
                "logical_items": self.size,
                "model": "lqft-snapshot",
            }
        )
        return stats


@dataclass(slots=True)
class LQFTReadSession:
    _owner: "LQFTMap"
    snapshot: LQFTSnapshot
    _closed: bool = False

    @property
    def snapshot_id(self):
        return self.snapshot.snapshot_id

    def get(self, key, default=None):
        value = self.snapshot.root.search(key)
        if value is None:
            return default
        return value

    def search(self, key):
        return self.get(key)

    def contains(self, key):
        return bool(self.snapshot.root.contains(key))

    def items(self):
        return self.snapshot.items()

    def stats(self):
        stats = dict(self.snapshot.stats())
        stats.update(
            {
                "model": "lqft-read-session",
                "reader_snapshot_id": self.snapshot.snapshot_id,
                "reader_snapshot_generation": self.snapshot.generation,
                "reader_session_closed": self._closed,
            }
        )
        return stats

    def close(self):
        if self._closed:
            return
        self._owner._release_pinned_snapshot(self.snapshot.snapshot_id)
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class LQFTMap:
    __slots__ = (
        "_mutable",
        "_pending_clear",
        "_snapshots",
        "_snapshot_handles",
        "_current_snapshot",
        "_next_snapshot_id",
        "_pending_mutations",
        "_compact_count",
        "_total_pruned_snapshots",
        "_last_compact_pruned_snapshots",
        "_last_compact_retain_last",
        "_read_cache",
        "_read_cache_max_entries",
        "_read_cache_hits",
        "_read_cache_misses",
        "_read_cache_evictions",
        "_state_lock",
        "_pinned_snapshot_readers",
        "_background_executor",
        "_background_tasks_submitted",
        "_background_tasks_completed",
        "_background_tasks_failed",
        "_state_version",
        "_wal_path",
        "_wal_fsync",
        "_wal_next_sequence",
        "_wal_records_written",
        "_wal_replay_mode",
    )

    def __init__(self, migration_threshold=50000):
        self._mutable = MutableLQFT(migration_threshold=migration_threshold)
        self._pending_clear = False
        self._snapshots = _new_snapshot_registry()
        self._snapshot_handles = []
        self._current_snapshot = None
        self._next_snapshot_id = 1
        self._pending_mutations = 0
        self._compact_count = 0
        self._total_pruned_snapshots = 0
        self._last_compact_pruned_snapshots = 0
        self._last_compact_retain_last = None
        self._read_cache = OrderedDict()
        self._read_cache_max_entries = 0
        self._read_cache_hits = 0
        self._read_cache_misses = 0
        self._read_cache_evictions = 0
        self._state_lock = threading.RLock()
        self._pinned_snapshot_readers = {}
        self._background_executor = None
        self._background_tasks_submitted = 0
        self._background_tasks_completed = 0
        self._background_tasks_failed = 0
        self._state_version = 0
        self._wal_path = None
        self._wal_fsync = True
        self._wal_next_sequence = 1
        self._wal_records_written = 0
        self._wal_replay_mode = False

    def _resolve_snapshot(self, snapshot):
        if isinstance(snapshot, LQFTSnapshot):
            if _snapshot_registry_get_metadata(self._snapshots, snapshot.snapshot_id) is None:
                raise KeyError(f"Unknown snapshot id: {snapshot.snapshot_id}")
            return snapshot
        if type(snapshot) is int:
            if _snapshot_registry_get_metadata(self._snapshots, snapshot) is None:
                raise KeyError(f"Unknown snapshot id: {snapshot}")
            for handle in self._snapshot_handles:
                if handle.snapshot_id == snapshot:
                    return handle
            raise KeyError(f"Unknown snapshot id: {snapshot}")
        raise TypeError("snapshot must be an LQFTSnapshot or snapshot id")

    def _clear_pending_delta(self):
        self._mutable.clear()
        self._pending_clear = False

    def _reset_runtime_state_locked(self):
        self._invalidate_read_cache()
        self._mutable.clear()
        self._pending_clear = False
        self._snapshots = _new_snapshot_registry()
        self._snapshot_handles = []
        self._current_snapshot = None
        self._next_snapshot_id = 1
        self._pending_mutations = 0
        self._pinned_snapshot_readers = {}
        self._last_compact_pruned_snapshots = 0
        self._last_compact_retain_last = None

    def _build_wal_record_locked(self, operation, **fields):
        return {
            "format": "lqft-wal-v1",
            "sequence": self._wal_next_sequence,
            "recorded_at_ns": time.time_ns(),
            "operation": operation,
            **fields,
        }

    def _append_wal_record_locked(self, operation, **fields):
        if self._wal_path is None or self._wal_replay_mode:
            return
        record = self._build_wal_record_locked(operation, **fields)
        _append_jsonl_record(self._wal_path, record, fsync=self._wal_fsync)
        self._wal_next_sequence += 1
        self._wal_records_written += 1

    def _append_wal_state_checkpoint_locked(self, reason):
        if self._wal_path is None or self._wal_replay_mode:
            return

        snapshot_bundle = None
        current_snapshot_id = None if self._current_snapshot is None else self._current_snapshot.snapshot_id
        if self._snapshot_handles:
            snapshot_bundle = _build_snapshot_bundle_payload(
                [snapshot.export() for snapshot in self._snapshot_handles],
                current_snapshot_id=current_snapshot_id,
            )

        self._append_wal_record_locked(
            "state-checkpoint",
            reason=reason,
            current_snapshot_id=current_snapshot_id,
            snapshot_bundle=snapshot_bundle,
        )

    def _restore_wal_checkpoint_locked(self, snapshot_bundle, current_snapshot_id):
        self._reset_runtime_state_locked()
        if snapshot_bundle is None:
            return

        self.load_snapshot_bundle(snapshot_bundle, activate=False, _suppress_wal_checkpoint=True)
        if current_snapshot_id is not None:
            self.rollback(current_snapshot_id, _suppress_wal_checkpoint=True)

    def _apply_wal_record_locked(self, record):
        operation = record.get("operation")
        if operation == "put":
            self.put(record["key"], record["value"])
            return
        if operation == "delete":
            self.delete(record["key"])
            return
        if operation == "clear":
            self.clear()
            return
        if operation == "state-checkpoint":
            self._restore_wal_checkpoint_locked(
                record.get("snapshot_bundle"),
                record.get("current_snapshot_id"),
            )
            return
        raise ValueError(f"unsupported write-ahead log operation: {operation}")

    def enable_write_ahead_log(self, path, *, fsync=True, truncate=False):
        path = _coerce_filesystem_path(path)
        if _path_uses_gzip(path):
            raise ValueError("write-ahead log paths must not use gzip")

        with self._state_lock:
            if truncate:
                _write_bytes_file_atomic(path, b"")
                last_sequence = 0
            elif os.path.exists(path) and os.path.getsize(path) > 0:
                records = _read_jsonl_records(path)
                last_sequence = int(records[-1]["sequence"]) if records else 0
            else:
                directory = os.path.dirname(path)
                if directory:
                    os.makedirs(directory, exist_ok=True)
                if not os.path.exists(path):
                    _write_bytes_file_atomic(path, b"")
                last_sequence = 0

            self._wal_path = path
            self._wal_fsync = bool(fsync)
            self._wal_next_sequence = last_sequence + 1
            self._wal_records_written = 0
            return path

    def disable_write_ahead_log(self):
        with self._state_lock:
            self._wal_path = None

    def _replay_wal_records_locked(self, records):
        expected_sequence = 1
        for record in records:
            sequence = record.get("sequence")
            if type(sequence) is not int or sequence <= 0:
                raise ValueError("write-ahead log record sequence must be a positive integer")
            if sequence != expected_sequence:
                raise ValueError("write-ahead log record sequence is not contiguous")
            self._apply_wal_record_locked(record)
            expected_sequence += 1

    def replay_write_ahead_log(self, path, *, truncate_incomplete_tail=False):
        path = _coerce_filesystem_path(path)
        records = _read_jsonl_records(path, truncate_incomplete_tail=truncate_incomplete_tail)
        with self._state_lock:
            self._wal_replay_mode = True
            try:
                self._replay_wal_records_locked(records)
            finally:
                self._wal_replay_mode = False
        return self

    @classmethod
    def recover_from_write_ahead_log(cls, path, *, migration_threshold=50000, truncate_incomplete_tail=False):
        path = _coerce_filesystem_path(path)
        recovered = cls(migration_threshold=migration_threshold)
        try:
            recovered.replay_write_ahead_log(path, truncate_incomplete_tail=truncate_incomplete_tail)
        except Exception:
            recovered.shutdown_background_worker(cancel_futures=True)
            raise
        return recovered

    def _bump_state_version(self):
        self._state_version += 1

    def _invalidate_read_cache(self):
        self._read_cache.clear()

    def _pin_snapshot_locked(self, snapshot):
        snapshot_id = snapshot.snapshot_id
        self._pinned_snapshot_readers[snapshot_id] = self._pinned_snapshot_readers.get(snapshot_id, 0) + 1
        return LQFTReadSession(self, snapshot)

    def _release_pinned_snapshot(self, snapshot_id):
        with self._state_lock:
            current_count = self._pinned_snapshot_readers.get(snapshot_id, 0)
            if current_count <= 1:
                self._pinned_snapshot_readers.pop(snapshot_id, None)
            else:
                self._pinned_snapshot_readers[snapshot_id] = current_count - 1

    def _ensure_background_executor_locked(self):
        if self._background_executor is None:
            self._background_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lqft-map")
        return self._background_executor

    def _background_operation_done(self, future):
        with self._state_lock:
            if future.cancelled() or future.exception() is not None:
                self._background_tasks_failed += 1
            else:
                self._background_tasks_completed += 1

    def _submit_background_operation(self, operation, *args, **kwargs):
        with self._state_lock:
            executor = self._ensure_background_executor_locked()
            self._background_tasks_submitted += 1
        future = executor.submit(operation, *args, **kwargs)
        future.add_done_callback(self._background_operation_done)
        return future

    def _capture_async_snapshot_plan_locked(self):
        snapshot_id = self._next_snapshot_id
        self._next_snapshot_id += 1
        captured_state_version = self._state_version

        if self._pending_mutations == 0 and self._current_snapshot is not None:
            return {
                "mode": "reuse",
                "snapshot_id": snapshot_id,
                "parent_snapshot_id": self._current_snapshot.snapshot_id,
                "captured_state_version": captured_state_version,
                "mutation_count": 0,
                "delta_size": 0,
                "snapshot_size": self._current_snapshot.size,
                "root": self._current_snapshot.root,
                "snapshot_data": self._current_snapshot._data_cache,
            }

        mutable_snapshot = _clone_mutable_frontend(self._mutable)
        if mutable_snapshot is not None:
            return {
                "mode": "mutable-clone",
                "snapshot_id": snapshot_id,
                "parent_snapshot_id": self._current_snapshot.snapshot_id if self._current_snapshot is not None else None,
                "captured_state_version": captured_state_version,
                "mutation_count": self._pending_mutations,
                "delta_size": self._pending_mutations,
                "snapshot_size": _logical_snapshot_size(
                    self._current_snapshot,
                    mutable_snapshot,
                    pending_clear=self._pending_clear,
                ),
                "base_root": None if self._pending_clear else self._committed_root(),
                "migration_threshold": getattr(self._mutable, "migration_threshold", 50000),
                "mutable_snapshot": mutable_snapshot,
            }

        keys, values = _export_mutable_items(self._mutable)
        tombstones = _export_mutable_tombstones(self._mutable)
        return {
            "mode": "materialize",
            "snapshot_id": snapshot_id,
            "parent_snapshot_id": self._current_snapshot.snapshot_id if self._current_snapshot is not None else None,
            "captured_state_version": captured_state_version,
            "mutation_count": self._pending_mutations,
            "delta_size": self._pending_mutations,
            "snapshot_size": len(self),
            "base_root": None if self._pending_clear else self._committed_root(),
            "migration_threshold": getattr(self._mutable, "migration_threshold", 50000),
            "keys": keys,
            "values": values,
            "tombstones": tombstones,
        }

    def _materialize_async_snapshot_root(self, plan):
        if plan["mode"] == "reuse":
            return plan["root"], plan.get("snapshot_data")

        if plan["mode"] == "mutable-clone":
            base_root = plan["base_root"]
            mutable_snapshot = plan["mutable_snapshot"]
            persistent_apply = getattr(lqft_c_engine, "persistent_apply_mutable_delta", None)
            base_state = getattr(base_root, "_persistent_state", None)
            mutable_state = getattr(mutable_snapshot, "_native_state", None)

            if base_state is not None and mutable_state is not None and persistent_apply is not None:
                state_capsule = persistent_apply(base_state, mutable_state)
                return _new_native_lqft(
                    migration_threshold=plan["migration_threshold"],
                    _state_capsule=state_capsule,
                ), None

            can_clone_base = (
                base_root is not None
                and getattr(lqft_c_engine, "persistent_clone_state", None) is not None
            )
            root = _new_native_lqft(
                plan["migration_threshold"],
                clone_from=base_root if can_clone_base else None,
                shared_with=base_root if base_root is not None and not can_clone_base else None,
            )
            if base_root is not None and not can_clone_base:
                base_keys, base_values = _export_persistent_items(base_root)
                _load_items_into_persistent(root, base_keys, base_values)
            _apply_mutable_delta_into_persistent(mutable_snapshot, root)
            return root, None

        base_root = plan["base_root"]
        can_clone_base = (
            base_root is not None
            and getattr(lqft_c_engine, "persistent_clone_state", None) is not None
        )
        root = _new_native_lqft(
            plan["migration_threshold"],
            clone_from=base_root if can_clone_base else None,
            shared_with=base_root if base_root is not None and not can_clone_base else None,
        )
        if base_root is not None and not can_clone_base:
            base_keys, base_values = _export_persistent_items(base_root)
            _load_items_into_persistent(root, base_keys, base_values)
        for tombstone_key in plan["tombstones"]:
            root.delete(tombstone_key)
        _load_items_into_persistent(root, plan["keys"], plan["values"])
        return root, None

    def _finalize_async_snapshot(self, plan, root, snapshot_data):
        with self._state_lock:
            snapshot = LQFTSnapshot(
                _registry=self._snapshots,
                _snapshot_id=plan["snapshot_id"],
                _data_cache=snapshot_data,
            )
            _snapshot_registry_append(
                self._snapshots,
                lqft_c_engine.create_snapshot_metadata(
                    plan["snapshot_id"],
                    plan["snapshot_id"],
                    plan["parent_snapshot_id"],
                    time.time_ns(),
                    plan["mutation_count"],
                    plan["delta_size"],
                    plan["snapshot_size"],
                ),
                root,
            )
            self._snapshot_handles.append(snapshot)

            if plan["captured_state_version"] == self._state_version:
                self._current_snapshot = snapshot
                self._clear_pending_delta()
                self._pending_mutations = 0

            self._bump_state_version()
            return snapshot

    def _run_async_snapshot_plan(self, plan):
        root, snapshot_data = self._materialize_async_snapshot_root(plan)
        return self._finalize_async_snapshot(plan, root, snapshot_data)

    def _can_use_read_cache(self):
        return (
            self._read_cache_max_entries > 0
            and self._pending_mutations == 0
            and not self._pending_clear
            and self._current_snapshot is not None
        )

    def _read_cache_get(self, key):
        if not self._can_use_read_cache():
            return False, None
        found, value = _read_cache_fetch(self._read_cache, key)
        if found:
            self._read_cache_hits += 1
        else:
            self._read_cache_misses += 1
        return found, value

    def _read_cache_put(self, key, value):
        if not self._can_use_read_cache():
            return
        self._read_cache_evictions += _read_cache_store(
            self._read_cache,
            key,
            value,
            self._read_cache_max_entries,
        )

    def enable_read_cache(self, max_entries=256):
        with self._state_lock:
            max_entries = int(max_entries)
            if max_entries <= 0:
                raise ValueError("max_entries must be a positive integer")
            self._read_cache_max_entries = max_entries
            self._invalidate_read_cache()

    def disable_read_cache(self):
        with self._state_lock:
            self._read_cache_max_entries = 0
            self._invalidate_read_cache()

    def clear_read_cache(self):
        with self._state_lock:
            self._invalidate_read_cache()

    def read_snapshot(self, snapshot=None):
        with self._state_lock:
            if snapshot is None:
                if self._current_snapshot is None:
                    raise ValueError("no committed snapshot is available for a read session")
                resolved = self._current_snapshot
            else:
                resolved = self._resolve_snapshot(snapshot)
            return self._pin_snapshot_locked(resolved)

    def pin_snapshot(self, snapshot=None):
        return self.read_snapshot(snapshot)

    def snapshot_async(self):
        with self._state_lock:
            plan = self._capture_async_snapshot_plan_locked()
            executor = self._ensure_background_executor_locked()
            self._background_tasks_submitted += 1
        future = executor.submit(self._run_async_snapshot_plan, plan)
        future.add_done_callback(self._background_operation_done)
        return future

    def compact_async(self, retain_last=None):
        return self._submit_background_operation(self.compact, retain_last=retain_last)

    def shutdown_background_worker(self, wait=True, cancel_futures=False):
        with self._state_lock:
            executor = self._background_executor
            self._background_executor = None
        if executor is not None:
            executor.shutdown(wait=bool(wait), cancel_futures=bool(cancel_futures))

    def _committed_root(self):
        if self._current_snapshot is None or self._pending_clear:
            return None
        return self._current_snapshot.root

    def _committed_contains(self, key):
        root = self._committed_root()
        if root is None:
            return False
        return bool(root.contains(key))

    def _committed_get(self, key):
        root = self._committed_root()
        if root is None:
            return None
        return root.search(key)

    def _materialize_snapshot_root(self):
        base_root = self._committed_root()
        base_state = getattr(base_root, "_persistent_state", None)
        native_object_apply_delta = getattr(self._mutable, "apply_delta_from_persistent", None)
        mutable_state = getattr(self._mutable, "_native_state", None)
        native_apply_delta = getattr(lqft_c_engine, "persistent_apply_mutable_delta", None)
        if base_state is not None and callable(native_object_apply_delta):
            return _new_native_lqft(
                getattr(self._mutable, "migration_threshold", 50000),
                state_capsule=native_object_apply_delta(base_state),
            )

        if base_state is not None and mutable_state is not None and native_apply_delta is not None:
            return _new_native_lqft(
                getattr(self._mutable, "migration_threshold", 50000),
                state_capsule=native_apply_delta(base_state, mutable_state),
            )

        can_clone_base = (
            base_root is not None
            and getattr(lqft_c_engine, "persistent_clone_state", None) is not None
        )
        root = _new_native_lqft(
            getattr(self._mutable, "migration_threshold", 50000),
            clone_from=base_root if can_clone_base else None,
            shared_with=base_root if not can_clone_base else None,
        )
        if base_root is not None and not can_clone_base:
            keys, values = _export_persistent_items(base_root)
            _load_items_into_persistent(root, keys, values)
        _apply_mutable_delta_into_persistent(self._mutable, root)
        return root

    def put(self, key, value):
        with self._state_lock:
            self._invalidate_read_cache()
            self._append_wal_record_locked("put", key=key, value=value)
            self._mutable.insert(key, value)
            self._pending_mutations += 1
            self._bump_state_version()

    def insert(self, key, value):
        self.put(key, value)

    def get(self, key, default=None):
        with self._state_lock:
            value = self._mutable.search(key)
            if value is not None:
                return value
            if self._mutable.has_tombstone(key):
                return default
            found, cached_value = self._read_cache_get(key)
            if found:
                if cached_value is _READ_CACHE_ABSENT:
                    return default
                return cached_value
            value = self._committed_get(key)
            self._read_cache_put(key, _READ_CACHE_ABSENT if value is None else value)
            if value is not None:
                return value
            return default

    def search(self, key):
        return self.get(key)

    def delete(self, key):
        with self._state_lock:
            self._invalidate_read_cache()
            has_tombstone = self._mutable.has_tombstone(key)
            existed_in_mutable = self._mutable.contains(key)
            existed_in_committed = self._committed_contains(key)
            if self._pending_clear:
                if existed_in_mutable:
                    self._append_wal_record_locked("delete", key=key)
                    self._mutable.delete(key)
                    self._pending_mutations += 1
                return
            if has_tombstone:
                return
            if existed_in_mutable or existed_in_committed:
                self._append_wal_record_locked("delete", key=key)
            if existed_in_committed:
                self._mutable.mark_deleted(key)
            elif existed_in_mutable:
                self._mutable.delete(key)
            if existed_in_mutable or existed_in_committed:
                self._pending_mutations += 1
                self._bump_state_version()

    def remove(self, key):
        self.delete(key)

    def contains(self, key):
        with self._state_lock:
            if self._mutable.contains(key):
                return True
            if self._mutable.has_tombstone(key):
                return False
            found, cached_value = self._read_cache_get(key)
            if found:
                return cached_value is not _READ_CACHE_ABSENT
            value = self._committed_get(key)
            self._read_cache_put(key, _READ_CACHE_ABSENT if value is None else value)
            return value is not None

    def clear(self):
        with self._state_lock:
            self._invalidate_read_cache()
            if len(self):
                self._append_wal_record_locked("clear")
                self._pending_mutations += 1
                self._bump_state_version()
            self._mutable.clear()
            self._pending_clear = self._current_snapshot is not None

    def snapshot(self):
        with self._state_lock:
            self._invalidate_read_cache()
            if self._pending_mutations == 0 and self._current_snapshot is not None:
                root = self._current_snapshot.root
                snapshot_data = self._current_snapshot._data_cache
                snapshot_size = self._current_snapshot.size
                delta_size = 0
            else:
                snapshot_size = len(self)
                root = self._materialize_snapshot_root()
                snapshot_data = None
                delta_size = self._pending_mutations

            snapshot = LQFTSnapshot(
                _registry=self._snapshots,
                _snapshot_id=self._next_snapshot_id,
                _data_cache=snapshot_data,
            )
            _snapshot_registry_append(
                self._snapshots,
                lqft_c_engine.create_snapshot_metadata(
                    self._next_snapshot_id,
                    self._next_snapshot_id,
                    self._current_snapshot.snapshot_id if self._current_snapshot is not None else None,
                    time.time_ns(),
                    self._pending_mutations,
                    delta_size,
                    snapshot_size,
                ),
                root,
            )
            self._snapshot_handles.append(snapshot)
            self._current_snapshot = snapshot
            self._next_snapshot_id += 1
            self._clear_pending_delta()
            self._pending_mutations = 0
            self._bump_state_version()
            self._append_wal_state_checkpoint_locked("snapshot")
            return snapshot

    def freeze(self):
        return self.snapshot().root

    def export_snapshot(self, snapshot=None):
        with self._state_lock:
            if snapshot is None:
                if self._current_snapshot is None:
                    raise ValueError("no snapshots are available to export")
                resolved = self._current_snapshot
            else:
                resolved = self._resolve_snapshot(snapshot)
            return resolved.export()

    def export_snapshot_bundle(self, snapshots=None):
        with self._state_lock:
            if snapshots is None:
                resolved_snapshots = list(self._snapshot_handles)
            else:
                if not isinstance(snapshots, (list, tuple)):
                    raise TypeError("snapshots must be a list or tuple of snapshots or snapshot ids")
                resolved_snapshots = [self._resolve_snapshot(snapshot) for snapshot in snapshots]

            if not resolved_snapshots:
                raise ValueError("no snapshots are available to export")

            payload_snapshots = [snapshot.export() for snapshot in resolved_snapshots]
            current_snapshot_id = None if self._current_snapshot is None else self._current_snapshot.snapshot_id
            if current_snapshot_id not in {snapshot["metadata"]["snapshot_id"] for snapshot in payload_snapshots}:
                current_snapshot_id = None
            return _build_snapshot_bundle_payload(payload_snapshots, current_snapshot_id=current_snapshot_id)

    def save_snapshot(self, path, snapshot=None, *, indent=2, sort_keys=False, sign_key=None, validity=None):
        with self._state_lock:
            if snapshot is None:
                if self._current_snapshot is None:
                    raise ValueError("no snapshots are available to export")
                resolved = self._current_snapshot
            else:
                resolved = self._resolve_snapshot(snapshot)
            payload = resolved.export()

        document = _build_persisted_file_document(
            payload,
            compression="gzip" if _path_uses_gzip(path) else "none",
            sign_key=sign_key,
            validity=validity,
        )
        _write_json_file_atomic(path, document, indent=indent, sort_keys=sort_keys)
        return _coerce_filesystem_path(path)

    def save_snapshot_bundle(self, path, snapshots=None, *, indent=2, sort_keys=False, sign_key=None, validity=None):
        payload = self.export_snapshot_bundle(snapshots=snapshots)
        document = _build_persisted_file_document(
            payload,
            compression="gzip" if _path_uses_gzip(path) else "none",
            sign_key=sign_key,
            validity=validity,
        )
        _write_json_file_atomic(path, document, indent=indent, sort_keys=sort_keys)
        return _coerce_filesystem_path(path)

    def load_snapshot(self, snapshot_payload, activate=False, _suppress_wal_checkpoint=False):
        with self._state_lock:
            if not isinstance(snapshot_payload, dict):
                raise TypeError("snapshot_payload must be a dictionary")

            if snapshot_payload.get("format") != "lqft-snapshot-v1":
                raise ValueError("unsupported snapshot payload format")

            metadata = snapshot_payload.get("metadata")
            if not isinstance(metadata, dict):
                raise ValueError("snapshot payload must include a metadata dictionary")

            snapshot_id = metadata.get("snapshot_id")
            if snapshot_id is None:
                snapshot_id = self._next_snapshot_id
            elif type(snapshot_id) is not int or snapshot_id <= 0:
                raise ValueError("snapshot metadata snapshot_id must be a positive integer")

            if _snapshot_registry_get_metadata(self._snapshots, snapshot_id) is not None:
                raise ValueError(f"snapshot id already exists: {snapshot_id}")

            generation = metadata.get("generation", snapshot_id)
            if type(generation) is not int or generation <= 0:
                raise ValueError("snapshot metadata generation must be a positive integer")

            parent_snapshot_id = metadata.get("parent_snapshot_id")
            if parent_snapshot_id is not None and type(parent_snapshot_id) is not int:
                raise ValueError("snapshot metadata parent_snapshot_id must be an integer or null")

            created_at_ns = metadata.get("created_at_ns", time.time_ns())
            if type(created_at_ns) is not int or created_at_ns < 0:
                raise ValueError("snapshot metadata created_at_ns must be a non-negative integer")

            mutation_count = metadata.get("mutation_count", 0)
            delta_size = metadata.get("delta_size", 0)
            if type(mutation_count) is not int or mutation_count < 0:
                raise ValueError("snapshot metadata mutation_count must be a non-negative integer")
            if type(delta_size) is not int or delta_size < 0:
                raise ValueError("snapshot metadata delta_size must be a non-negative integer")

            root, items = _snapshot_payload_to_root(
                snapshot_payload,
                getattr(self._mutable, "migration_threshold", 50000),
            )
            size = metadata.get("size", len(items))
            if type(size) is not int or size < 0:
                raise ValueError("snapshot metadata size must be a non-negative integer")
            if size != len(items):
                raise ValueError("snapshot metadata size does not match the exported item count")

            snapshot = LQFTSnapshot(
                _registry=self._snapshots,
                _snapshot_id=snapshot_id,
            )
            _snapshot_registry_append(
                self._snapshots,
                lqft_c_engine.create_snapshot_metadata(
                    snapshot_id,
                    generation,
                    parent_snapshot_id,
                    created_at_ns,
                    mutation_count,
                    delta_size,
                    size,
                ),
                root,
            )
            self._snapshot_handles.append(snapshot)
            self._next_snapshot_id = max(self._next_snapshot_id, snapshot_id + 1)

            if activate:
                self._invalidate_read_cache()
                self._clear_pending_delta()
                self._current_snapshot = snapshot
                self._pending_mutations = 0
                self._bump_state_version()

            if not _suppress_wal_checkpoint:
                self._append_wal_state_checkpoint_locked("load-snapshot")

            return snapshot

    def load_snapshot_bundle(self, bundle_payload, activate=False, _suppress_wal_checkpoint=False):
        if not isinstance(bundle_payload, dict):
            raise TypeError("bundle_payload must be a dictionary")
        if bundle_payload.get("format") != "lqft-snapshot-bundle-v1":
            raise ValueError("unsupported snapshot bundle payload format")

        metadata = bundle_payload.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("snapshot bundle payload must include a metadata dictionary")

        snapshots = bundle_payload.get("snapshots")
        if not isinstance(snapshots, list) or not snapshots:
            raise ValueError("snapshot bundle payload must include a non-empty snapshots list")

        loaded_snapshots = [
            self.load_snapshot(snapshot_payload, activate=False, _suppress_wal_checkpoint=True)
            for snapshot_payload in snapshots
        ]

        if activate:
            current_snapshot_id = metadata.get("current_snapshot_id")
            if current_snapshot_id is None:
                current_snapshot_id = loaded_snapshots[-1].snapshot_id
            if type(current_snapshot_id) is not int:
                raise ValueError("snapshot bundle metadata current_snapshot_id must be an integer or null")
            self.rollback(current_snapshot_id, _suppress_wal_checkpoint=True)

        if not _suppress_wal_checkpoint:
            self._append_wal_state_checkpoint_locked("load-snapshot-bundle")

        return loaded_snapshots

    def load_snapshot_file(self, path, activate=False, verify_key=None, trusted_signers=None, min_remaining_validity_seconds=None):
        snapshot_payload = _extract_payload_from_file_document(
            _read_json_file(path),
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        return self.load_snapshot(snapshot_payload, activate=activate)

    def load_snapshot_bundle_file(self, path, activate=False, verify_key=None, trusted_signers=None, min_remaining_validity_seconds=None):
        bundle_payload = _extract_payload_from_file_document(
            _read_json_file(path),
            verify_key=verify_key,
            trusted_signers=trusted_signers,
            min_remaining_validity_seconds=min_remaining_validity_seconds,
        )
        return self.load_snapshot_bundle(bundle_payload, activate=activate)

    def rollback(self, snapshot, _suppress_wal_checkpoint=False):
        with self._state_lock:
            resolved = self._resolve_snapshot(snapshot)
            self._invalidate_read_cache()
            self._clear_pending_delta()
            self._current_snapshot = resolved
            self._pending_mutations = 0
            self._bump_state_version()
            if not _suppress_wal_checkpoint:
                self._append_wal_state_checkpoint_locked("rollback")
            return resolved

    def compact(self, retain_last=None):
        with self._state_lock:
            self._invalidate_read_cache()
            if self._pending_mutations or self._current_snapshot is None:
                current = self.snapshot()
            else:
                current = self._current_snapshot

            self._compact_count += 1
            self._last_compact_retain_last = retain_last
            pruned_snapshot_count = 0

            for snapshot in self._snapshot_handles:
                object.__setattr__(snapshot, "_data_cache", None)

            if retain_last is not None:
                if type(retain_last) is not int or retain_last < 0:
                    raise ValueError("retain_last must be a non-negative integer")

                retained_handles = self._snapshot_handles[-retain_last:] if retain_last else []
                if current not in retained_handles:
                    retained_handles = [*retained_handles, current]

                retained_snapshot_ids = {snapshot.snapshot_id for snapshot in retained_handles}
                retained_snapshot_ids.update(
                    snapshot_id
                    for snapshot_id, reader_count in self._pinned_snapshot_readers.items()
                    if reader_count > 0
                )
                previous_snapshot_count = len(self._snapshot_handles)
                _snapshot_registry_prune(self._snapshots, retained_snapshot_ids)
                self._snapshot_handles = [
                    snapshot for snapshot in self._snapshot_handles if snapshot.snapshot_id in retained_snapshot_ids
                ]
                pruned_snapshot_count = previous_snapshot_count - len(self._snapshot_handles)

            self._last_compact_pruned_snapshots = pruned_snapshot_count
            self._total_pruned_snapshots += pruned_snapshot_count
            self._bump_state_version()
            self._append_wal_state_checkpoint_locked("compact")

            return current

    def diff(self, snapshot_a, snapshot_b):
        with self._state_lock:
            left_snapshot = self._resolve_snapshot(snapshot_a)
            right_snapshot = self._resolve_snapshot(snapshot_b)

            native_diff = _diff_native_persistent_roots(left_snapshot.root, right_snapshot.root)
            if native_diff is not None:
                return native_diff

            left = left_snapshot.data
            right = right_snapshot.data
            left_keys = set(left)
            right_keys = set(right)
            added_keys = sorted(right_keys - left_keys)
            removed_keys = sorted(left_keys - right_keys)
            changed_keys = sorted(
                key for key in (left_keys & right_keys) if left[key] != right[key]
            )
            return {
                "added": {key: right[key] for key in added_keys},
                "removed": {key: left[key] for key in removed_keys},
                "changed": {
                    key: {"from": left[key], "to": right[key]}
                    for key in changed_keys
                },
            }

    def stats(self):
        with self._state_lock:
            mutable_stats = dict(self._mutable.get_stats())
            pending_tombstones = int(mutable_stats.get("mutable_tombstones", 0) or 0)
            committed_stats = _committed_root_stats(self._current_snapshot)
            committed_estimated_native_bytes = int(committed_stats.get("estimated_native_bytes", 0) or 0)
            committed_active_child_bytes = int(committed_stats.get("active_child_bytes", 0) or 0)
            committed_value_pool_bytes = int(committed_stats.get("value_pool_bytes", 0) or 0)
            committed_physical_nodes = int(committed_stats.get("physical_nodes", 0) or 0)
            active_reader_count = sum(self._pinned_snapshot_readers.values())
            background_tasks_pending = (
                self._background_tasks_submitted
                - self._background_tasks_completed
                - self._background_tasks_failed
            )
            mutable_stats.update(
                {
                    "model": "lqft-map",
                    "logical_items": len(self),
                    "estimated_native_bytes": committed_estimated_native_bytes,
                    "active_child_bytes": committed_active_child_bytes,
                    "value_pool_bytes": committed_value_pool_bytes,
                    "physical_nodes": committed_physical_nodes,
                    "committed_estimated_native_bytes": committed_estimated_native_bytes,
                    "committed_active_child_bytes": committed_active_child_bytes,
                    "committed_value_pool_bytes": committed_value_pool_bytes,
                    "committed_physical_nodes": committed_physical_nodes,
                    "committed_engine_scope": committed_stats.get("engine_scope"),
                    "estimated_bytes_per_retained_snapshot": _safe_ratio(
                        committed_estimated_native_bytes,
                        len(self._snapshot_handles),
                    ),
                    "snapshot_count": len(self._snapshot_handles),
                    "retained_snapshot_count": len(self._snapshot_handles),
                    "native_snapshot_count": len(self._snapshots),
                    "cached_snapshot_mappings": sum(
                        1 for snapshot in self._snapshot_handles if snapshot._data_cache is not None
                    ),
                    "current_snapshot_id": (
                        self._current_snapshot.snapshot_id if self._current_snapshot is not None else None
                    ),
                    "pending_mutations": self._pending_mutations,
                    "pending_tombstones": pending_tombstones,
                    "pending_clear": self._pending_clear,
                    "compact_count": self._compact_count,
                    "total_pruned_snapshots": self._total_pruned_snapshots,
                    "last_compact_pruned_snapshots": self._last_compact_pruned_snapshots,
                    "last_compact_retain_last": self._last_compact_retain_last,
                    "read_cache_enabled": self._read_cache_max_entries > 0,
                    "read_cache_max_entries": self._read_cache_max_entries,
                    "read_cache_size": len(self._read_cache),
                    "read_cache_hits": self._read_cache_hits,
                    "read_cache_misses": self._read_cache_misses,
                    "read_cache_lookups": self._read_cache_hits + self._read_cache_misses,
                    "read_cache_hit_rate": _safe_ratio(
                        self._read_cache_hits,
                        self._read_cache_hits + self._read_cache_misses,
                    ),
                    "read_cache_evictions": self._read_cache_evictions,
                    "mutable_model": mutable_stats.get("model"),
                    "mutable_engine_scope": mutable_stats.get("engine_scope"),
                    "active_reader_count": active_reader_count,
                    "pinned_snapshot_reader_count": len(self._pinned_snapshot_readers),
                    "pinned_snapshot_ids": tuple(sorted(self._pinned_snapshot_readers)),
                    "background_worker_enabled": self._background_executor is not None,
                    "background_tasks_submitted": self._background_tasks_submitted,
                    "background_tasks_completed": self._background_tasks_completed,
                    "background_tasks_failed": self._background_tasks_failed,
                    "background_tasks_pending": max(0, background_tasks_pending),
                    "write_ahead_log_enabled": self._wal_path is not None,
                    "write_ahead_log_path": self._wal_path,
                    "write_ahead_log_fsync": self._wal_fsync,
                    "write_ahead_log_next_sequence": self._wal_next_sequence,
                    "write_ahead_log_records_written": self._wal_records_written,
                }
            )
            mutable_stats.update(_compute_retained_snapshot_subtree_metrics(self._snapshot_handles))
            for key, default in _COMMITTED_PHASE5_STAT_DEFAULTS.items():
                value = committed_stats.get(key, default)
                mutable_stats[key] = value
                mutable_stats[f"committed_{key}"] = value
            if self._current_snapshot is not None:
                mutable_stats["snapshot_generation"] = self._current_snapshot.generation
                mutable_stats["snapshot_size"] = self._current_snapshot.size
            return mutable_stats

    def export_current_state_payload(self):
        with self._state_lock:
            if self._pending_clear or self._current_snapshot is None:
                state = {}
                base_snapshot_id = None
            else:
                state = dict(self._current_snapshot.data)
                base_snapshot_id = self._current_snapshot.snapshot_id

            for key in _export_mutable_tombstones(self._mutable):
                state.pop(key, None)

            mutable_keys, mutable_values = _export_mutable_items(self._mutable)
            for key, value in zip(mutable_keys, mutable_values):
                state[key] = value

            items = [
                {"key": key, "value": value}
                for key, value in sorted(state.items())
            ]
            return {
                "format": "lqft-current-state-v1",
                "metadata": {
                    "base_snapshot_id": base_snapshot_id,
                    "logical_items": len(items),
                    "pending_mutations": self._pending_mutations,
                    "exported_at_ns": time.time_ns(),
                },
                "items": items,
            }

    def get_stats(self):
        return self.stats()

    def latest_snapshot(self):
        with self._state_lock:
            return self._current_snapshot

    def __len__(self):
        with self._state_lock:
            return _logical_snapshot_size(
                self._current_snapshot,
                self._mutable,
                pending_clear=self._pending_clear,
            )

    def __setitem__(self, key, value):
        self.put(key, value)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key):
        if not self.contains(key):
            raise KeyError(key)
        self.delete(key)


def _stable_shard_index(key, shard_count):
    if type(shard_count) is not int or shard_count <= 0:
        raise ValueError("shard_count must be a positive integer")
    if isinstance(key, bytes):
        payload = key
    else:
        payload = str(key).encode("utf-8", "surrogatepass")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


@dataclass(frozen=True, slots=True)
class LQFTShardedSnapshot:
    shard_snapshots: tuple
    shard_count: int
    created_at: str

    def _route(self, key):
        return _stable_shard_index(key, self.shard_count)

    def get(self, key, default=None):
        snapshot = self.shard_snapshots[self._route(key)]
        return snapshot.get(key, default)

    def contains(self, key):
        snapshot = self.shard_snapshots[self._route(key)]
        return snapshot.contains(key)

    def export(self):
        return {
            "format": "lqft-sharded-snapshot-v1",
            "metadata": {
                "shard_count": self.shard_count,
                "created_at": self.created_at,
                "snapshot_count": len(self.shard_snapshots),
            },
            "shards": [snapshot.export() for snapshot in self.shard_snapshots],
        }


class LQFTShardedMap:
    def __init__(self, shard_count=16, migration_threshold=50000):
        if type(shard_count) is not int or shard_count <= 0:
            raise ValueError("shard_count must be a positive integer")
        self.shard_count = shard_count
        self.migration_threshold = migration_threshold
        self._shards = [
            LQFTMap(migration_threshold=migration_threshold)
            for _ in range(shard_count)
        ]

    def _shard_index(self, key):
        return _stable_shard_index(key, self.shard_count)

    def _shard_for_key(self, key):
        return self._shards[self._shard_index(key)]

    def put(self, key, value):
        self._shard_for_key(key).put(key, value)

    def insert(self, key, value):
        self.put(key, value)

    def get(self, key, default=None):
        return self._shard_for_key(key).get(key, default)

    def search(self, key):
        return self.get(key)

    def delete(self, key):
        self._shard_for_key(key).delete(key)

    def remove(self, key):
        self.delete(key)

    def contains(self, key):
        return self._shard_for_key(key).contains(key)

    def clear(self):
        for shard in self._shards:
            shard.clear()

    def snapshot(self):
        snapshots = tuple(shard.snapshot() for shard in self._shards)
        return LQFTShardedSnapshot(
            shard_snapshots=snapshots,
            shard_count=self.shard_count,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def export_current_state_payload(self):
        items = []
        for shard in self._shards:
            payload = shard.export_current_state_payload()
            items.extend(payload.get("items", []))
        items.sort(key=lambda entry: str(entry.get("key")))
        return {
            "format": "lqft-sharded-current-state-v1",
            "metadata": {
                "shard_count": self.shard_count,
                "logical_items": len(items),
                "exported_at_ns": time.time_ns(),
            },
            "items": items,
        }

    def stats(self):
        shard_stats = [shard.stats() for shard in self._shards]
        shard_item_counts = [int(stats.get("logical_items", 0) or 0) for stats in shard_stats]
        total_items = sum(shard_item_counts)
        pending_mutations = sum(int(stats.get("pending_mutations", 0) or 0) for stats in shard_stats)
        snapshot_count = sum(int(stats.get("snapshot_count", 0) or 0) for stats in shard_stats)
        total_native_bytes = sum(int(stats.get("estimated_native_bytes", 0) or 0) for stats in shard_stats)
        return {
            "model": "lqft-sharded-map",
            "shard_count": self.shard_count,
            "logical_items": total_items,
            "estimated_native_bytes": total_native_bytes,
            "pending_mutations": pending_mutations,
            "snapshot_count": snapshot_count,
            "shard_item_counts": tuple(shard_item_counts),
            "max_shard_items": max(shard_item_counts) if shard_item_counts else 0,
            "min_shard_items": min(shard_item_counts) if shard_item_counts else 0,
            "avg_shard_items": _safe_ratio(total_items, self.shard_count),
        }

    def get_stats(self):
        return self.stats()

    def shard_for_key(self, key):
        return self._shard_index(key)

    def shard(self, index):
        if type(index) is not int or index < 0 or index >= self.shard_count:
            raise IndexError("shard index out of range")
        return self._shards[index]

    def shutdown_background_worker(self, wait=True, cancel_futures=False):
        for shard in self._shards:
            shard.shutdown_background_worker(wait=wait, cancel_futures=cancel_futures)

    def __len__(self):
        return sum(len(shard) for shard in self._shards)

    def __setitem__(self, key, value):
        self.put(key, value)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key):
        if not self.contains(key):
            raise KeyError(key)
        self.delete(key)

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
        "_native_state_insert_kv",
        "_native_state_search_key",
        "_native_state_delete_key",
        "_native_state_contains_key",
        "_native_state_bulk_insert_keys",
        "_native_state_bulk_insert_key_values",
        "_native_state_get_metrics",
        "_native_state_export_items",
        "_native_state_clear",
        "_native_state_set_reads_sealed",
        "_native_state_clone",
        "_persistent_state",
        "_pending_keys",
        "_pending_value",
        "_pending_values",
        "_pending_batch_size",
        "_use_prehash_fastpath",
        "_reads_sealed",
    )

    # F-03 & F-04: Restored migration_threshold to sync API signatures across the suite
    def __init__(self, migration_threshold=50000, _shared_with=None, _clone_from=None, _state_capsule=None):
        self.is_native = True
        # Keep destructive purge opt-in; each wrapper owns one instance-local persistent lineage.
        self.auto_purge_enabled = False
        self.max_memory_mb = 1000.0
        self.total_ops = 0
        self.migration_threshold = migration_threshold
        self._process = psutil.Process(os.getpid()) if psutil else None
        self._closed = False
        self._persistent_state = None
        self._native_state_insert_kv = getattr(lqft_c_engine, "persistent_insert_key_value", None)
        self._native_state_search_key = getattr(lqft_c_engine, "persistent_search_key", None)
        self._native_state_delete_key = getattr(lqft_c_engine, "persistent_delete_key", None)
        self._native_state_contains_key = getattr(lqft_c_engine, "persistent_contains_key", None)
        self._native_state_bulk_insert_keys = getattr(lqft_c_engine, "persistent_bulk_insert_keys", None)
        self._native_state_bulk_insert_key_values = getattr(lqft_c_engine, "persistent_bulk_insert_key_values", None)
        self._native_state_get_metrics = getattr(lqft_c_engine, "persistent_get_metrics", None)
        self._native_state_export_items = getattr(lqft_c_engine, "persistent_export_items", None)
        self._native_state_clear = getattr(lqft_c_engine, "persistent_clear", None)
        self._native_state_set_reads_sealed = getattr(lqft_c_engine, "persistent_set_reads_sealed", None)
        self._native_state_clone = getattr(lqft_c_engine, "persistent_clone_state", None)
        persistent_new = getattr(lqft_c_engine, "persistent_new", None)
        if persistent_new is None:
            raise RuntimeError(
                "Persistent LQFT requires the stateful native backend. "
                "Rebuild lqft_c_engine from this repository or install a current wheel."
            )
        source_count = sum(value is not None for value in (_shared_with, _clone_from, _state_capsule))
        if source_count > 1:
            raise ValueError("LQFT cannot be initialized with more than one native state source")
        clone_source_state = getattr(_clone_from, "_persistent_state", None)
        related_state = getattr(_shared_with, "_persistent_state", None)
        if _state_capsule is not None:
            self._persistent_state = _state_capsule
        elif clone_source_state is not None and self._native_state_clone is not None:
            self._persistent_state = self._native_state_clone(clone_source_state)
        elif related_state is not None:
            self._persistent_state = persistent_new(related_state)
        else:
            self._persistent_state = persistent_new()
        if self._persistent_state is None:
            raise RuntimeError("Persistent LQFT failed to initialize a native state handle.")
        self._pending_keys = []
        self._pending_value = None
        self._pending_values = []
        self._pending_batch_size = 8192
        self._use_prehash_fastpath = False
        self._reads_sealed = False
        with LQFT._instance_lock:
            LQFT._live_instances += 1

    def _validate_type(self, key, value=None):
        if not isinstance(key, str):
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")

    def _get_64bit_hash(self, key):
        # Process-local hash fast path; fallback stays 64-bit masked.
        return hash(key) & 0xFFFFFFFFFFFFFFFF

    def set_prehash_fastpath(self, enabled: bool):
        self._use_prehash_fastpath = bool(enabled)

    def _require_stateful_backend(self, method_name, method):
        if self._persistent_state is None or method is None:
            raise RuntimeError(
                f"Persistent LQFT requires native '{method_name}' support from the stateful backend."
            )
        return method

    def seal_reads(self):
        if self._pending_keys:
            self._flush_pending_inserts()
        state_set_reads_sealed = self._require_stateful_backend(
            "persistent_set_reads_sealed",
            self._native_state_set_reads_sealed,
        )
        state_set_reads_sealed(self._persistent_state, True)
        self._reads_sealed = True

    def unseal_reads(self):
        state_set_reads_sealed = self._require_stateful_backend(
            "persistent_set_reads_sealed",
            self._native_state_set_reads_sealed,
        )
        state_set_reads_sealed(self._persistent_state, False)
        self._reads_sealed = False

    def _flush_pending_inserts(self):
        if not self._pending_keys:
            return

        keys = self._pending_keys
        value = self._pending_value
        values = self._pending_values
        self._pending_keys = []
        self._pending_value = None
        self._pending_values = []

        state = self._persistent_state
        state_bulk_insert_key_values = self._native_state_bulk_insert_key_values
        state_bulk_insert_keys = self._native_state_bulk_insert_keys
        state_insert_kv = self._require_stateful_backend(
            "persistent_insert_key_value",
            self._native_state_insert_kv,
        )

        if values and state is not None and state_bulk_insert_key_values is not None:
            state_bulk_insert_key_values(state, keys, values)
            return

        if state is not None and state_bulk_insert_keys is not None:
            state_bulk_insert_keys(state, keys, value)
            return

        for key, item_value in zip(keys, values or [value] * len(keys)):
            state_insert_kv(state, key, item_value)

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

    def set_write_batch_size(self, batch_size: int):
        batch_size = int(batch_size)
        if batch_size <= 0:
            raise ValueError("Write batch size must be > 0.")
        if self._pending_keys:
            self._flush_pending_inserts()
        self._pending_batch_size = batch_size

    def purge(self):
        current_mb = self._current_memory_mb()
        print(f"\n[WARN CIRCUIT Breaker] Engine exceeded limit (Currently {current_mb:.1f} MB). Auto-Purging!")
        self.clear()

    def get_stats(self):
        if self._pending_keys:
            self._flush_pending_inserts()
        state_get_metrics = self._require_stateful_backend(
            "persistent_get_metrics",
            self._native_state_get_metrics,
        )
        return _annotate_persistent_stats(
            state_get_metrics(self._persistent_state),
            pending_buffered_writes=len(self._pending_keys),
            reads_sealed=self._reads_sealed,
            engine_scope="instance-local",
        )

    def get_metrics(self):
        return self.get_stats()

    def export_items(self):
        if self._pending_keys:
            self._flush_pending_inserts()
        state_export_items = self._require_stateful_backend(
            "persistent_export_items",
            self._native_state_export_items,
        )
        keys, values = state_export_items(self._persistent_state)
        return list(keys), list(values)

    # F-02: Standardized Metric Mapping (Dunder Method)
    def __len__(self):
        """Allows native Python len() to fetch logical_inserts from the C-Engine."""
        stats = self.get_stats()
        # Maps directly to the sharded hardware counters in the C-kernel
        return stats.get('logical_inserts', 0)

    def clear(self):
        if self._reads_sealed:
            self.unseal_reads()
        if self._pending_keys:
            self._flush_pending_inserts()
        state_clear = self._require_stateful_backend(
            "persistent_clear",
            self._native_state_clear,
        )
        return state_clear(self._persistent_state)

    def insert(self, key, value):
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        if self._reads_sealed:
            self.unseal_reads()
        
        # Heuristic Circuit Breaker check
        if self.auto_purge_enabled:
            self.total_ops += 1
            if self.total_ops % 5000 == 0:
                current_mb = self._current_memory_mb()
                if current_mb >= self.max_memory_mb:
                    self.purge()

        state_insert_kv = self._require_stateful_backend(
            "persistent_insert_key_value",
            self._native_state_insert_kv,
        )
        has_bulk_insert_key_values = self._native_state_bulk_insert_key_values is not None
        has_bulk_insert_keys = self._native_state_bulk_insert_keys is not None

        if has_bulk_insert_key_values:
            if self._pending_batch_size <= 1:
                state_insert_kv(self._persistent_state, key, value)
                return

            if self._pending_values:
                self._pending_keys.append(key)
                self._pending_values.append(value)
                if len(self._pending_keys) >= self._pending_batch_size:
                    self._flush_pending_inserts()
                return

            if has_bulk_insert_keys:
                if self._pending_value is None:
                    self._pending_value = value
                    self._pending_keys.append(key)
                elif value == self._pending_value:
                    self._pending_keys.append(key)
                else:
                    self._pending_values = [self._pending_value] * len(self._pending_keys)
                    self._pending_value = None
                    self._pending_keys.append(key)
                    self._pending_values.append(value)
                if len(self._pending_keys) >= self._pending_batch_size:
                    self._flush_pending_inserts()
                return

            self._pending_keys.append(key)
            self._pending_values.append(value)
            if len(self._pending_keys) >= self._pending_batch_size:
                self._flush_pending_inserts()
            return

        if has_bulk_insert_keys:
            if self._pending_value is None:
                self._pending_value = value
            if value != self._pending_value:
                self._flush_pending_inserts()
                self._pending_value = value
            self._pending_keys.append(key)
            if len(self._pending_keys) >= self._pending_batch_size:
                self._flush_pending_inserts()
            return

        state_insert_kv(self._persistent_state, key, value)

    def search(self, key):
        if self._pending_keys:
            self._flush_pending_inserts()
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        state_search_key = self._require_stateful_backend(
            "persistent_search_key",
            self._native_state_search_key,
        )
        return state_search_key(self._persistent_state, key)

    def remove(self, key):
        if self._pending_keys:
            self._flush_pending_inserts()
        if self._reads_sealed:
            self.unseal_reads()
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        state_delete_key = self._require_stateful_backend(
            "persistent_delete_key",
            self._native_state_delete_key,
        )
        state_delete_key(self._persistent_state, key)

    def delete(self, key):
        self.remove(key)

    def contains(self, key):
        if self._pending_keys:
            self._flush_pending_inserts()
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        state_contains_key = self._require_stateful_backend(
            "persistent_contains_key",
            self._native_state_contains_key,
        )
        return state_contains_key(self._persistent_state, key)

    def bulk_insert(self, keys, value):
        if self._reads_sealed:
            self.unseal_reads()
        if self._pending_keys:
            self._flush_pending_inserts()
        if self._native_state_bulk_insert_keys is not None:
            self._native_state_bulk_insert_keys(self._persistent_state, keys, value)
            return
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        for key in keys:
            self.insert(key, value)

    def bulk_contains_count(self, keys):
        if self._pending_keys:
            self._flush_pending_inserts()
        count = 0
        for key in keys:
            if self.contains(key):
                count += 1
        return count

    def bulk_insert_range(self, prefix, start, count, value):
        if self._reads_sealed:
            self.unseal_reads()
        if self._pending_keys:
            self._flush_pending_inserts()
        if type(prefix) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(prefix).__name__}")
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        end = int(start) + int(count)
        for i in range(int(start), end):
            self.insert(f"{prefix}{i}", value)

    def bulk_contains_range_count(self, prefix, start, count):
        if self._pending_keys:
            self._flush_pending_inserts()
        if type(prefix) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(prefix).__name__}")
        hit = 0
        end = int(start) + int(count)
        for i in range(int(start), end):
            if self.contains(f"{prefix}{i}"):
                hit += 1
        return hit

    def __setitem__(self, key, value):
        self.insert(key, value)

    def __getitem__(self, key):
        if self._pending_keys:
            self._flush_pending_inserts()
        res = self.search(key)
        if res is None:
            raise KeyError(key)
        return res

    def __delitem__(self, key):
        self.delete(key)

    def __del__(self):
        try:
            if self._pending_keys:
                self._flush_pending_inserts()
            if not self._closed:
                with LQFT._instance_lock:
                    LQFT._live_instances = max(0, LQFT._live_instances - 1)
                self._closed = True
        except Exception:
            pass

    def status(self):
        if self._pending_keys:
            self._flush_pending_inserts()
        stats = self.get_stats()
        return {
            "mode": "Strict Native C-Engine (Arena Allocator)",
            "items": stats.get('physical_nodes', 0),
            "threshold": f"{self.max_memory_mb} MB Circuit Breaker",
            "auto_purge_enabled": self.auto_purge_enabled,
        }

class _FallbackMutableLQFT:
    __slots__ = (
        "is_native",
        "auto_purge_enabled",
        "max_memory_mb",
        "total_ops",
        "migration_threshold",
        "_native_frontend",
        "_native_insert_kv",
        "_native_search_key",
        "_native_delete_key",
        "_native_contains_key",
        "_native_clear",
        "_native_len",
        "_native_metrics",
        "_native_export_items",
        "_tombstones",
        "_data",
    )

    def __init__(self, migration_threshold=50000):
        self._native_frontend = False
        self._native_insert_kv = None
        self._native_search_key = None
        self._native_delete_key = None
        self._native_contains_key = None
        self._native_clear = None
        self._native_len = None
        self._native_metrics = None
        self._native_export_items = None
        self.is_native = False
        self.auto_purge_enabled = False
        self.max_memory_mb = 1000.0
        self.total_ops = 0
        self.migration_threshold = migration_threshold
        self._tombstones = set()
        self._data = {}

    def _validate_type(self, key, value=None):
        if type(key) is not str:
            raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
        if value is not None and type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")

    def set_prehash_fastpath(self, enabled: bool):
        return None

    def seal_reads(self):
        return None

    def unseal_reads(self):
        return None

    def set_auto_purge_threshold(self, threshold: float):
        threshold = float(threshold)
        if threshold <= 0:
            raise ValueError("Auto-purge threshold must be > 0 MB.")
        self.max_memory_mb = threshold
        self.auto_purge_enabled = True

    def disable_auto_purge(self):
        self.auto_purge_enabled = False

    def set_write_batch_size(self, batch_size: int):
        batch_size = int(batch_size)
        if batch_size <= 0:
            raise ValueError("Write batch size must be > 0.")

    def purge(self):
        self.clear()

    def clear(self):
        self._data.clear()
        self._tombstones.clear()

    def insert(self, key, value):
        self._validate_type(key, value)
        self._data[key] = value
        self._tombstones.discard(key)

    def search(self, key):
        self._validate_type(key)
        return self._data.get(key)

    def remove(self, key):
        self._validate_type(key)
        self._data.pop(key, None)
        self._tombstones.discard(key)

    def delete(self, key):
        self.remove(key)

    def mark_deleted(self, key):
        self._validate_type(key)
        self._data.pop(key, None)
        self._tombstones.add(key)

    def contains(self, key):
        self._validate_type(key)
        return key in self._data

    def has_tombstone(self, key):
        self._validate_type(key)
        return key in self._tombstones

    def bulk_insert(self, keys, value):
        if type(value) is not str:
            raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")
        for key in keys:
            self.insert(key, value)

    def bulk_contains_count(self, keys):
        hit = 0
        for key in keys:
            if self.contains(key):
                hit += 1
        return hit

    def bulk_insert_range(self, prefix, start, count, value):
        self._validate_type(prefix, value)
        end = int(start) + int(count)
        for i in range(int(start), end):
            self.insert(f"{prefix}{i}", value)

    def bulk_contains_range_count(self, prefix, start, count):
        self._validate_type(prefix)
        hit = 0
        end = int(start) + int(count)
        for i in range(int(start), end):
            if f"{prefix}{i}" in self._data:
                hit += 1
        return hit

    def export_items(self):
        keys = list(self._data.keys())
        return keys, [self._data[key] for key in keys]

    def export_tombstones(self):
        return list(self._tombstones)

    def clone(self):
        cloned = type(self)(migration_threshold=self.migration_threshold)
        cloned._data.update(self._data)
        cloned._tombstones.update(self._tombstones)
        cloned.auto_purge_enabled = self.auto_purge_enabled
        cloned.max_memory_mb = self.max_memory_mb
        cloned.total_ops = self.total_ops
        return cloned

    def freeze(self, target=None):
        target = target or _new_native_lqft(self.migration_threshold)
        _apply_mutable_delta_into_persistent(self, target)
        return target

    def get_stats(self):
        return _annotate_mutable_stats({
            "logical_inserts": len(self._data),
            "physical_nodes": len(self._data),
            "mutable_tombstones": len(self._tombstones),
            "frontend": "python-dict",
        }, model="mutable-python-dict", engine_scope="instance-local")

    def status(self):
        return {
            "mode": "Mutable Python dict frontend",
            "items": len(self._data),
            "threshold": f"{self.max_memory_mb} MB Circuit Breaker",
            "auto_purge_enabled": self.auto_purge_enabled,
        }

    def __len__(self):
        return len(self._data)

    def __setitem__(self, key, value):
        self.insert(key, value)

    def __getitem__(self, key):
        result = self.search(key)
        if result is None:
            raise KeyError(key)
        return result

    def __delitem__(self, key):
        if not self.contains(key):
            raise KeyError(key)
        self.delete(key)


if hasattr(lqft_c_engine, "NativeMutableLQFT"):
    class _NativeMutableLQFT(lqft_c_engine.NativeMutableLQFT):
        __slots__ = (
            "is_native",
            "auto_purge_enabled",
            "max_memory_mb",
            "total_ops",
            "migration_threshold",
        )

        def __init__(self, migration_threshold=50000):
            super().__init__()
            self.is_native = True
            self.auto_purge_enabled = False
            self.max_memory_mb = 1000.0
            self.total_ops = 0
            self.migration_threshold = migration_threshold

        def set_prehash_fastpath(self, enabled: bool):
            return None

        def seal_reads(self):
            return None

        def unseal_reads(self):
            return None

        def set_auto_purge_threshold(self, threshold: float):
            threshold = float(threshold)
            if threshold <= 0:
                raise ValueError("Auto-purge threshold must be > 0 MB.")
            self.max_memory_mb = threshold
            self.auto_purge_enabled = True

        def disable_auto_purge(self):
            self.auto_purge_enabled = False

        def set_write_batch_size(self, batch_size: int):
            batch_size = int(batch_size)
            if batch_size <= 0:
                raise ValueError("Write batch size must be > 0.")

        def purge(self):
            self.clear()

        def bulk_insert(self, keys, value):
            for key in keys:
                self.insert(key, value)

        def bulk_contains_count(self, keys):
            hit = 0
            for key in keys:
                if self.contains(key):
                    hit += 1
            return hit

        def bulk_insert_range(self, prefix, start, count, value):
            end = int(start) + int(count)
            for i in range(int(start), end):
                self.insert(f"{prefix}{i}", value)

        def bulk_contains_range_count(self, prefix, start, count):
            hit = 0
            end = int(start) + int(count)
            for i in range(int(start), end):
                if self.contains(f"{prefix}{i}"):
                    hit += 1
            return hit

        def export_items(self):
            return super().export_items()

        def mark_deleted(self, key):
            return super().mark_deleted(key)

        def has_tombstone(self, key):
            return bool(super().has_tombstone(key))

        def export_tombstones(self):
            return list(super().export_tombstones())

        def clone(self):
            cloned = super().clone()
            cloned.auto_purge_enabled = self.auto_purge_enabled
            cloned.max_memory_mb = self.max_memory_mb
            cloned.total_ops = self.total_ops
            cloned.migration_threshold = self.migration_threshold
            return cloned

        def restore_persistent(self, persistent_state):
            return super().restore_persistent(persistent_state)

        def freeze(self, target=None):
            target = target or _new_native_lqft(self.migration_threshold)
            _apply_mutable_delta_into_persistent(self, target)
            return target

        def get_stats(self):
            return _annotate_mutable_stats(
                self.get_metrics(),
                model="mutable-native-hashtable",
                engine_scope="instance-local",
            )

        def status(self):
            return {
                "mode": "Mutable native C hash table",
                "items": len(self),
                "threshold": f"{self.max_memory_mb} MB Circuit Breaker",
                "auto_purge_enabled": self.auto_purge_enabled,
            }

        def __setitem__(self, key, value):
            self.insert(key, value)

        def __getitem__(self, key):
            result = self.search(key)
            if result is None:
                raise KeyError(key)
            return result

        def __delitem__(self, key):
            if not self.contains(key):
                raise KeyError(key)
            self.delete(key)
else:
    class _NativeMutableLQFT:
        __slots__ = (
            "is_native",
            "auto_purge_enabled",
            "max_memory_mb",
            "total_ops",
            "migration_threshold",
            "_native_state",
            "_native_insert_kv",
            "_native_search_key",
            "_native_delete_key",
            "_native_mark_deleted_key",
            "_native_contains_key",
            "_native_has_tombstone_key",
            "_native_clear",
            "_native_len",
            "_native_metrics",
            "_native_export_items",
            "_native_export_tombstones",
            "_native_clone",
            "_native_restore_persistent",
        )

        def __init__(self, migration_threshold=50000):
            self.is_native = True
            self.auto_purge_enabled = False
            self.max_memory_mb = 1000.0
            self.total_ops = 0
            self.migration_threshold = migration_threshold
            self._native_state = lqft_c_engine.mutable_new()
            self._native_insert_kv = lqft_c_engine.mutable_insert_key_value
            self._native_search_key = lqft_c_engine.mutable_search_key
            self._native_delete_key = lqft_c_engine.mutable_delete_key
            self._native_mark_deleted_key = lqft_c_engine.mutable_mark_deleted_key
            self._native_contains_key = lqft_c_engine.mutable_contains_key
            self._native_has_tombstone_key = lqft_c_engine.mutable_has_tombstone_key
            self._native_clear = lqft_c_engine.mutable_clear
            self._native_len = lqft_c_engine.mutable_len
            self._native_metrics = lqft_c_engine.mutable_get_metrics
            self._native_export_items = lqft_c_engine.mutable_export_items
            self._native_export_tombstones = lqft_c_engine.mutable_export_tombstones
            self._native_clone = getattr(lqft_c_engine, "mutable_clone", None)
            self._native_restore_persistent = lqft_c_engine.mutable_restore_persistent

        def _validate_type(self, key, value=None):
            if type(key) is not str:
                raise TypeError(f"LQFT keys must be strings. Received: {type(key).__name__}")
            if value is not None and type(value) is not str:
                raise TypeError(f"LQFT values must be strings. Received: {type(value).__name__}")

        def set_prehash_fastpath(self, enabled: bool):
            return None

        def seal_reads(self):
            return None

        def unseal_reads(self):
            return None

        def set_auto_purge_threshold(self, threshold: float):
            threshold = float(threshold)
            if threshold <= 0:
                raise ValueError("Auto-purge threshold must be > 0 MB.")
            self.max_memory_mb = threshold
            self.auto_purge_enabled = True

        def disable_auto_purge(self):
            self.auto_purge_enabled = False

        def set_write_batch_size(self, batch_size: int):
            batch_size = int(batch_size)
            if batch_size <= 0:
                raise ValueError("Write batch size must be > 0.")

        def purge(self):
            self.clear()

        def clear(self):
            self._native_clear(self._native_state)

        def insert(self, key, value):
            self._native_insert_kv(self._native_state, key, value)

        def search(self, key):
            return self._native_search_key(self._native_state, key)

        def remove(self, key):
            self._native_delete_key(self._native_state, key)

        def delete(self, key):
            self.remove(key)

        def mark_deleted(self, key):
            self._native_mark_deleted_key(self._native_state, key)

        def contains(self, key):
            return bool(self._native_contains_key(self._native_state, key))

        def has_tombstone(self, key):
            return bool(self._native_has_tombstone_key(self._native_state, key))

        def bulk_insert(self, keys, value):
            for key in keys:
                self.insert(key, value)

        def bulk_contains_count(self, keys):
            hit = 0
            for key in keys:
                if self.contains(key):
                    hit += 1
            return hit

        def bulk_insert_range(self, prefix, start, count, value):
            end = int(start) + int(count)
            for i in range(int(start), end):
                self.insert(f"{prefix}{i}", value)

        def bulk_contains_range_count(self, prefix, start, count):
            hit = 0
            end = int(start) + int(count)
            for i in range(int(start), end):
                if self.contains(f"{prefix}{i}"):
                    hit += 1
            return hit

        def export_items(self):
            return self._native_export_items(self._native_state)

        def export_tombstones(self):
            return list(self._native_export_tombstones(self._native_state))

        def clone(self):
            if self._native_clone is None:
                return None
            cloned = type(self).__new__(type(self))
            cloned.is_native = self.is_native
            cloned.auto_purge_enabled = self.auto_purge_enabled
            cloned.max_memory_mb = self.max_memory_mb
            cloned.total_ops = self.total_ops
            cloned.migration_threshold = self.migration_threshold
            cloned._native_state = self._native_clone(self._native_state)
            cloned._native_insert_kv = self._native_insert_kv
            cloned._native_search_key = self._native_search_key
            cloned._native_delete_key = self._native_delete_key
            cloned._native_mark_deleted_key = self._native_mark_deleted_key
            cloned._native_contains_key = self._native_contains_key
            cloned._native_has_tombstone_key = self._native_has_tombstone_key
            cloned._native_clear = self._native_clear
            cloned._native_len = self._native_len
            cloned._native_metrics = self._native_metrics
            cloned._native_export_items = self._native_export_items
            cloned._native_export_tombstones = self._native_export_tombstones
            cloned._native_clone = self._native_clone
            cloned._native_restore_persistent = self._native_restore_persistent
            return cloned

        def restore_persistent(self, persistent_state):
            self._native_restore_persistent(self._native_state, persistent_state)

        def freeze(self, target=None):
            target = target or _new_native_lqft(self.migration_threshold)
            _apply_mutable_delta_into_persistent(self, target)
            return target

        def get_stats(self):
            return _annotate_mutable_stats(
                self._native_metrics(self._native_state),
                model="mutable-native-hashtable",
                engine_scope="instance-local",
            )

        def get_metrics(self):
            return self.get_stats()

        def status(self):
            return {
                "mode": "Mutable native C hash table",
                "items": len(self),
                "threshold": f"{self.max_memory_mb} MB Circuit Breaker",
                "auto_purge_enabled": self.auto_purge_enabled,
            }

        def __len__(self):
            return int(self._native_len(self._native_state))

        def __setitem__(self, key, value):
            self.insert(key, value)

        def __getitem__(self, key):
            result = self.search(key)
            if result is None:
                raise KeyError(key)
            return result

        def __delitem__(self, key):
            if not self.contains(key):
                raise KeyError(key)
            self.delete(key)


MutableLQFT = _NativeMutableLQFT if all(
    hasattr(lqft_c_engine, name)
    for name in (
        "mutable_new",
        "mutable_insert_key_value",
        "mutable_search_key",
        "mutable_delete_key",
        "mutable_mark_deleted_key",
        "mutable_contains_key",
        "mutable_has_tombstone_key",
        "mutable_clear",
        "mutable_len",
        "mutable_get_metrics",
        "mutable_export_items",
        "mutable_export_tombstones",
    )
) else _FallbackMutableLQFT


if __name__ == "__main__":
    raise SystemExit(_main())