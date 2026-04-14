"""Artifact store contract tests shared across local and S3-compatible backends."""

from __future__ import annotations

import io
import json

from backend.app.harness_lab.artifact_store import LocalFilesystemArtifactStore, S3CompatibleArtifactStore
from backend.app.harness_lab.storage import SqliteTestPlatformStore


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str):  # noqa: N803
        self.objects[(Bucket, Key)] = bytes(Body)
        return {"ContentType": ContentType}

    def get_object(self, Bucket: str, Key: str):  # noqa: N803
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def head_object(self, Bucket: str, Key: str):  # noqa: N803
        if (Bucket, Key) not in self.objects:
            raise FileNotFoundError(Key)
        return {}

    def head_bucket(self, Bucket: str):  # noqa: N803
        return {}

    def list_buckets(self):  # noqa: N803
        """Return list of buckets - used for endpoint connectivity check."""
        return {"Buckets": []}

    def create_bucket(self, Bucket: str):  # noqa: N803
        """Create bucket if it doesn't exist."""
        return {}


def test_local_artifact_store_round_trip(tmp_path):
    store = LocalFilesystemArtifactStore(tmp_path / "artifacts")
    artifact = store.write_text(
        run_id="run_local",
        artifact_type="context_bundle",
        filename="bundle.json",
        content='{"ok": true}',
        metadata={"kind": "context"},
        content_type="application/json",
    )

    assert artifact.storage_backend == "local"
    assert artifact.storage_key.endswith("run_local/context_bundle/bundle.json")
    assert artifact.relative_path == artifact.storage_key
    assert artifact.content_type == "application/json"
    assert artifact.size_bytes > 0
    assert artifact.sha256
    assert store.exists(artifact) is True
    assert store.read_text(artifact) == '{"ok": true}'
    assert store.resolve_locator(artifact).endswith(artifact.storage_key)
    assert store.status().ready is True


def test_s3_compatible_artifact_store_with_fake_client():
    client = FakeS3Client()
    store = S3CompatibleArtifactStore(
        bucket="hlab-artifacts",
        prefix="runs",
        endpoint_url="http://minio.local",
        client=client,
    )

    artifact = store.write_bytes(
        run_id="run_s3",
        artifact_type="patch_stage",
        filename="diff.patch",
        content=b"patch-body",
        content_type="text/x-diff",
    )

    assert artifact.storage_backend == "s3"
    assert artifact.storage_key == "runs/run_s3/patch_stage/diff.patch"
    assert artifact.relative_path == ""
    assert artifact.content_type == "text/x-diff"
    assert store.exists(artifact) is True
    assert store.read_bytes(artifact) == b"patch-body"
    assert store.resolve_locator(artifact) == "http://minio.local/hlab-artifacts/runs/run_s3/patch_stage/diff.patch"
    status = store.status()
    assert status.backend == "s3"
    assert status.ready is True
    assert status.bucket_or_root == "hlab-artifacts"


def test_artifact_ref_compatibility_with_legacy_rows(tmp_path):
    database = SqliteTestPlatformStore(
        db_path=str(tmp_path / "harness_lab.db"),
        artifact_root=str(tmp_path / "artifacts"),
    )
    database.execute(
        """
        INSERT INTO artifacts (artifact_id, run_id, artifact_type, relative_path, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "artifact_legacy",
            "run_legacy",
            "learning_summary",
            "run_legacy/learning_summary/summary.json",
            json.dumps({"source": "legacy"}),
            "2026-04-10T00:00:00+00:00",
        ),
    )

    artifact = database.get_artifact("artifact_legacy")
    assert artifact.storage_backend == "local"
    assert artifact.storage_key == "run_legacy/learning_summary/summary.json"
    assert artifact.relative_path == "run_legacy/learning_summary/summary.json"
    assert artifact.metadata == {"source": "legacy"}
