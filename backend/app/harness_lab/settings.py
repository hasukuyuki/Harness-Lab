from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


class HarnessLabSettings(BaseModel):
    db_url: str = Field(..., alias="HARNESS_DB_URL")
    redis_url: str = Field(..., alias="HARNESS_REDIS_URL")
    worker_poll_interval: float = Field(1.0, alias="HARNESS_WORKER_POLL_INTERVAL")
    host: str = Field("0.0.0.0", alias="HOST")
    port: int = Field(4600, alias="PORT")
    debug: bool = Field(False, alias="DEBUG")
    redis_namespace: str = Field("harness_lab", alias="HARNESS_REDIS_NAMESPACE")
    artifact_backend: str = Field("local", alias="HARNESS_ARTIFACT_BACKEND")
    artifact_root: str | None = Field(None, alias="HARNESS_ARTIFACT_ROOT")
    artifact_bucket: str = Field("harness-lab-artifacts", alias="HARNESS_ARTIFACT_BUCKET")
    artifact_prefix: str = Field("harness-lab", alias="HARNESS_ARTIFACT_PREFIX")
    aws_endpoint_url: str | None = Field(None, alias="HARNESS_AWS_ENDPOINT_URL")
    aws_region: str | None = Field("us-east-1", alias="HARNESS_AWS_REGION")
    aws_access_key_id: str | None = Field(None, alias="HARNESS_AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str | None = Field(None, alias="HARNESS_AWS_SECRET_ACCESS_KEY")
    sandbox_backend: str = Field("docker", alias="HARNESS_SANDBOX_BACKEND")
    sandbox_image: str = Field("harness-lab/sandbox:local", alias="HARNESS_SANDBOX_IMAGE")
    sandbox_timeout_seconds: int = Field(20, alias="HARNESS_SANDBOX_TIMEOUT_SECONDS")
    docker_bin: str = Field("docker", alias="HARNESS_DOCKER_BIN")
    microvm_binary: str = Field("python3", alias="HARNESS_MICROVM_BINARY")
    microvm_kernel_image: str | None = Field(None, alias="HARNESS_MICROVM_KERNEL_IMAGE")
    microvm_rootfs_image: str | None = Field(None, alias="HARNESS_MICROVM_ROOTFS_IMAGE")
    microvm_workdir: str | None = Field(None, alias="HARNESS_MICROVM_WORKDIR")
    microvm_timeout_seconds: int = Field(20, alias="HARNESS_MICROVM_TIMEOUT_SECONDS")
    microvm_enable_snapshots: bool = Field(False, alias="HARNESS_MICROVM_ENABLE_SNAPSHOTS")
    # Hardened sandbox settings
    sandbox_rootless_user: str = Field("1000:1000", alias="HARNESS_SANDBOX_ROOTLESS_USER")
    sandbox_no_new_privileges: bool = Field(True, alias="HARNESS_SANDBOX_NO_NEW_PRIVILEGES")
    sandbox_cap_drop_all: bool = Field(True, alias="HARNESS_SANDBOX_CAP_DROP_ALL")

    @classmethod
    def from_env(cls) -> "HarnessLabSettings":
        debug_value = os.getenv("DEBUG", "False")
        debug = str(debug_value).strip().lower() in {"1", "true", "yes", "on"}
        env = {
            "HARNESS_DB_URL": os.getenv("HARNESS_DB_URL", ""),
            "HARNESS_REDIS_URL": os.getenv("HARNESS_REDIS_URL", ""),
            "HARNESS_WORKER_POLL_INTERVAL": os.getenv("HARNESS_WORKER_POLL_INTERVAL", "1.0"),
            "HOST": os.getenv("HOST", "0.0.0.0"),
            "PORT": os.getenv("PORT", "4600"),
            "DEBUG": debug,
            "HARNESS_REDIS_NAMESPACE": os.getenv("HARNESS_REDIS_NAMESPACE", "harness_lab"),
            "HARNESS_ARTIFACT_BACKEND": os.getenv("HARNESS_ARTIFACT_BACKEND", "local"),
            "HARNESS_ARTIFACT_ROOT": os.getenv("HARNESS_ARTIFACT_ROOT"),
            "HARNESS_ARTIFACT_BUCKET": os.getenv("HARNESS_ARTIFACT_BUCKET", "harness-lab-artifacts"),
            "HARNESS_ARTIFACT_PREFIX": os.getenv("HARNESS_ARTIFACT_PREFIX", "harness-lab"),
            "HARNESS_AWS_ENDPOINT_URL": os.getenv("HARNESS_AWS_ENDPOINT_URL"),
            "HARNESS_AWS_REGION": os.getenv("HARNESS_AWS_REGION", "us-east-1"),
            "HARNESS_AWS_ACCESS_KEY_ID": os.getenv("HARNESS_AWS_ACCESS_KEY_ID"),
            "HARNESS_AWS_SECRET_ACCESS_KEY": os.getenv("HARNESS_AWS_SECRET_ACCESS_KEY"),
            "HARNESS_SANDBOX_BACKEND": os.getenv("HARNESS_SANDBOX_BACKEND", "docker"),
            "HARNESS_SANDBOX_IMAGE": os.getenv("HARNESS_SANDBOX_IMAGE", "harness-lab/sandbox:local"),
            "HARNESS_SANDBOX_TIMEOUT_SECONDS": os.getenv("HARNESS_SANDBOX_TIMEOUT_SECONDS", "20"),
            "HARNESS_DOCKER_BIN": os.getenv("HARNESS_DOCKER_BIN", "docker"),
            "HARNESS_MICROVM_BINARY": os.getenv("HARNESS_MICROVM_BINARY", "python3"),
            "HARNESS_MICROVM_KERNEL_IMAGE": os.getenv("HARNESS_MICROVM_KERNEL_IMAGE"),
            "HARNESS_MICROVM_ROOTFS_IMAGE": os.getenv("HARNESS_MICROVM_ROOTFS_IMAGE"),
            "HARNESS_MICROVM_WORKDIR": os.getenv("HARNESS_MICROVM_WORKDIR"),
            "HARNESS_MICROVM_TIMEOUT_SECONDS": os.getenv("HARNESS_MICROVM_TIMEOUT_SECONDS", "20"),
            "HARNESS_MICROVM_ENABLE_SNAPSHOTS": os.getenv("HARNESS_MICROVM_ENABLE_SNAPSHOTS", "false"),
            "HARNESS_SANDBOX_ROOTLESS_USER": os.getenv("HARNESS_SANDBOX_ROOTLESS_USER", "1000:1000"),
            "HARNESS_SANDBOX_NO_NEW_PRIVILEGES": os.getenv("HARNESS_SANDBOX_NO_NEW_PRIVILEGES", "true"),
            "HARNESS_SANDBOX_CAP_DROP_ALL": os.getenv("HARNESS_SANDBOX_CAP_DROP_ALL", "true"),
        }
        settings = cls.model_validate(env)
        settings.validate_runtime_backends()
        return settings

    def validate_runtime_backends(self) -> None:
        db_url = self.db_url.strip()
        if not db_url:
            raise RuntimeError("HARNESS_DB_URL is required and must point to Postgres.")
        if not db_url.startswith(("postgresql://", "postgres://")):
            raise RuntimeError("HARNESS_DB_URL must use a Postgres URL. SQLite is no longer supported for runtime state.")
        if not self.redis_url.strip():
            raise RuntimeError("HARNESS_REDIS_URL is required.")
        if self.artifact_backend not in {"local", "s3"}:
            raise RuntimeError("HARNESS_ARTIFACT_BACKEND must be either 'local' or 's3'.")
        if self.sandbox_backend not in {"docker", "microvm", "microvm_stub"}:
            raise RuntimeError("HARNESS_SANDBOX_BACKEND must be one of 'docker', 'microvm', or 'microvm_stub'.")

    def resolved_artifact_root(self) -> str | None:
        if self.artifact_root:
            return self.artifact_root
        repo_root = Path(__file__).resolve().parents[3]
        return str(repo_root / "backend" / "data" / "harness_lab" / "artifacts")

    def artifact_bucket_or_root(self) -> str:
        if self.artifact_backend == "s3":
            return self.artifact_bucket
        return self.resolved_artifact_root() or ""
