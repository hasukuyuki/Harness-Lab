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
    artifact_root: str | None = Field(None, alias="HARNESS_ARTIFACT_ROOT")
    sandbox_backend: str = Field("docker", alias="HARNESS_SANDBOX_BACKEND")
    sandbox_image: str = Field("harness-lab/sandbox:local", alias="HARNESS_SANDBOX_IMAGE")
    sandbox_timeout_seconds: int = Field(20, alias="HARNESS_SANDBOX_TIMEOUT_SECONDS")
    docker_bin: str = Field("docker", alias="HARNESS_DOCKER_BIN")

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
            "HARNESS_ARTIFACT_ROOT": os.getenv("HARNESS_ARTIFACT_ROOT"),
            "HARNESS_SANDBOX_BACKEND": os.getenv("HARNESS_SANDBOX_BACKEND", "docker"),
            "HARNESS_SANDBOX_IMAGE": os.getenv("HARNESS_SANDBOX_IMAGE", "harness-lab/sandbox:local"),
            "HARNESS_SANDBOX_TIMEOUT_SECONDS": os.getenv("HARNESS_SANDBOX_TIMEOUT_SECONDS", "20"),
            "HARNESS_DOCKER_BIN": os.getenv("HARNESS_DOCKER_BIN", "docker"),
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

    def resolved_artifact_root(self) -> str | None:
        if self.artifact_root:
            return self.artifact_root
        repo_root = Path(__file__).resolve().parents[3]
        return str(repo_root / "backend" / "data" / "harness_lab" / "artifacts")
