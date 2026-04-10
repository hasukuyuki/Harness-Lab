from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

from .types import ArtifactRef
from .utils import ensure_parent, new_id, utc_now


class ArtifactStore(ABC):
    @abstractmethod
    def write_text(
        self,
        run_id: str,
        artifact_type: str,
        filename: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ArtifactRef:
        """Persist an artifact blob and return a stable artifact reference."""


class LocalFilesystemArtifactStore(ArtifactStore):
    def __init__(self, artifact_root: Path | str) -> None:
        self.artifact_root = Path(artifact_root)
        self.artifact_root.mkdir(parents=True, exist_ok=True)

    def write_text(
        self,
        run_id: str,
        artifact_type: str,
        filename: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ArtifactRef:
        artifact = ArtifactRef(
            artifact_id=new_id("artifact"),
            run_id=run_id,
            artifact_type=artifact_type,
            relative_path=str(Path(run_id) / artifact_type / filename),
            metadata=metadata or {},
            created_at=utc_now(),
        )
        absolute_path = self.artifact_root / artifact.relative_path
        ensure_parent(absolute_path)
        absolute_path.write_text(content, encoding="utf-8")
        return artifact
