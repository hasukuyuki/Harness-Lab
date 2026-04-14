from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import psycopg
from psycopg.rows import dict_row

from .artifact_store import ArtifactStore, LocalFilesystemArtifactStore
from .types import ApprovalRequestModel, ArtifactRef, ArtifactStoreStatus, EventEnvelope, Mission, TaskAttempt, WorkerLease
from .utils import json_dumps, new_id, utc_now


POSTGRES_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        goal TEXT NOT NULL,
        status TEXT NOT NULL,
        active_policy_id TEXT NOT NULL,
        workflow_template_id TEXT,
        constraint_set_id TEXT NOT NULL,
        context_profile_id TEXT NOT NULL,
        prompt_template_id TEXT NOT NULL,
        model_profile_id TEXT NOT NULL,
        execution_mode TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        status TEXT NOT NULL,
        prompt_frame_id TEXT,
        mission_id TEXT,
        current_attempt_id TEXT,
        active_lease_id TEXT,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS constraints_documents (
        document_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        scope TEXT NOT NULL,
        status TEXT NOT NULL,
        version TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS constraint_scenarios (
        scenario_id TEXT PRIMARY KEY,
        root_document_id TEXT NOT NULL,
        name TEXT NOT NULL,
        expected_decision TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS constraint_validation_reports (
        report_id TEXT PRIMARY KEY,
        document_id TEXT NOT NULL,
        root_document_id TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS context_profiles (
        context_profile_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS prompt_templates (
        prompt_template_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS model_profiles (
        model_profile_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        provider TEXT NOT NULL,
        profile TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS harness_policies (
        policy_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        constraint_set_id TEXT NOT NULL,
        context_profile_id TEXT NOT NULL,
        prompt_template_id TEXT NOT NULL,
        model_profile_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS experiments (
        experiment_id TEXT PRIMARY KEY,
        scenario_suite TEXT NOT NULL,
        status TEXT NOT NULL,
        winner TEXT,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replays (
        replay_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        seq BIGSERIAL PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        session_id TEXT,
        run_id TEXT,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS approvals (
        approval_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        verdict_id TEXT NOT NULL,
        subject TEXT NOT NULL,
        status TEXT NOT NULL,
        decision TEXT,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS artifacts (
        artifact_id TEXT PRIMARY KEY,
        run_id TEXT,
        artifact_type TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_templates (
        workflow_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS improvement_candidates (
        candidate_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        target_id TEXT NOT NULL,
        target_version_id TEXT NOT NULL,
        publish_status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS evaluation_reports (
        evaluation_id TEXT PRIMARY KEY,
        candidate_id TEXT,
        suite TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS failure_clusters (
        cluster_id TEXT PRIMARY KEY,
        signature TEXT NOT NULL UNIQUE,
        frequency INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workers (
        worker_id TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        state TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS missions (
        mission_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        run_id TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS task_attempts (
        attempt_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        task_node_id TEXT NOT NULL,
        worker_id TEXT,
        lease_id TEXT,
        status TEXT NOT NULL,
        retry_index INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS worker_leases (
        lease_id TEXT PRIMARY KEY,
        worker_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        task_node_id TEXT NOT NULL,
        attempt_id TEXT NOT NULL,
        status TEXT NOT NULL,
        approval_token TEXT,
        expires_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id, seq)",
    "CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id, seq)",
    "CREATE INDEX IF NOT EXISTS idx_approvals_run_id ON approvals(run_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_task_attempts_run_id ON task_attempts(run_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_worker_leases_run_id ON worker_leases(run_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_worker_leases_worker_id ON worker_leases(worker_id, created_at)",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_leases_active_task
    ON worker_leases(run_id, task_node_id)
    WHERE status IN ('leased', 'running')
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_task_attempts_active_task
    ON task_attempts(run_id, task_node_id)
    WHERE status IN ('leased', 'running')
    """,
]

SQLITE_SCHEMA_SCRIPT = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    active_policy_id TEXT NOT NULL,
    workflow_template_id TEXT,
    constraint_set_id TEXT NOT NULL,
    context_profile_id TEXT NOT NULL,
    prompt_template_id TEXT NOT NULL,
    model_profile_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    status TEXT NOT NULL,
    prompt_frame_id TEXT,
    mission_id TEXT,
    current_attempt_id TEXT,
    active_lease_id TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS constraints_documents (
    document_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    scope TEXT NOT NULL,
    status TEXT NOT NULL,
    version TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS constraint_scenarios (
    scenario_id TEXT PRIMARY KEY,
    root_document_id TEXT NOT NULL,
    name TEXT NOT NULL,
    expected_decision TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS constraint_validation_reports (
    report_id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    root_document_id TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS context_profiles (
    context_profile_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prompt_templates (
    prompt_template_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_profiles (
    model_profile_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    profile TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS harness_policies (
    policy_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    constraint_set_id TEXT NOT NULL,
    context_profile_id TEXT NOT NULL,
    prompt_template_id TEXT NOT NULL,
    model_profile_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    scenario_suite TEXT NOT NULL,
    status TEXT NOT NULL,
    winner TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replays (
    replay_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT,
    run_id TEXT,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    verdict_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    status TEXT NOT NULL,
    decision TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    run_id TEXT,
    artifact_type TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_templates (
    workflow_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS improvement_candidates (
    candidate_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    target_id TEXT NOT NULL,
    target_version_id TEXT NOT NULL,
    publish_status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evaluation_reports (
    evaluation_id TEXT PRIMARY KEY,
    candidate_id TEXT,
    suite TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS failure_clusters (
    cluster_id TEXT PRIMARY KEY,
    signature TEXT NOT NULL UNIQUE,
    frequency INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    state TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS missions (
    mission_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    task_node_id TEXT NOT NULL,
    worker_id TEXT,
    lease_id TEXT,
    status TEXT NOT NULL,
    retry_index INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_leases (
    lease_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    task_node_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    status TEXT NOT NULL,
    approval_token TEXT,
    expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class PlatformStore(ABC):
    """Truth-source store for Harness Lab control-plane state."""

    backend_name = "unknown"

    def __init__(self, artifact_root: Optional[str] = None, artifact_store: ArtifactStore | None = None) -> None:
        self.repo_root = Path(__file__).resolve().parents[3]
        self.data_dir = self.repo_root / "backend" / "data" / "harness_lab"
        self.artifact_root = Path(artifact_root) if artifact_root else self.data_dir / "artifacts"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.artifact_store: ArtifactStore = artifact_store or LocalFilesystemArtifactStore(self.artifact_root)

    @abstractmethod
    def ping(self) -> None:
        """Raise if the backing store is unavailable."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""

    @contextmanager
    @abstractmethod
    def connection(self) -> Iterator[Any]:
        """Yield a DB connection."""

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with self.connection() as conn:
            yield conn

    @abstractmethod
    def execute(self, query: str, params: tuple = (), conn: Any | None = None) -> None:
        """Execute a query."""

    @abstractmethod
    def fetchone(self, query: str, params: tuple = (), conn: Any | None = None) -> Optional[Dict[str, Any]]:
        """Fetch one row as dict."""

    @abstractmethod
    def fetchall(self, query: str, params: tuple = (), conn: Any | None = None) -> List[Dict[str, Any]]:
        """Fetch many rows as dicts."""

    def upsert_row(self, table: str, payload: Dict[str, Any], conflict_field: str, conn: Any | None = None) -> None:
        columns = list(payload.keys())
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(f"{column}=excluded.{column}" for column in columns if column != conflict_field)
        query = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_field}) DO UPDATE SET {updates}"
        )
        self.execute(query, tuple(payload[column] for column in columns), conn=conn)

    def append_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
        session_id: Optional[str] = None,
        run_id: Optional[str] = None,
        conn: Any | None = None,
    ) -> EventEnvelope:
        model = EventEnvelope(
            seq=0,
            event_id=new_id("event"),
            session_id=session_id,
            run_id=run_id,
            event_type=event_type,
            payload=payload,
            created_at=utc_now(),
        )
        self.execute(
            """
            INSERT INTO events (event_id, session_id, run_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                model.event_id,
                model.session_id,
                model.run_id,
                model.event_type,
                json_dumps(model.payload),
                model.created_at,
            ),
            conn=conn,
        )
        row = self.fetchone("SELECT * FROM events WHERE event_id = ?", (model.event_id,), conn=conn)
        return EventEnvelope(
            seq=row["seq"],
            event_id=row["event_id"],
            session_id=row["session_id"],
            run_id=row["run_id"],
            event_type=row["event_type"],
            payload=json.loads(row["payload_json"]),
            created_at=row["created_at"],
        )

    def list_events(
        self,
        session_id: Optional[str] = None,
        run_id: Optional[str] = None,
        after_seq: int = 0,
        limit: int = 200,
        conn: Any | None = None,
    ) -> List[EventEnvelope]:
        clauses = ["seq > ?"]
        params: List[Any] = [after_seq]
        if session_id:
            clauses.append("session_id = ?")
            params.append(session_id)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        rows = self.fetchall(
            f"SELECT * FROM events WHERE {' AND '.join(clauses)} ORDER BY seq ASC LIMIT ?",
            tuple(params + [limit]),
            conn=conn,
        )
        return [
            EventEnvelope(
                seq=row["seq"],
                event_id=row["event_id"],
                session_id=row["session_id"],
                run_id=row["run_id"],
                event_type=row["event_type"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def artifact_status(self) -> ArtifactStoreStatus:
        return self.artifact_store.status()

    def write_artifact_text(
        self,
        run_id: str,
        artifact_type: str,
        filename: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
    ) -> ArtifactRef:
        artifact = self.artifact_store.write_text(
            run_id=run_id,
            artifact_type=artifact_type,
            filename=filename,
            content=content,
            metadata=metadata,
            content_type=content_type,
        )
        self.record_artifact_ref(artifact)
        return artifact

    def write_artifact_bytes(
        self,
        run_id: str,
        artifact_type: str,
        filename: str,
        content: bytes,
        metadata: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
    ) -> ArtifactRef:
        artifact = self.artifact_store.write_bytes(
            run_id=run_id,
            artifact_type=artifact_type,
            filename=filename,
            content=content,
            metadata=metadata,
            content_type=content_type,
        )
        self.record_artifact_ref(artifact)
        return artifact

    def get_artifact(self, artifact_id: str, conn: Any | None = None) -> ArtifactRef:
        row = self.fetchone("SELECT * FROM artifacts WHERE artifact_id = ?", (artifact_id,), conn=conn)
        if not row:
            raise ValueError("Artifact not found")
        return self._artifact_from_row(row)

    def list_artifacts(self, run_id: Optional[str] = None, conn: Any | None = None) -> List[ArtifactRef]:
        if run_id:
            rows = self.fetchall("SELECT * FROM artifacts WHERE run_id = ? ORDER BY created_at DESC", (run_id,), conn=conn)
        else:
            rows = self.fetchall("SELECT * FROM artifacts ORDER BY created_at DESC", conn=conn)
        return [self._artifact_from_row(row) for row in rows]

    def read_artifact_text(self, artifact_id: str, conn: Any | None = None) -> str:
        artifact = self.get_artifact(artifact_id, conn=conn)
        return self.artifact_store.read_text(artifact)

    def read_artifact_bytes(self, artifact_id: str, conn: Any | None = None) -> bytes:
        artifact = self.get_artifact(artifact_id, conn=conn)
        return self.artifact_store.read_bytes(artifact)

    def record_artifact_ref(self, artifact: ArtifactRef, conn: Any | None = None) -> None:
        self.upsert_row(
            "artifacts",
            {
                "artifact_id": artifact.artifact_id,
                "run_id": artifact.run_id,
                "artifact_type": artifact.artifact_type,
                "relative_path": artifact.relative_path or artifact.storage_key,
                "payload_json": json_dumps(artifact.model_dump()),
                "created_at": artifact.created_at,
            },
            "artifact_id",
            conn=conn,
        )

    def _artifact_from_row(self, row: Dict[str, Any]) -> ArtifactRef:
        payload = json.loads(row["payload_json"]) if row.get("payload_json") else {}
        if payload.get("artifact_id"):
            payload.setdefault("run_id", row["run_id"])
            payload.setdefault("artifact_type", row["artifact_type"])
            payload.setdefault("storage_backend", "local")
            payload.setdefault("storage_key", row["relative_path"])
            payload.setdefault("relative_path", row["relative_path"])
            payload.setdefault("created_at", row["created_at"])
            return ArtifactRef(**payload)
        return ArtifactRef(
            artifact_id=row["artifact_id"],
            run_id=row["run_id"],
            artifact_type=row["artifact_type"],
            storage_backend="local",
            storage_key=row["relative_path"],
            relative_path=row["relative_path"],
            metadata=payload,
            created_at=row["created_at"],
        )

    def create_approval(
        self,
        run_id: str,
        verdict_id: str,
        subject: str,
        summary: str,
        payload: Dict[str, Any],
        conn: Any | None = None,
    ) -> ApprovalRequestModel:
        approval = ApprovalRequestModel(
            approval_id=new_id("approval"),
            run_id=run_id,
            verdict_id=verdict_id,
            subject=subject,
            summary=summary,
            payload=payload,
            status="pending",
            decision=None,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self.execute(
            """
            INSERT INTO approvals (
                approval_id, run_id, verdict_id, subject, status, decision, payload_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                approval.approval_id,
                approval.run_id,
                approval.verdict_id,
                approval.subject,
                approval.status,
                approval.decision,
                json_dumps(approval.model_dump()),
                approval.created_at,
                approval.updated_at,
            ),
            conn=conn,
        )
        return approval

    def get_approval(self, approval_id: str, conn: Any | None = None) -> ApprovalRequestModel:
        row = self.fetchone("SELECT * FROM approvals WHERE approval_id = ?", (approval_id,), conn=conn)
        if not row:
            raise ValueError("Approval not found")
        payload = json.loads(row["payload_json"])
        payload["status"] = row["status"]
        payload["decision"] = row["decision"]
        payload["updated_at"] = row["updated_at"]
        return ApprovalRequestModel(**payload)

    def list_approvals(
        self,
        run_id: Optional[str] = None,
        status: Optional[str] = None,
        conn: Any | None = None,
    ) -> List[ApprovalRequestModel]:
        clauses: List[str] = []
        params: List[Any] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.fetchall(f"SELECT * FROM approvals {where} ORDER BY created_at DESC", tuple(params), conn=conn)
        output: List[ApprovalRequestModel] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload["status"] = row["status"]
            payload["decision"] = row["decision"]
            payload["updated_at"] = row["updated_at"]
            output.append(ApprovalRequestModel(**payload))
        return output

    def resolve_approval(self, approval_id: str, decision: str, conn: Any | None = None) -> ApprovalRequestModel:
        approval = self.get_approval(approval_id, conn=conn)
        now = utc_now()
        status = "approved" if decision in {"approve", "approve_once"} else "denied"
        approval.status = status
        approval.decision = decision
        approval.updated_at = now
        self.execute(
            "UPDATE approvals SET status = ?, decision = ?, payload_json = ?, updated_at = ? WHERE approval_id = ?",
            (status, decision, json_dumps(approval.model_dump()), now, approval_id),
            conn=conn,
        )
        return approval

    def upsert_replay(self, replay_id: str, run_id: str, payload: Dict[str, Any], conn: Any | None = None) -> None:
        now = utc_now()
        self.upsert_row(
            "replays",
            {
                "replay_id": replay_id,
                "run_id": run_id,
                "payload_json": json_dumps(payload),
                "created_at": now,
                "updated_at": now,
            },
            "replay_id",
            conn=conn,
        )

    def get_replay(self, replay_id: str, conn: Any | None = None) -> Optional[Dict[str, Any]]:
        row = self.fetchone("SELECT * FROM replays WHERE replay_id = ? OR run_id = ?", (replay_id, replay_id), conn=conn)
        if not row:
            return None
        payload = json.loads(row["payload_json"])
        payload["replay_id"] = row["replay_id"]
        payload["updated_at"] = row["updated_at"]
        return payload

    def upsert_mission(self, mission: Mission, conn: Any | None = None) -> None:
        self.upsert_row(
            "missions",
            {
                "mission_id": mission.mission_id,
                "session_id": mission.session_id,
                "run_id": mission.run_id,
                "status": mission.status,
                "payload_json": json_dumps(mission.model_dump()),
                "created_at": mission.created_at,
                "updated_at": mission.updated_at,
            },
            "mission_id",
            conn=conn,
        )

    def get_mission_by_run(self, run_id: str, conn: Any | None = None) -> Optional[Mission]:
        row = self.fetchone("SELECT payload_json FROM missions WHERE run_id = ?", (run_id,), conn=conn)
        if not row:
            return None
        return Mission(**json.loads(row["payload_json"]))

    def list_missions(self, status: Optional[str] = None, conn: Any | None = None) -> List[Mission]:
        if status:
            rows = self.fetchall(
                "SELECT payload_json FROM missions WHERE status = ? ORDER BY created_at ASC",
                (status,),
                conn=conn,
            )
        else:
            rows = self.fetchall("SELECT payload_json FROM missions ORDER BY created_at ASC", conn=conn)
        return [Mission(**json.loads(row["payload_json"])) for row in rows]

    def upsert_attempt(self, attempt: TaskAttempt, conn: Any | None = None) -> None:
        self.upsert_row(
            "task_attempts",
            {
                "attempt_id": attempt.attempt_id,
                "run_id": attempt.run_id,
                "task_node_id": attempt.task_node_id,
                "worker_id": attempt.worker_id,
                "lease_id": attempt.lease_id,
                "status": attempt.status,
                "retry_index": attempt.retry_index,
                "payload_json": json_dumps(attempt.model_dump()),
                "created_at": attempt.created_at,
                "updated_at": attempt.updated_at,
            },
            "attempt_id",
            conn=conn,
        )

    def get_attempt(self, attempt_id: str, conn: Any | None = None) -> TaskAttempt:
        row = self.fetchone("SELECT payload_json FROM task_attempts WHERE attempt_id = ?", (attempt_id,), conn=conn)
        if not row:
            raise ValueError("Task attempt not found")
        return TaskAttempt(**json.loads(row["payload_json"]))

    def list_attempts(self, run_id: Optional[str] = None, conn: Any | None = None) -> List[TaskAttempt]:
        if run_id:
            rows = self.fetchall(
                "SELECT payload_json FROM task_attempts WHERE run_id = ? ORDER BY created_at ASC",
                (run_id,),
                conn=conn,
            )
        else:
            rows = self.fetchall("SELECT payload_json FROM task_attempts ORDER BY created_at ASC", conn=conn)
        return [TaskAttempt(**json.loads(row["payload_json"])) for row in rows]

    def upsert_lease(self, lease: WorkerLease, conn: Any | None = None) -> None:
        self.upsert_row(
            "worker_leases",
            {
                "lease_id": lease.lease_id,
                "worker_id": lease.worker_id,
                "run_id": lease.run_id,
                "task_node_id": lease.task_node_id,
                "attempt_id": lease.attempt_id,
                "status": lease.status,
                "approval_token": lease.approval_token,
                "expires_at": lease.expires_at,
                "heartbeat_at": lease.heartbeat_at,
                "payload_json": json_dumps(lease.model_dump()),
                "created_at": lease.created_at,
                "updated_at": lease.updated_at,
            },
            "lease_id",
            conn=conn,
        )

    def get_lease(self, lease_id: str, conn: Any | None = None) -> WorkerLease:
        row = self.fetchone("SELECT payload_json FROM worker_leases WHERE lease_id = ?", (lease_id,), conn=conn)
        if not row:
            raise ValueError("Worker lease not found")
        return WorkerLease(**json.loads(row["payload_json"]))

    def list_leases(
        self,
        run_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        status: Optional[str] = None,
        conn: Any | None = None,
    ) -> List[WorkerLease]:
        clauses: List[str] = []
        params: List[Any] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if worker_id:
            clauses.append("worker_id = ?")
            params.append(worker_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.fetchall(
            f"SELECT payload_json FROM worker_leases {where} ORDER BY created_at ASC",
            tuple(params),
            conn=conn,
        )
        return [WorkerLease(**json.loads(row["payload_json"])) for row in rows]


class BaseSqlPlatformStore(PlatformStore):
    """Shared SQL helpers for production Postgres and test SQLite stores."""

    def __init__(self, artifact_root: Optional[str] = None, artifact_store: ArtifactStore | None = None) -> None:
        super().__init__(artifact_root=artifact_root, artifact_store=artifact_store)
        self._initialize()

    def _initialize(self) -> None:
        for statement in self._schema_statements():
            self.execute(statement)
        self._ensure_schema_evolution()

    @abstractmethod
    def _schema_statements(self) -> List[str]:
        """Return DDL statements."""

    @abstractmethod
    def _ensure_schema_evolution(self) -> None:
        """Handle light schema evolution."""

    @abstractmethod
    def _translate_query(self, query: str) -> str:
        """Convert repo SQL into backend SQL."""

    @abstractmethod
    def _normalize_rows(self, rows: Iterable[Any]) -> List[Dict[str, Any]]:
        """Convert backend rows to dicts."""


class PostgresPlatformStore(BaseSqlPlatformStore):
    backend_name = "postgresql"

    def __init__(self, db_url: str, artifact_root: Optional[str] = None, artifact_store: ArtifactStore | None = None) -> None:
        self.db_url = db_url
        super().__init__(artifact_root=artifact_root, artifact_store=artifact_store)

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        conn = psycopg.connect(self.db_url, row_factory=dict_row)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ping(self) -> None:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

    def close(self) -> None:
        return None

    def _schema_statements(self) -> List[str]:
        return POSTGRES_SCHEMA_STATEMENTS

    def _ensure_schema_evolution(self) -> None:
        statements = [
            "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS workflow_template_id TEXT",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS mission_id TEXT",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS current_attempt_id TEXT",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS active_lease_id TEXT",
            "ALTER TABLE worker_leases ADD COLUMN IF NOT EXISTS approval_token TEXT",
            "ALTER TABLE constraints_documents ADD COLUMN IF NOT EXISTS root_document_id TEXT",
            "ALTER TABLE constraints_documents ADD COLUMN IF NOT EXISTS parent_document_id TEXT",
        ]
        for statement in statements:
            self.execute(statement)

    def _translate_query(self, query: str) -> str:
        return query.replace("?", "%s")

    def _normalize_rows(self, rows: Iterable[Any]) -> List[Dict[str, Any]]:
        return [dict(row) for row in rows]

    def execute(self, query: str, params: tuple = (), conn: Any | None = None) -> None:
        translated = self._translate_query(query)
        if conn is None:
            with self.connection() as owned:
                with owned.cursor() as cursor:
                    cursor.execute(translated, params)
            return
        with conn.cursor() as cursor:
            cursor.execute(translated, params)

    def fetchone(self, query: str, params: tuple = (), conn: Any | None = None) -> Optional[Dict[str, Any]]:
        translated = self._translate_query(query)
        if conn is None:
            with self.connection() as owned:
                with owned.cursor() as cursor:
                    cursor.execute(translated, params)
                    row = cursor.fetchone()
        else:
            with conn.cursor() as cursor:
                cursor.execute(translated, params)
                row = cursor.fetchone()
        return dict(row) if row is not None else None

    def fetchall(self, query: str, params: tuple = (), conn: Any | None = None) -> List[Dict[str, Any]]:
        translated = self._translate_query(query)
        if conn is None:
            with self.connection() as owned:
                with owned.cursor() as cursor:
                    cursor.execute(translated, params)
                    rows = cursor.fetchall()
        else:
            with conn.cursor() as cursor:
                cursor.execute(translated, params)
                rows = cursor.fetchall()
        return self._normalize_rows(rows)


class SqliteTestPlatformStore(BaseSqlPlatformStore):
    """SQLite-backed store kept only for local tests and injected fakes."""

    backend_name = "sqlite_test"

    def __init__(
        self,
        db_path: Optional[str] = None,
        artifact_root: Optional[str] = None,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path else (Path(__file__).resolve().parents[3] / "backend" / "data" / "harness_lab" / "test_harness_lab.db")
        super().__init__(artifact_root=artifact_root, artifact_store=artifact_store)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def ping(self) -> None:
        with self.connection() as conn:
            conn.execute("SELECT 1").fetchone()

    def close(self) -> None:
        return None

    def _schema_statements(self) -> List[str]:
        return [statement for statement in SQLITE_SCHEMA_SCRIPT.split(";\n\n") if statement.strip()]

    def _ensure_schema_evolution(self) -> None:
        self._ensure_column("sessions", "workflow_template_id", "TEXT")
        self._ensure_column("runs", "mission_id", "TEXT")
        self._ensure_column("runs", "current_attempt_id", "TEXT")
        self._ensure_column("runs", "active_lease_id", "TEXT")
        self._ensure_column("worker_leases", "approval_token", "TEXT")
        self._ensure_column("constraints_documents", "root_document_id", "TEXT")
        self._ensure_column("constraints_documents", "parent_document_id", "TEXT")

    def _ensure_column(self, table: str, column: str, column_type: str) -> None:
        with self.connection() as conn:
            existing_columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if column in existing_columns:
                return
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def _translate_query(self, query: str) -> str:
        return query.replace(" FOR UPDATE", "")

    def _normalize_rows(self, rows: Iterable[Any]) -> List[Dict[str, Any]]:
        return [dict(row) for row in rows]

    def execute(self, query: str, params: tuple = (), conn: Any | None = None) -> None:
        translated = self._translate_query(query)
        if conn is None:
            with self.connection() as owned:
                owned.execute(translated, params)
            return
        conn.execute(translated, params)

    def fetchone(self, query: str, params: tuple = (), conn: Any | None = None) -> Optional[Dict[str, Any]]:
        translated = self._translate_query(query)
        if conn is None:
            with self.connection() as owned:
                row = owned.execute(translated, params).fetchone()
        else:
            row = conn.execute(translated, params).fetchone()
        return dict(row) if row is not None else None

    def fetchall(self, query: str, params: tuple = (), conn: Any | None = None) -> List[Dict[str, Any]]:
        translated = self._translate_query(query)
        if conn is None:
            with self.connection() as owned:
                rows = owned.execute(translated, params).fetchall()
        else:
            rows = conn.execute(translated, params).fetchall()
        return self._normalize_rows(rows)


HarnessLabDatabase = PlatformStore
