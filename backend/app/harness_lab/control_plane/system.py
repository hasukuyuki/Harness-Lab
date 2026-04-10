from __future__ import annotations

from fastapi import APIRouter

from ..bootstrap import harness_lab_services

router = APIRouter(tags=["system"])


@router.get("/api/settings/catalog")
async def settings_catalog():
    provider_settings = harness_lab_services.runtime.get_model_provider_settings()
    execution = harness_lab_services.runtime.execution_plane_status()
    knowledge = harness_lab_services.knowledge.status()
    return {
        "success": True,
        "data": {
            "constraints": [item.model_dump() for item in harness_lab_services.constraint_engine.list_documents()],
            "context_profiles": [item.model_dump() for item in harness_lab_services.runtime.list_context_profiles()],
            "prompt_templates": [item.model_dump() for item in harness_lab_services.runtime.list_prompt_templates()],
            "model_profiles": [item.model_dump() for item in harness_lab_services.runtime.list_model_profiles()],
            "workflow_templates": [item.model_dump() for item in harness_lab_services.improvement.list_workflows()],
            "workers": [item.model_dump() for item in harness_lab_services.workers.list_workers()],
            "tools": [item.model_dump(by_alias=True) for item in harness_lab_services.tool_gateway.list_tools()],
            "model_provider": {
                "provider": provider_settings.provider,
                "base_url": provider_settings.base_url,
                "model_ready": provider_settings.model_ready,
                "fallback_mode": provider_settings.fallback_mode,
                "default_model_name": provider_settings.model_name,
            },
            "knowledge_index_ready": knowledge.ready,
            "knowledge_document_count": knowledge.document_count,
            "knowledge_chunk_count": knowledge.chunk_count,
            "knowledge_last_indexed_at": knowledge.last_indexed_at,
            "knowledge_index": knowledge.model_dump(),
            "execution_plane": execution,
            "sandbox": harness_lab_services.sandbox.status().model_dump(),
        },
    }


@router.get("/api/health")
async def health():
    doctor = harness_lab_services.doctor_report()
    execution = doctor["execution_plane"]
    knowledge = doctor["knowledge"]
    return {
        "success": True,
        "data": {
            "status": (
                "healthy"
                if execution["postgres_ready"]
                and execution["redis_ready"]
                and execution["docker_ready"]
                and execution["sandbox_image_ready"]
                else "degraded"
            ),
            "mode": "multi_agent_platform",
            "sessions": doctor["control_plane"]["sessions"],
            "runs": doctor["control_plane"]["runs"],
            "policies": doctor["control_plane"]["policies"],
            "workflows": doctor["control_plane"]["workflows"],
            "workers": doctor["workers"]["count"],
            "doctor_ready": doctor["doctor_ready"],
            "warnings": doctor["warnings"],
            "model_provider": doctor["provider"]["provider"],
            "model_ready": doctor["provider"]["model_ready"],
            "fallback_mode": doctor["provider"]["fallback_mode"],
            "model_profile": doctor["provider"]["model_name"],
            "base_url": doctor["provider"]["base_url"],
            "knowledge_index_ready": knowledge["ready"],
            "knowledge_document_count": knowledge["document_count"],
            "knowledge_chunk_count": knowledge["chunk_count"],
            "knowledge_last_indexed_at": knowledge["last_indexed_at"],
            "knowledge_fallback_mode": knowledge["fallback_mode"],
            "storage_backend": execution["storage_backend"],
            "postgres_ready": execution["postgres_ready"],
            "redis_ready": execution["redis_ready"],
            "ready_queue_depth": execution["ready_queue_depth"],
            "queue_depth_by_shard": execution["queue_depth_by_shard"],
            "active_leases": execution["active_leases"],
            "stale_leases": execution["stale_leases"],
            "reclaimed_leases": execution["reclaimed_leases"],
            "lease_reclaim_rate": execution["lease_reclaim_rate"],
            "late_callback_count": execution["late_callback_count"],
            "worker_count_by_state": execution["worker_count_by_state"],
            "workers_by_role": execution["workers_by_role"],
            "draining_workers": execution["draining_workers"],
            "missions_running": execution["missions_running"],
            "leases_by_status": execution["leases_by_status"],
            "last_sweep_at": execution["last_sweep_at"],
            "offline_workers": execution["offline_workers"],
            "unhealthy_workers": execution["unhealthy_workers"],
            "active_workers": execution["active_workers"],
            "stuck_runs": execution["stuck_runs"],
            "sandbox_backend": execution["sandbox_backend"],
            "docker_ready": execution["docker_ready"],
            "sandbox_image_ready": execution["sandbox_image_ready"],
            "sandbox_active_runs": execution["sandbox_active_runs"],
            "sandbox_failures": execution["sandbox_failures"],
            "sandbox_fallback_mode": execution["sandbox_fallback_mode"],
            "sandbox_last_probe_error": execution["sandbox_last_probe_error"],
        },
    }


@router.get("/api/fleet/status")
async def fleet_status():
    return {"success": True, "data": harness_lab_services.runtime.fleet_status().model_dump()}


@router.get("/api/queues")
async def queue_status():
    return {"success": True, "data": [item.model_dump() for item in harness_lab_services.runtime.queue_status()]}
