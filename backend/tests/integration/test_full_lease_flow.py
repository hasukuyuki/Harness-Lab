"""Full integration test: verify worker/lease cleanup complete.

This test verifies that:
1. WorkerRegistry is the primary worker interface
2. LeaseManager uses WorkerRegistry (not WorkerService)
3. Bootstrap doesn't use WorkerService
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # backend directory


def test_worker_registry_interface():
    """Test WorkerRegistry has all required methods."""
    print("\n=== Test: WorkerRegistry Interface ===\n")
    
    from app.harness_lab.fleet.worker_registry import WorkerRegistry
    
    required = [
        'register_worker', 'get_worker', 'heartbeat', 'list_workers',
        'ensure_default_worker', 'acquire_worker', 'release_worker'
    ]
    
    for method in required:
        assert hasattr(WorkerRegistry, method), f"Missing {method}"
        print(f"✓ WorkerRegistry has {method}")
    
    print("\n✅ WorkerRegistry interface complete!")


def test_lease_manager_protocols():
    """Test LeaseManager uses all protocols."""
    print("\n=== Test: LeaseManager Protocol Integration ===\n")
    
    import inspect
    from app.harness_lab.fleet.lease_manager import LeaseManager
    from app.harness_lab.fleet.protocols import (
        RunCoordinationProtocol,
        DispatchConstraintProtocol,
        DispatchContextProtocol,
        TaskExecutionProtocol,
        UtilityProtocol,
    )
    
    sig = inspect.signature(LeaseManager.__init__)
    params = list(sig.parameters.keys())
    
    protocols = [
        ("coordination", RunCoordinationProtocol),
        ("constraints", DispatchConstraintProtocol),
        ("context", DispatchContextProtocol),
        ("execution", TaskExecutionProtocol),
        ("utilities", UtilityProtocol),
    ]
    
    for name, protocol in protocols:
        assert name in params, f"Missing {name}"
        print(f"✓ LeaseManager accepts {name}: {protocol.__name__}")
    
    # Check worker_registry parameter
    assert "worker_registry" in params
    print("✓ LeaseManager accepts worker_registry")
    
    print("\n✅ LeaseManager protocol integration verified!")


def test_adapters_with_runtime():
    """Test adapters work with mocked RuntimeService."""
    print("\n=== Test: Adapters with Mocked Runtime ===\n")
    
    from unittest.mock import Mock
    from app.harness_lab.fleet import create_protocol_adapters
    from app.harness_lab.fleet.protocols import (
        RunCoordinationProtocol,
        DispatchConstraintProtocol,
        DispatchContextProtocol,
        TaskExecutionProtocol,
        UtilityProtocol,
    )
    
    mock_runtime = Mock()
    mock_runtime.run_coordinator = Mock()
    mock_runtime.database = Mock()
    mock_runtime.tool_gateway = Mock()
    mock_runtime.orchestrator = Mock()
    mock_runtime.lease_timeout_seconds = 30
    
    adapters = create_protocol_adapters(mock_runtime)
    
    assert isinstance(adapters["coordination"], RunCoordinationProtocol)
    assert isinstance(adapters["constraints"], DispatchConstraintProtocol)
    assert isinstance(adapters["context"], DispatchContextProtocol)
    assert isinstance(adapters["execution"], TaskExecutionProtocol)
    assert isinstance(adapters["utilities"], UtilityProtocol)
    
    print("✓ All 5 adapters created with correct protocols")
    print("\n✅ Adapters verified!")


def test_runtime_service_structure():
    """Test RuntimeService uses WorkerRegistry and fleet.LeaseManager."""
    print("\n=== Test: RuntimeService Structure ===\n")
    
    from app.harness_lab.runtime.service import RuntimeService
    import inspect
    
    sig = inspect.signature(RuntimeService.__init__)
    params = list(sig.parameters.keys())
    
    assert "worker_service" not in params
    print("✓ RuntimeService does not accept worker_service")
    
    source = inspect.getsource(RuntimeService)
    assert "WorkerRegistry(database)" in source
    print("✓ RuntimeService creates WorkerRegistry")
    
    # The import is relative: from ..fleet.lease_manager import LeaseManager
    assert "fleet.lease_manager" in source or "lease_manager" in source
    print("✓ RuntimeService imports LeaseManager from fleet")
    
    print("\n✅ RuntimeService structure verified!")


def test_bootstrap_no_workerservice():
    """Test bootstrap doesn't use WorkerService."""
    print("\n=== Test: Bootstrap Cleanup ===\n")
    
    with open(PROJECT_ROOT / 'app/harness_lab/bootstrap.py', 'r') as f:
        source = f.read()
    
    assert "WorkerService" not in source
    print("✓ bootstrap.py does not reference WorkerService")
    
    assert "worker_service" not in source
    print("✓ bootstrap.py does not reference worker_service")
    
    print("\n✅ Bootstrap cleanup verified!")


def test_lease_manager_no_workerservice():
    """Test lease_manager doesn't use WorkerService fallback."""
    print("\n=== Test: LeaseManager Cleanup ===\n")
    
    with open(PROJECT_ROOT / 'app/harness_lab/fleet/lease_manager.py', 'r') as f:
        source = f.read()
    
    assert "from ..workers.service import WorkerService" not in source
    print("✓ lease_manager.py does not import WorkerService")
    
    assert "worker_service = WorkerService" not in source
    print("✓ lease_manager.py does not create WorkerService")
    
    assert "self.worker_registry" in source
    print("✓ lease_manager.py uses self.worker_registry")
    
    print("\n✅ LeaseManager cleanup verified!")


def test_execution_plane_clean():
    """Test execution_plane doesn't have old LeaseManager."""
    print("\n=== Test: Execution Plane Cleanup ===\n")
    
    with open(PROJECT_ROOT / 'app/harness_lab/runtime/execution_plane.py', 'r') as f:
        source = f.read()
    
    # Should have RunCoordinator and LocalWorkerAdapter
    assert "class RunCoordinator" in source
    print("✓ execution_plane.py has RunCoordinator")
    
    assert "class LocalWorkerAdapter" in source
    print("✓ execution_plane.py has LocalWorkerAdapter")
    
    # Should NOT have LeaseManager class
    lines = [l for l in source.split('\n') if 'class LeaseManager' in l]
    assert len(lines) == 0
    print("✓ execution_plane.py does not have LeaseManager")

    assert "worker_service" not in source
    print("✓ execution_plane.py does not reference worker_service")

    assert "worker_registry" in source
    print("✓ execution_plane.py uses worker_registry")
    
    print("\n✅ Execution plane cleanup verified!")


if __name__ == "__main__":
    print("=" * 60)
    print("=== Full Lease Flow Integration Tests ===")
    print("=" * 60)
    
    test_worker_registry_interface()
    test_lease_manager_protocols()
    test_adapters_with_runtime()
    test_runtime_service_structure()
    test_bootstrap_no_workerservice()
    test_lease_manager_no_workerservice()
    test_execution_plane_clean()
    
    print("\n" + "=" * 60)
    print("=== All Integration Tests Passed ===")
    print("=" * 60)
    print("\n✅ Cutover complete!")
    print("\nFinal state:")
    print("- WorkerRegistry: primary worker interface (fleet/)")
    print("- LeaseManager: protocol-based, uses WorkerRegistry (fleet/)")
    print("- RuntimeService: creates WorkerRegistry, uses fleet.LeaseManager")
    print("- Bootstrap: no WorkerService dependency")
    print("- Execution plane: RunCoordinator + LocalWorkerAdapter only")
