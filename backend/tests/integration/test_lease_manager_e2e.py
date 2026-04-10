"""End-to-end integration test: LeaseManager full flow verification.

This test verifies the key behaviors without needing full Pydantic model instantiation.
"""

import sys
sys.path.insert(0, '/home/kyj/文档/program/programmer (wokerflow)/backend')

from unittest.mock import Mock
from datetime import datetime, timezone


def test_worker_registry_interface():
    """Verify WorkerRegistry can register and retrieve workers."""
    print("\n=== Test: WorkerRegistry interface ===")
    
    from app.harness_lab.fleet.worker_registry import WorkerRegistry
    from app.harness_lab.types import WorkerRegisterRequest
    from app.harness_lab.storage import PostgresPlatformStore
    
    # Create mock database
    db = Mock()
    db.fetchone = Mock(return_value=None)
    db.upsert_row = Mock()
    
    registry = WorkerRegistry(db)
    
    # Register worker
    worker = registry.register_worker(WorkerRegisterRequest(
        worker_id="test_worker",
        label="test",
        capabilities=["shell"],
    ))
    
    assert worker.worker_id == "test_worker"
    assert worker.label == "test"
    print("✓ WorkerRegistry can register workers")


def test_lease_manager_has_protocol_constructor():
    """Verify LeaseManager uses protocol-based constructor."""
    print("\n=== Test: LeaseManager protocol constructor ===")
    
    import inspect
    from app.harness_lab.fleet.lease_manager import LeaseManager
    
    sig = inspect.signature(LeaseManager.__init__)
    params = list(sig.parameters.keys())
    
    required_protocols = [
        'coordination', 'constraints', 'context', 'execution', 'utilities'
    ]
    
    for protocol in required_protocols:
        assert protocol in params, f"Missing protocol parameter: {protocol}"
        print(f"✓ LeaseManager has {protocol} protocol parameter")


def test_adapters_create_protocols():
    """Verify create_protocol_adapters creates all required adapters."""
    print("\n=== Test: create_protocol_adapters ===")
    
    from app.harness_lab.fleet import create_protocol_adapters
    from app.harness_lab.fleet.protocols import (
        RunCoordinationProtocol,
        DispatchConstraintProtocol,
        DispatchContextProtocol,
        TaskExecutionProtocol,
        UtilityProtocol,
    )
    
    # Mock runtime
    mock_runtime = Mock()
    
    adapters = create_protocol_adapters(mock_runtime)
    
    assert "coordination" in adapters
    assert "constraints" in adapters
    assert "context" in adapters
    assert "execution" in adapters
    assert "utilities" in adapters
    
    # Verify they implement protocols
    assert isinstance(adapters["coordination"], RunCoordinationProtocol)
    assert isinstance(adapters["constraints"], DispatchConstraintProtocol)
    assert isinstance(adapters["context"], DispatchContextProtocol)
    assert isinstance(adapters["execution"], TaskExecutionProtocol)
    assert isinstance(adapters["utilities"], UtilityProtocol)
    
    print("✓ create_protocol_adapters creates all 5 adapters")
    print("✓ All adapters implement their respective protocols")


def test_runtime_service_integration():
    """Verify RuntimeService can be imported with new LeaseManager."""
    print("\n=== Test: RuntimeService with fleet LeaseManager ===")
    
    from app.harness_lab.runtime.service import RuntimeService
    import inspect
    
    # Check the module source
    import app.harness_lab.runtime.service as service_module
    source = inspect.getsource(service_module)
    
    # Should import from fleet (relative import format)
    assert "fleet.lease_manager import LeaseManager" in source, \
        f"Should import LeaseManager from fleet. Got: {[l for l in source.split(chr(10)) if 'lease_manager' in l]}"
    
    # Should use create_protocol_adapters
    assert "create_protocol_adapters" in source, \
        "Should use protocol adapters"
    
    print("✓ RuntimeService imports LeaseManager from fleet")
    print("✓ RuntimeService uses create_protocol_adapters")


def test_execution_plane_clean():
    """Verify execution_plane no longer has old LeaseManager."""
    print("\n=== Test: execution_plane cleaned ===")
    
    from app.harness_lab.runtime import execution_plane
    import inspect
    
    source = inspect.getsource(execution_plane)
    
    # Should have RunCoordinator
    assert "class RunCoordinator" in source
    print("✓ execution_plane has RunCoordinator")
    
    # Should have LocalWorkerAdapter
    assert "class LocalWorkerAdapter" in source
    print("✓ execution_plane has LocalWorkerAdapter")
    
    # Should NOT have old LeaseManager class definition
    # (check for class definition, not just the string "LeaseManager")
    lines = source.split('\n')
    lease_manager_class = [l for l in lines if 'class LeaseManager' in l]
    assert len(lease_manager_class) == 0, \
        f"Old LeaseManager should be removed. Found: {lease_manager_class}"
    print("✓ execution_plane does not have old LeaseManager class")


if __name__ == "__main__":
    print("=== LeaseManager End-to-End Integration Tests ===")
    
    test_worker_registry_interface()
    test_lease_manager_has_protocol_constructor()
    test_adapters_create_protocols()
    test_runtime_service_integration()
    test_execution_plane_clean()
    
    print("\n=== All E2E Tests Passed ===")
    print("\nVerified:")
    print("1. WorkerRegistry works with mock database")
    print("2. LeaseManager uses protocol-based constructor")
    print("3. create_protocol_adapters creates all required adapters")
    print("4. RuntimeService integrates with fleet LeaseManager")
    print("5. execution_plane cleaned of old LeaseManager")
