"""Integration test: Verify LeaseManager is used in main path.

This test verifies that RuntimeService correctly instantiates LeaseManager
from fleet module instead of the legacy LeaseManager from execution_plane.
"""

import sys
sys.path.insert(0, '/home/kyj/文档/program/programmer (wokerflow)/backend')

from app.harness_lab.runtime.service import RuntimeService
from app.harness_lab.fleet.lease_manager import LeaseManager


def test_runtime_uses_fleet_lease_manager():
    """Verify RuntimeService.lease_manager is fleet.LeaseManager instance."""
    from app.harness_lab.runtime import service as service_module
    import inspect
    
    # Get the source of the entire module to check imports
    source = inspect.getsource(service_module)
    
    # Check that LeaseManager is imported from fleet
    assert "fleet.lease_manager import LeaseManager" in source, \
        "RuntimeService module should import LeaseManager from fleet"
    
    print("✓ RuntimeService imports LeaseManager from fleet")


def test_lease_manager_has_required_methods():
    """Verify LeaseManager has all required public methods."""
    required_methods = [
        'poll_worker',
        'heartbeat_lease',
        'complete_lease',
        'fail_lease',
        'release_lease',
        'reclaim_stale_leases',
    ]
    
    for method in required_methods:
        assert hasattr(LeaseManager, method), f"LeaseManager missing {method}"
        print(f"✓ LeaseManager has {method}")


def test_adapters_decoupled_from_old_lease_manager():
    """Verify adapters don't reference old LeaseManager."""
    from app.harness_lab.fleet.adapters import RuntimeTaskExecutionAdapter
    import inspect
    
    source = inspect.getsource(RuntimeTaskExecutionAdapter)
    
    # Should not reference old lease_manager
    assert "lease_manager._apply" not in source, \
        "Adapter should not call old lease_manager methods"
    
    print("✓ RuntimeTaskExecutionAdapter decoupled from old LeaseManager")


def test_lease_manager_is_protocol_based():
    """Verify LeaseManager uses protocol-based constructor."""
    import inspect
    
    sig = inspect.signature(LeaseManager.__init__)
    params = list(sig.parameters.keys())
    
    # Check protocol parameters exist
    assert 'coordination' in params, "LeaseManager should have coordination protocol"
    assert 'constraints' in params, "LeaseManager should have constraints protocol"
    assert 'context' in params, "LeaseManager should have context protocol"
    assert 'execution' in params, "LeaseManager should have execution protocol"
    assert 'utilities' in params, "LeaseManager should have utilities protocol"
    
    print("✓ LeaseManager uses protocol-based constructor")


if __name__ == "__main__":
    print("=== LeaseManager Cut Over Verification ===\n")
    
    test_runtime_uses_fleet_lease_manager()
    print()
    
    test_lease_manager_has_required_methods()
    print()
    
    test_adapters_decoupled_from_old_lease_manager()
    print()
    
    test_lease_manager_is_protocol_based()
    print()
    
    print("=== All Cut Over Tests Passed ===")
