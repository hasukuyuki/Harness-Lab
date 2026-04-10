"""Fleet module: unified worker fleet coordination.

This module centralizes lease management, worker registry, and dispatch logic.

Current Status:
    - WorkerRegistry: ✅ Migrated from workers/service.py
    - Dispatcher: 🔄 Partial (queue logic migrated, matching pending)
    - LeaseManager: ✅ Protocol-based (lease_manager.py)
    - Protocols: ✅ Defined (protocols.py)
    - Adapters: ✅ Implemented (adapters.py)
"""

from .worker_registry import WorkerRegistry
from .dispatcher import Dispatcher, InMemoryDispatcher
from .lease_manager import LeaseManager
from .protocols import (
    RunCoordinationProtocol,
    DispatchConstraintProtocol,
    DispatchContextProtocol,
    TaskExecutionProtocol,
    UtilityProtocol,
)
from .adapters import (
    RuntimeCoordinationAdapter,
    RuntimeConstraintAdapter,
    RuntimeDispatchContextAdapter,
    RuntimeTaskExecutionAdapter,
    RuntimeUtilityAdapter,
    create_protocol_adapters,
)

__all__ = [
    # Core classes
    "WorkerRegistry",
    "Dispatcher",
    "InMemoryDispatcher",
    "LeaseManager",
    # Protocols
    "RunCoordinationProtocol",
    "DispatchConstraintProtocol",
    "DispatchContextProtocol",
    "TaskExecutionProtocol",
    "UtilityProtocol",
    # Adapters
    "RuntimeCoordinationAdapter",
    "RuntimeConstraintAdapter",
    "RuntimeDispatchContextAdapter",
    "RuntimeTaskExecutionAdapter",
    "RuntimeUtilityAdapter",
    "create_protocol_adapters",
]
