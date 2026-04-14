"""Execution boundary and tool gateway with pluggable sandbox backends.

This module provides the sandbox execution abstraction layer, enabling
different backends (Docker, MicroVM, etc.) to be used interchangeably.
"""

from .docker_executor import DockerSandboxExecutor
from .executor import (
    ExecutorCapabilities,
    SandboxBackendSelector,
    SandboxExecutor,
    SandboxExecutorRegistry,
)
from .gateway import ToolGateway
from .microvm_executor import MicroVMSandboxExecutor
from .microvm_stub_executor import StubMicroVMSandboxExecutor
from .sandbox import SandboxManager

__all__ = [
    # Core abstraction
    "SandboxExecutor",
    "ExecutorCapabilities",
    "SandboxExecutorRegistry",
    "SandboxBackendSelector",
    # Backend implementations
    "DockerSandboxExecutor",
    "MicroVMSandboxExecutor",
    "StubMicroVMSandboxExecutor",
    # Orchestration facade
    "SandboxManager",
    # Tool gateway
    "ToolGateway",
]
