"""Tool descriptor and execution types."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class ToolDescriptor(BaseModel):
    """Descriptor for a tool."""
    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: str
    risk_level: str
    timeout_ms: int
    side_effect_class: str
    input_schema: Dict[str, Any] = Field(default_factory=dict, alias="schema", serialization_alias="schema")


class ToolExecutionResult(BaseModel):
    """Result of tool execution."""
    ok: bool
    output: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
