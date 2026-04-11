"""Constraint engine subsystem.

The constraint engine provides semantic policy enforcement through a
three-stage pipeline:

1. Parser: Extracts rule intentions from natural language
2. Compiler: Produces standardized, executable rule sets
3. Verifier: Evaluates tool calls against compiled rules

Example:
    from harness_lab.constraints import ConstraintEngine
    
    engine = ConstraintEngine(database)
    
    # Create and compile a constraint document
    doc = engine.create_document(ConstraintCreateRequest(
        title="Research Guardrails",
        body="Shell commands require approval. Read-only access is allowed.",
        tags=["research"],
    ))
    
    # Verify a tool call
    result = engine.verify(
        subject="tool.shell.execute",
        payload={"command": "ls -la"},
        constraint_set_id=doc.document_id,
    )
    
    # Access detailed explanation
    print(result.explanation.final_decision)
    print(result.explanation.matched_rules)
"""

from .engine import ConstraintEngine
from .parser import ConstraintParser, ParsedRule, ParsedCondition
from .compiler import ConstraintCompiler
from .verifier import ConstraintVerifier, VerificationContext

__all__ = [
    "ConstraintEngine",
    "ConstraintParser",
    "ConstraintCompiler",
    "ConstraintVerifier",
    "ParsedRule",
    "ParsedCondition",
    "VerificationContext",
]
