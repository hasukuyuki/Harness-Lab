"""ConstraintParser - Extracts rule intentions from natural language and tags.

The parser analyzes constraint documents and extracts structured rule intentions
without requiring a formal DSL. It uses pattern matching and heuristics to
identify tool restrictions, action classifications, and conditions.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field


@dataclass
class ParsedCondition:
    """A condition extracted from natural language."""
    field: str
    operator: str
    value: Any
    confidence: float = 1.0  # 0.0-1.0, how confident we are in this extraction
    source_text: str = ""  # The original text that led to this condition


@dataclass
class ParsedRule:
    """A rule intention extracted from natural language."""
    subject_pattern: str
    decision: str  # allow, approval_required, deny
    conditions: List[ParsedCondition] = field(default_factory=list)
    priority: int = 50
    reason_template: str = ""
    confidence: float = 1.0
    source_sentences: List[str] = field(default_factory=list)


# Tool name patterns for identifying subject references
TOOL_PATTERNS = {
    "shell": [
        r"shell\s+(?:command|execution|operation)",
        r"bash\s+(?:command|execution|operation)",
        r"terminal\s+(?:command|execution|operation)",
        r"command\s+line",
        r"\.shell\.",
    ],
    "filesystem": [
        r"file\s+(?:system|access|operation|read|write)",
        r"filesystem",
        r"\.filesystem\.",
    ],
    "git": [
        r"git\s+(?:command|operation|action)",
        r"version\s+control",
        r"\.git\.",
    ],
    "http_fetch": [
        r"http\s+(?:fetch|request|call)",
        r"network\s+(?:access|fetch)",
        r"\.http_fetch\.",
    ],
    "knowledge_search": [
        r"knowledge\s+(?:search|query|retrieval)",
        r"search\s+(?:index|knowledge)",
        r"\.knowledge_search\.",
    ],
    "model_reflection": [
        r"model\s+(?:reflection|inference|call)",
        r"llm\s+(?:call|reflection)",
        r"\.model_reflection\.",
    ],
    "mcp_proxy": [
        r"mcp\s+(?:proxy|external)",
        r"external\s+(?:proxy|call)",
        r"\.mcp_proxy",
    ],
}

# Action classification patterns
ACTION_PATTERNS = {
    "read": [
        r"read[- ]?only",
        r"read\s+(?:file|content|data)",
        r"inspect",
        r"view",
        r"list",
        r"cat\b",
        r"ls\b",
    ],
    "write": [
        r"write[- ]?(?:file|content|data)",
        r"create\s+(?:file|directory)",
        r"save\s+(?:file|data)",
    ],
    "mutate": [
        r"mutat(?:e|ion)",
        r"modify",
        r"update",
        r"change",
        r"mv\b",
        r"cp\b",
    ],
    "destructive": [
        r"destructive",
        r"dangerous",
        r"rm\b",
        r"remove",
        r"delete",
        r"chmod",
        r"chown",
        r"mkfs",
        r"dd\b",
        r"shutdown",
        r"reboot",
    ],
    "network": [
        r"network\s+(?:access|call|request)",
        r"http\s+(?:request|fetch)",
        r"external\s+(?:api|service)",
    ],
    "external": [
        r"external\s+(?:service|call|proxy)",
        r"third[- ]?party",
        r"outbound",
    ],
}

# Decision patterns
DECISION_PATTERNS = {
    "allow": [
        r"(?:is\s+|are\s+)?allowed",
        r"(?:is\s+|are\s+)?permitted",
        r"may\s+(?:be\s+)?run",
        r"can\s+(?:be\s+)?execute",
        r"without\s+approval",
        r"no\s+approval\s+needed",
    ],
    "deny": [
        r"(?:is\s+|are\s+)?denied",
        r"(?:is\s+|are\s+)?prohibited",
        r"(?:is\s+|are\s+)?forbidden",
        r"(?:is\s+|are\s+)?blocked",
        r"not\s+allowed",
        r"must\s+not",
        r"should\s+not\s+be\s+allowed",
    ],
    "approval_required": [
        r"require[s]?\s+approval",
        r"require[s]?\s+review",
        r"must\s+be\s+approved",
        r"escalate\s+to",
        r"approval\s+required",
        r"review\s+required",
    ],
}

# Condition extraction patterns
CONDITION_PATTERNS = {
    "path_prefix": [
        (r"(?:under|in|within)\s+(?:the\s+)?(?:path\s+)?[`\"']?([^`\"'\s]+)[`\"']?", "prefix"),
        (r"path\s+(?:starting\s+with|prefix)\s+[`\"']?([^`\"']+)[`\"']?", "prefix"),
        (r"(?:in|under)\s+[`\"']?(/[^`\"']*)[`\"']?", "prefix"),
    ],
    "network_mode": [
        (r"strict[- ]?network", "strict"),
        (r"network[- ]?restricted", "restricted"),
        (r"no\s+network", "none"),
    ],
    "sandbox_required": [
        (r"sandbox\s+required", True),
        (r"(?:must|should)\s+(?:run\s+)?in\s+sandbox", True),
    ],
    "git_mutability": [
        (r"git\s+(?:mutable|mutability)", "mutable"),
        (r"git\s+(?:immutable|read[- ]?only)", "immutable"),
        (r"(?:allow|permit)\s+git\s+(?:push|commit)", "mutable"),
    ],
}

# Shell command classification patterns (for heuristic fallback)
SHELL_PATTERNS = {
    "read_only": re.compile(
        r"^(?:pwd|ls|rg|find|cat|head|tail|git\s+status|git\s+diff|git\s+log|python3?\s+-m\s+compileall|echo|printf|grep)",
        re.IGNORECASE,
    ),
    "mutating": re.compile(
        r"(?:>|>>|\|\s*tee|\bmv\b|\bcp\b|\btouch\b|\bmkdir\b|\brmdir\b)",
        re.IGNORECASE,
    ),
    "destructive": re.compile(
        r"(?:^|\s)(?:rm|chmod|chown|mkfs|dd|shutdown|reboot|git\s+push|git\s+commit|git\s+reset|sed\s+-i|>\s*/[^>]|sudo)",
        re.IGNORECASE,
    ),
}


class ConstraintParser:
    """Parses natural language constraint documents into rule intentions.
    
    The parser uses pattern matching and heuristics to extract structured
    rule intentions from natural language text. It does not require a
    formal DSL, making it easy for humans to write constraints.
    
    The confidence score indicates how certain the parser is about each
    extraction, allowing the compiler to decide when to use fallback.
    """

    def __init__(self) -> None:
        self.tool_patterns = {tool: [re.compile(p, re.IGNORECASE) for p in patterns] 
                              for tool, patterns in TOOL_PATTERNS.items()}
        self.action_patterns = {action: [re.compile(p, re.IGNORECASE) for p in patterns]
                                for action, patterns in ACTION_PATTERNS.items()}
        self.decision_patterns = {decision: [re.compile(p, re.IGNORECASE) for p in patterns]
                                  for decision, patterns in DECISION_PATTERNS.items()}

    def parse(self, body: str, tags: List[str]) -> List[ParsedRule]:
        """Parse a constraint document body and tags into rule intentions.
        
        Args:
            body: The natural language constraint document
            tags: Tags associated with the document
            
        Returns:
            List of parsed rule intentions with confidence scores
        """
        rules: List[ParsedRule] = []
        
        # Parse sentences into potential rules
        sentences = self._split_sentences(body)
        
        for sentence in sentences:
            sentence_rules = self._parse_sentence(sentence, tags)
            rules.extend(sentence_rules)
        
        # Extract global rules from tags
        tag_rules = self._parse_tags(tags)
        rules.extend(tag_rules)
        
        # If no rules were parsed, create default rules based on heuristics
        if not rules:
            rules = self._create_default_rules(body, tags)
        
        return rules

    def _split_sentences(self, text: str) -> List[str]:
        """Split text into sentences."""
        # Simple sentence splitting
        sentences = re.split(r'[.!?]+', text)
        return [s.strip() for s in sentences if s.strip()]

    def _parse_sentence(self, sentence: str, tags: List[str]) -> List[ParsedRule]:
        """Parse a single sentence for rule intentions."""
        rules: List[ParsedRule] = []
        sentence_lower = sentence.lower()
        
        # Identify tools mentioned in the sentence
        mentioned_tools = self._identify_tools(sentence)
        
        # Identify actions mentioned
        mentioned_actions = self._identify_actions(sentence)
        
        # Identify decision
        decision = self._identify_decision(sentence)
        
        # If we have tools and a decision, create rules
        if mentioned_tools and decision:
            for tool in mentioned_tools:
                conditions: List[ParsedCondition] = []
                
                # Add action conditions
                for action in mentioned_actions:
                    conditions.append(ParsedCondition(
                        field="action",
                        operator="eq",
                        value=action,
                        confidence=0.8,
                        source_text=sentence,
                    ))
                
                # Extract path conditions
                path_conditions = self._extract_path_conditions(sentence)
                conditions.extend(path_conditions)
                
                # Extract network conditions
                network_conditions = self._extract_network_conditions(sentence)
                conditions.extend(network_conditions)
                
                # Extract sandbox conditions
                sandbox_conditions = self._extract_sandbox_conditions(sentence)
                conditions.extend(sandbox_conditions)
                
                # Determine subject pattern
                if mentioned_actions:
                    subject_pattern = f"tool.{tool}.*"
                else:
                    subject_pattern = f"tool.{tool}.*"
                
                # Build reason template
                reason_template = self._build_reason_template(tool, decision, mentioned_actions)
                
                rule = ParsedRule(
                    subject_pattern=subject_pattern,
                    decision=decision,
                    conditions=conditions,
                    priority=self._calculate_priority(decision, conditions),
                    reason_template=reason_template,
                    confidence=0.7 if len(conditions) > 0 else 0.5,
                    source_sentences=[sentence],
                )
                rules.append(rule)
        
        return rules

    def _identify_tools(self, text: str) -> List[str]:
        """Identify which tools are mentioned in the text."""
        mentioned = []
        text_lower = text.lower()
        
        for tool, patterns in self.tool_patterns.items():
            for pattern in patterns:
                if pattern.search(text_lower):
                    mentioned.append(tool)
                    break
        
        return mentioned

    def _identify_actions(self, text: str) -> List[str]:
        """Identify which actions are mentioned in the text."""
        mentioned = []
        text_lower = text.lower()
        
        for action, patterns in self.action_patterns.items():
            for pattern in patterns:
                if pattern.search(text_lower):
                    mentioned.append(action)
                    break
        
        return mentioned

    def _identify_decision(self, text: str) -> Optional[str]:
        """Identify the decision (allow/deny/approval_required) in the text."""
        text_lower = text.lower()
        
        # Check for deny first (highest precedence)
        for pattern in self.decision_patterns["deny"]:
            if pattern.search(text_lower):
                return "deny"
        
        # Check for approval_required
        for pattern in self.decision_patterns["approval_required"]:
            if pattern.search(text_lower):
                return "approval_required"
        
        # Check for allow
        for pattern in self.decision_patterns["allow"]:
            if pattern.search(text_lower):
                return "allow"
        
        return None

    def _extract_path_conditions(self, text: str) -> List[ParsedCondition]:
        """Extract path-related conditions from text."""
        conditions = []
        
        for pattern, operator in CONDITION_PATTERNS["path_prefix"]:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                value = match.group(1) if match.groups() else match.group(0)
                conditions.append(ParsedCondition(
                    field="path",
                    operator=operator,
                    value=value,
                    confidence=0.75,
                    source_text=match.group(0),
                ))
        
        return conditions

    def _extract_network_conditions(self, text: str) -> List[ParsedCondition]:
        """Extract network-related conditions from text."""
        conditions = []
        text_lower = text.lower()
        
        for pattern, value in CONDITION_PATTERNS["network_mode"]:
            if re.search(pattern, text_lower):
                conditions.append(ParsedCondition(
                    field="network_mode",
                    operator="eq",
                    value=value,
                    confidence=0.8,
                    source_text=text,
                ))
        
        return conditions

    def _extract_sandbox_conditions(self, text: str) -> List[ParsedCondition]:
        """Extract sandbox-related conditions from text."""
        conditions = []
        text_lower = text.lower()
        
        for pattern, value in CONDITION_PATTERNS["sandbox_required"]:
            if re.search(pattern, text_lower):
                conditions.append(ParsedCondition(
                    field="sandbox_required",
                    operator="eq",
                    value=value,
                    confidence=0.85,
                    source_text=text,
                ))
        
        return conditions

    def _build_reason_template(self, tool: str, decision: str, actions: List[str]) -> str:
        """Build a human-readable reason template."""
        action_str = ", ".join(actions) if actions else "operations"
        
        if decision == "allow":
            return f"{tool} {action_str} are allowed by policy."
        elif decision == "deny":
            return f"{tool} {action_str} are denied by policy guardrails."
        else:  # approval_required
            return f"{tool} {action_str} require operator approval."

    def _calculate_priority(self, decision: str, conditions: List[ParsedCondition]) -> int:
        """Calculate rule priority based on decision and specificity."""
        base_priority = {"deny": 10, "approval_required": 30, "allow": 50}.get(decision, 50)
        
        # More conditions = higher specificity = lower priority number
        specificity_bonus = len(conditions) * 5
        
        return max(1, base_priority - specificity_bonus)

    def _parse_tags(self, tags: List[str]) -> List[ParsedRule]:
        """Extract rules from document tags."""
        rules: List[ParsedRule] = []
        tag_set = {t.lower() for t in tags}
        
        # Check for read-only filesystem
        if "read-only" in tag_set or "read_only" in tag_set:
            rules.append(ParsedRule(
                subject_pattern="tool.filesystem.write_file",
                decision="deny",
                conditions=[ParsedCondition(
                    field="action",
                    operator="eq",
                    value="write",
                    confidence=0.9,
                    source_text="read-only tag",
                )],
                priority=5,
                reason_template="Filesystem writes are denied by read-only policy.",
                confidence=0.9,
                source_sentences=["tag: read-only"],
            ))
        
        # Check for strict network
        if "strict-network" in tag_set or "strict_network" in tag_set:
            rules.append(ParsedRule(
                subject_pattern="tool.http_fetch.*",
                decision="approval_required",
                conditions=[ParsedCondition(
                    field="network_mode",
                    operator="eq",
                    value="strict",
                    confidence=0.9,
                    source_text="strict-network tag",
                )],
                priority=15,
                reason_template="Network fetches require approval in strict-network mode.",
                confidence=0.9,
                source_sentences=["tag: strict-network"],
            ))
        
        # Check for deny-destructive
        if "deny-destructive" in tag_set or "deny_destructive" in tag_set:
            rules.append(ParsedRule(
                subject_pattern="tool.shell.*",
                decision="deny",
                conditions=[ParsedCondition(
                    field="action",
                    operator="eq",
                    value="destructive",
                    confidence=0.85,
                    source_text="deny-destructive tag",
                )],
                priority=5,
                reason_template="Destructive shell operations are denied by policy.",
                confidence=0.85,
                source_sentences=["tag: deny-destructive"],
            ))
        
        return rules

    def _create_default_rules(self, body: str, tags: List[str]) -> List[ParsedRule]:
        """Create default rules when parsing yields no explicit rules."""
        rules: List[ParsedRule] = []
        
        # Default shell rules
        rules.append(ParsedRule(
            subject_pattern="tool.shell.*",
            decision="approval_required",
            conditions=[],
            priority=50,
            reason_template="Shell commands require operator approval by default.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default filesystem read
        rules.append(ParsedRule(
            subject_pattern="tool.filesystem.read_file",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Read-only filesystem access is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        rules.append(ParsedRule(
            subject_pattern="tool.filesystem.list_dir",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Directory listing is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default filesystem write
        rules.append(ParsedRule(
            subject_pattern="tool.filesystem.write_file",
            decision="approval_required",
            conditions=[],
            priority=50,
            reason_template="Filesystem writes require operator approval.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default git read-only
        rules.append(ParsedRule(
            subject_pattern="tool.git.status",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Git status inspection is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        rules.append(ParsedRule(
            subject_pattern="tool.git.diff",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Git diff inspection is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        rules.append(ParsedRule(
            subject_pattern="tool.git.log",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Git log inspection is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default git mutable operations
        rules.append(ParsedRule(
            subject_pattern="tool.git.*",
            decision="approval_required",
            conditions=[ParsedCondition(
                field="action",
                operator="eq",
                value="mutate",
                confidence=0.5,
                source_text="default git mutable rule",
            )],
            priority=40,
            reason_template="Mutable git operations require operator approval.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default http_fetch
        rules.append(ParsedRule(
            subject_pattern="tool.http_fetch.get",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="HTTP GET is allowed by default.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default knowledge search
        rules.append(ParsedRule(
            subject_pattern="tool.knowledge_search.query",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Knowledge search is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default model reflection
        rules.append(ParsedRule(
            subject_pattern="tool.model_reflection.run",
            decision="allow",
            conditions=[],
            priority=50,
            reason_template="Model reflection is allowed.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        # Default mcp_proxy
        rules.append(ParsedRule(
            subject_pattern="tool.mcp_proxy.*",
            decision="approval_required",
            conditions=[],
            priority=30,
            reason_template="External proxy calls require operator approval.",
            confidence=0.5,
            source_sentences=["default rule"],
        ))
        
        return rules

    def classify_shell_command(self, command: str) -> Dict[str, bool]:
        """Classify a shell command by its risk level.
        
        This is used by the verifier for shell-specific rules.
        """
        if not command:
            return {"read_only": False, "mutating": False, "destructive": False}
        
        cmd_lower = command.lower()
        
        return {
            "read_only": bool(SHELL_PATTERNS["read_only"].search(cmd_lower)),
            "mutating": bool(SHELL_PATTERNS["mutating"].search(cmd_lower)),
            "destructive": bool(SHELL_PATTERNS["destructive"].search(cmd_lower)),
        }
