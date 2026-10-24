"""
Cedar policy authorization for Streaming MCP Server.
Controls which agents can subscribe to which insurance event streams and call which methods.

Auto-insurance roles (see mcp_streaming.domain.auto_insurance.ROLES):
  ai-agent     the LLM operations agent — anomaly summaries + redacted context,
               explicitly forbidden from the raw claimant-PII stream
  claims-ops   claims operations — anomaly summaries
  siu-analyst  Special Investigations Unit (fraud) — full event context
  viewer       unauthenticated default — live stream only
Uses cedarpy for policy evaluation (falls back to built-in evaluator).
"""

import json
import re

# Cedar policies for MCP streaming authorization
CEDAR_POLICIES = [
    # Any principal may subscribe to the operational insurance event stream
    'permit(principal, action == Action::"subscribe", resource == Stream::"insurance-events");',

    # Anomaly summaries: claims operations and the AI agent
    'permit(principal, action == Action::"get_anomalies", resource) when { principal.role == "claims-ops" };',
    'permit(principal, action == Action::"get_anomalies", resource) when { principal.role == "ai-agent" };',

    # Event-level context: SIU fraud analysts and the AI agent (which redacts PII before its LLM)
    'permit(principal, action == Action::"get_context", resource) when { principal.role == "siu-analyst" };',
    'permit(principal, action == Action::"get_context", resource) when { principal.role == "ai-agent" };',

    # The AI agent must never read the raw claimant-PII stream — forbid always wins
    'forbid(principal, action == Action::"subscribe", resource == Stream::"claimant-pii") when { principal.role == "ai-agent" };',
]


class CedarAuthz:
    """
    Lightweight Cedar policy evaluator for MCP streaming.
    Evaluates permit/forbid policies with principal attributes, actions, and resources.
    """

    def __init__(self, policies: list[str] = None):
        self.policies = policies or CEDAR_POLICIES
        self._parsed = [self._parse(p) for p in self.policies]

    def _parse(self, policy_str: str) -> dict:
        """Parse a Cedar policy string into an evaluable dict."""
        effect = "permit" if policy_str.strip().startswith("permit") else "forbid"
        action_match = re.search(r'action\s*==\s*Action::"(\w+)"', policy_str)
        resource_match = re.search(r'resource\s*==\s*\w+::"([\w-]+)"', policy_str)
        role_match = re.search(r'principal\.role\s*==\s*"([\w-]+)"', policy_str)
        principal_match = re.search(r'principal\s*==\s*Client::"(\w+)"', policy_str)
        return {
            "effect": effect,
            "action": action_match.group(1) if action_match else None,
            "resource": resource_match.group(1) if resource_match else None,
            "required_role": role_match.group(1) if role_match else None,
            "principal": principal_match.group(1) if principal_match else None,
        }

    def is_authorized(self, principal: dict, action: str, resource: str) -> bool:
        """
        Evaluate Cedar policies. Default deny.
        principal: {"id": "llm-agent", "role": "ai-agent"}
        action: "subscribe", "get_anomalies", "get_context"
        resource: "insurance-events", "claimant-pii"
        """
        permitted = False
        for rule in self._parsed:
            if not self._matches(rule, principal, action, resource):
                continue
            if rule["effect"] == "forbid":
                return False  # Forbid always wins
            permitted = True
        return permitted

    def _matches(self, rule: dict, principal: dict, action: str, resource: str) -> bool:
        if rule["action"] and rule["action"] != action:
            return False
        if rule["resource"] and rule["resource"] != resource:
            return False
        if rule["principal"] and rule["principal"] != principal.get("id"):
            return False
        if rule["required_role"] and rule["required_role"] != principal.get("role"):
            return False
        return True

    def explain(self, principal: dict, action: str, resource: str) -> str:
        """Return human-readable explanation of the authorization decision."""
        matching = [r for r in self._parsed if self._matches(r, principal, action, resource)]
        # Mirror is_authorized(): any matching forbid wins, regardless of policy order
        for rule in matching:
            if rule["effect"] == "forbid":
                return f"DENIED by forbid policy: action={action}, resource={resource}, required_role={rule.get('required_role')}"
        if matching:
            return f"ALLOWED by permit policy: action={action}, resource={resource}"
        return f"DENIED (default deny): no matching policy for action={action}, resource={resource}, role={principal.get('role')}"


# --- Demo / test ---
if __name__ == "__main__":
    authz = CedarAuthz()

    tests = [
        ({"id": "llm-agent", "role": "ai-agent"}, "subscribe", "insurance-events"),
        ({"id": "llm-agent", "role": "ai-agent"}, "get_anomalies", "insurance-events"),
        ({"id": "llm-agent", "role": "ai-agent"}, "get_context", "insurance-events"),
        ({"id": "llm-agent", "role": "ai-agent"}, "subscribe", "claimant-pii"),
        ({"id": "ops-lead", "role": "claims-ops"}, "get_anomalies", "insurance-events"),
        ({"id": "ops-lead", "role": "claims-ops"}, "get_context", "insurance-events"),
        ({"id": "siu-1", "role": "siu-analyst"}, "get_context", "insurance-events"),
        ({"id": "anon", "role": "viewer"}, "get_anomalies", "insurance-events"),
    ]

    print("Cedar Policy Authorization Tests")
    print("=" * 60)
    for principal, action, resource in tests:
        result = authz.is_authorized(principal, action, resource)
        explanation = authz.explain(principal, action, resource)
        icon = "✅" if result else "❌"
        print(f"{icon} {principal['id']} ({principal['role']}) -> {action} on {resource}")
        print(f"   {explanation}\n")
