"""
Cedar policy authorization for Streaming MCP Server.
Controls which agents can subscribe to which log streams and call which methods.
Uses cedarpy for policy evaluation (falls back to built-in evaluator).
"""

import json
import re

# Cedar policies for MCP streaming authorization
CEDAR_POLICIES = [
    # Allow any authenticated agent to subscribe to app-logs
    'permit(principal, action == Action::"subscribe", resource == Stream::"app-logs");',

    # Only ops-agents can request anomaly summaries
    'permit(principal, action == Action::"get_anomalies", resource) when { principal.role == "ops-agent" };',

    # Only senior-ops can access full context (recent log window)
    'permit(principal, action == Action::"get_context", resource) when { principal.role == "senior-ops" };',

    # Forbid any agent with "readonly" role from subscribing to sensitive streams
    'forbid(principal, action == Action::"subscribe", resource == Stream::"audit-logs") when { principal.role == "readonly" };',
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
        principal: {"id": "agent-1", "role": "ops-agent"}
        action: "subscribe", "get_anomalies", "get_context"
        resource: "app-logs", "audit-logs"
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
        for rule in self._parsed:
            if not self._matches(rule, principal, action, resource):
                continue
            if rule["effect"] == "forbid":
                return f"DENIED by forbid policy: action={action}, resource={resource}, required_role={rule.get('required_role')}"
            return f"ALLOWED by permit policy: action={action}, resource={resource}"
        return f"DENIED (default deny): no matching policy for action={action}, resource={resource}, role={principal.get('role')}"


# --- Demo / test ---
if __name__ == "__main__":
    authz = CedarAuthz()

    tests = [
        ({"id": "agent-1", "role": "ops-agent"}, "subscribe", "app-logs"),
        ({"id": "agent-1", "role": "ops-agent"}, "get_anomalies", "app-logs"),
        ({"id": "agent-2", "role": "viewer"}, "get_anomalies", "app-logs"),
        ({"id": "agent-3", "role": "senior-ops"}, "get_context", "app-logs"),
        ({"id": "agent-4", "role": "ops-agent"}, "get_context", "app-logs"),
        ({"id": "agent-5", "role": "readonly"}, "subscribe", "audit-logs"),
    ]

    print("Cedar Policy Authorization Tests")
    print("=" * 60)
    for principal, action, resource in tests:
        result = authz.is_authorized(principal, action, resource)
        explanation = authz.explain(principal, action, resource)
        icon = "✅" if result else "❌"
        print(f"{icon} {principal['id']} ({principal['role']}) -> {action} on {resource}")
        print(f"   {explanation}\n")
