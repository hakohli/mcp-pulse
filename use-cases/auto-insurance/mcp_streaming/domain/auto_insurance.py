"""
Auto-insurance domain model for the streaming MCP demo.

Models the claims & telematics platform of a fictional auto insurer: connected-car
telematics, crash detection, First Notice of Loss (FNOL), policy/coverage checks,
claims workflow, fraud scoring, usage-based-insurance (UBI) rating and payouts.

Everything here is SYNTHETIC: identifiers are random (POL-/CLM-/VEH-/RS- prefixes),
there are no real people, VINs, phone numbers or addresses.

Used by:
  * simulator/log_simulator.py  — normal traffic + incident scenarios
  * agents/ai_agent.py          — system prompt, runbook search tool, PII redaction
  * llm/providers.py (mock)     — offline diagnoses from the runbooks
  * agents/agent_client*.py     — rule-based fix suggestions
  * server/cedar_authz.py       — insurance roles and streams
"""

import os
import random
import re
import time
from typing import Dict, List, Optional

# --------------------------------------------------------------------------- #
# Streams, roles, services
# --------------------------------------------------------------------------- #

TOPIC = os.environ.get("KAFKA_TOPIC", "insurance-events")
PII_STREAM = "claimant-pii"  # raw claimant PII; the AI agent is explicitly forbidden (see cedar_authz.py)

ROLES = {
    "ai-agent": "The LLM operations agent: anomaly summaries + redacted event context",
    "claims-ops": "Claims operations team: live stream + anomaly summaries",
    "siu-analyst": "Special Investigations Unit (fraud): full event context",
    "viewer": "Anonymous / unauthenticated default: live stream only",
}

SERVICES = {
    "telematics-gateway": "Ingests connected-car and mobile-app trip data (UBI)",
    "crash-detection": "Scores telematics impact events and triggers automatic FNOL",
    "fnol-service": "First Notice of Loss intake (mobile app, phone/IVR, auto-FNOL)",
    "policy-admin": "Policy and coverage verification",
    "claims-service": "Claim creation, adjuster assignment, estimates and workflow",
    "fraud-scoring": "ML fraud scoring and SIU referral",
    "rating-engine": "Usage-based-insurance premium rating",
    "payments-service": "Claim payouts and premium collection",
}

REGIONS = ["TX-DFW", "FL-MIA", "CA-LAX", "IL-CHI", "AZ-PHX", "GA-ATL", "CO-DEN"]

OWNERS = {
    "platform-sre": "Platform SRE on-call",
    "claims-ops": "Claims operations leadership",
    "ml-platform": "ML platform / model owners",
    "siu": "Special Investigations Unit",
    "payments-eng": "Payments engineering",
    "cat-response": "Catastrophe (CAT) response team",
    "vendor-mgmt": "Vendor management + integration engineering",
}

# --------------------------------------------------------------------------- #
# Synthetic identifiers
# --------------------------------------------------------------------------- #

def _ids(region: Optional[str] = None) -> Dict[str, str]:
    return {
        "pol": f"POL-{random.randint(100000, 999999)}",
        "clm": f"CLM-{random.randint(100000, 999999)}",
        "clm2": f"CLM-{random.randint(100000, 999999)}",
        "veh": f"VEH-{random.randint(1000, 9999)}",
        "shop": f"RS-{random.randint(100, 999)}",
        "region": region or random.choice(REGIONS),
        "ms": str(random.randint(8, 95)),
        "n": str(random.randint(120, 950)),
        "score": f"{random.uniform(0.01, 0.18):.2f}",
        "amt": f"{random.randint(400, 9800):,}.{random.randint(0, 99):02d}",
    }


def make_event(level: str, service: str, message: str, event_type: str, ids: Dict[str, str]) -> dict:
    event = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "level": level,
        "service": service,
        "event_type": event_type,
        "region": ids["region"],
        "message": message.format(**ids),
        "request_id": f"req-{random.randint(10000, 99999)}",
    }
    if "{pol}" in message:
        event["policy_id"] = ids["pol"]
    if "{clm}" in message:
        event["claim_id"] = ids["clm"]
    return event


# --------------------------------------------------------------------------- #
# Normal (healthy) traffic
# --------------------------------------------------------------------------- #

NORMAL_TEMPLATES = [
    ("telematics-gateway", "telematics", "Trip batch ingested for {veh}: {n} GPS/accelerometer points"),
    ("telematics-gateway", "telematics", "Driving-behavior features computed for {veh} in {ms}ms"),
    ("crash-detection", "telematics", "Impact score 0.{ms} below crash threshold for {veh}; no event"),
    ("fnol-service", "fnol", "FNOL {clm} created via mobile app for {pol}"),
    ("fnol-service", "fnol", "Photos uploaded for {clm} (6 images) — queued for estimate"),
    ("policy-admin", "policy", "Coverage verified for {pol}: collision + comprehensive active"),
    ("claims-service", "claim", "Claim {clm} assigned to adjuster queue auto-physical-damage"),
    ("claims-service", "claim", "Photo estimate received for {clm}: ${amt}"),
    ("fraud-scoring", "fraud", "Fraud score {score} for {clm} (low) — straight-through processing"),
    ("rating-engine", "rating", "UBI premium recalculated for {pol} in {ms}ms"),
    ("payments-service", "payment", "Payout of ${amt} issued for {clm}"),
    ("payments-service", "payment", "Premium installment collected for {pol}"),
]

WARN_TEMPLATES = [
    ("telematics-gateway", "telematics", "Trip upload retried for {veh} (mobile network timeout)"),
    ("policy-admin", "policy", "Coverage check slow for {pol}: {n}ms"),
    ("claims-service", "claim", "Estimate for {clm} exceeds auto-approval limit; routed to adjuster"),
]


def normal_event() -> dict:
    service, etype, tpl = random.choice(NORMAL_TEMPLATES)
    return make_event("INFO", service, tpl, etype, _ids())


def warn_event() -> dict:
    service, etype, tpl = random.choice(WARN_TEMPLATES)
    return make_event("WARN", service, tpl, etype, _ids())


# --------------------------------------------------------------------------- #
# Incident scenarios (also serve as the runbook knowledge base)
# --------------------------------------------------------------------------- #

SCENARIOS: List[dict] = [
    {
        "key": "telematics_ingestion_outage",
        "title": "Telematics ingestion outage starves crash detection",
        "category": "platform",
        "signatures": ["Consumer lag", "not leader", "crash detection degraded", "Auto-FNOL"],
        "root_cause": "Telematics ingestion partition lost its leader and is lagging; crash detection "
                      "and automatic FNOL receive no data",
        "business_impact": "Crash-triggered FNOL and roadside/emergency dispatch are delayed for "
                           "connected-car customers; UBI pricing data has gaps",
        "owner": "platform-sre",
        "remediation": [
            "Check broker health and partition leadership for telematics-raw topics",
            "Scale telematics-gateway consumers and replay from the last committed offset",
            "Tell claims ops auto-FNOL is degraded; route crash alerts to the manual intake queue",
            "Backfill missed trips so UBI scores are not computed on partial data",
        ],
        "events": [
            ("WARN", "telematics-gateway", "telematics", "Consumer lag 48,210 on partition telematics-raw-3 (threshold 5,000)"),
            ("ERROR", "telematics-gateway", "telematics", "Trip batch write failed for {veh}: partition telematics-raw-3 not leader"),
            ("ERROR", "crash-detection", "telematics", "No telematics data from {region} for 120s — crash detection degraded"),
            ("ERROR", "fnol-service", "fnol", "Auto-FNOL from crash events stalled: 0 crash events received in 5m"),
        ],
    },
    {
        "key": "policy_db_pool_exhausted",
        "title": "Policy database connection pool exhausted",
        "category": "platform",
        "signatures": ["connection pool exhausted", "Coverage verification timeout", "policy lookup unavailable"],
        "root_cause": "policy-admin exhausted its database connection pool, so coverage verification "
                      "times out and downstream FNOL/claim creation fails",
        "business_impact": "New claims cannot be opened or verified; customers reporting accidents see "
                           "errors or delays at first notice of loss",
        "owner": "platform-sre",
        "remediation": [
            "Identify long-running/leaked connections on policy-db-primary and kill them",
            "Temporarily raise the pool size or add read replicas for coverage lookups",
            "Enable the FNOL 'pending-verification' fallback so intake keeps accepting claims",
            "Re-run coverage verification for FNOLs saved as pending once healthy",
        ],
        "events": [
            ("ERROR", "policy-admin", "policy", "DB connection pool exhausted (50/50) on policy-db-primary"),
            ("ERROR", "fnol-service", "fnol", "Coverage verification timeout calling policy-admin (5000ms) for {pol}"),
            ("ERROR", "claims-service", "claim", "Claim creation failed for {pol}: policy lookup unavailable"),
            ("WARN", "fnol-service", "fnol", "FNOL for {pol} saved as pending-verification; customer notified of delay"),
        ],
    },
    {
        "key": "fraud_model_unavailable",
        "title": "Fraud-scoring model endpoint down",
        "category": "platform",
        "signatures": ["fraud-model", "Fraud score unavailable", "Manual review queue", "Scoring request"],
        "root_cause": "The fraud-scoring model endpoint has no healthy instances; claims cannot be "
                      "scored and fall back to manual review",
        "business_impact": "Straight-through processing stops; manual review backlog grows and "
                           "payouts to legitimate claimants are delayed",
        "owner": "ml-platform",
        "remediation": [
            "Restore fraud-model-v7 capacity or roll back to the last healthy model version",
            "Apply the approved rules-based fallback score for low-value claims, per the model-risk policy",
            "Ask claims ops to prioritize the manual review queue by claim age and severity",
        ],
        "events": [
            ("ERROR", "fraud-scoring", "fraud", "Model endpoint fraud-model-v7 returned 503 (no healthy instances)"),
            ("ERROR", "claims-service", "claim", "Fraud score unavailable for {clm}; claim held for manual review"),
            ("WARN", "claims-service", "claim", "Manual review queue depth 1,240 (SLA threshold 300)"),
            ("ERROR", "fraud-scoring", "fraud", "Scoring request for {clm} failed after 3 retries"),
        ],
    },
    {
        "key": "estimate_vendor_certificate",
        "title": "Photo-estimate vendor integration failing (expired certificate)",
        "category": "third-party",
        "signatures": ["certificate expired", "Photo estimate request failed", "vendor unreachable"],
        "root_cause": "The TLS certificate for the photo-estimating vendor endpoint expired, so all "
                      "estimate requests fail",
        "business_impact": "Photo-based estimates are unavailable; claims fall back to field "
                           "appraisers, increasing cycle time and loss adjustment expense",
        "owner": "vendor-mgmt",
        "remediation": [
            "Contact the vendor to renew the certificate (or update the pinned certificate if it was rotated)",
            "Add certificate-expiry monitoring for all partner endpoints",
            "Keep the field-appraiser fallback on and re-submit failed estimates once restored",
        ],
        "events": [
            ("ERROR", "claims-service", "claim", "SSL handshake failed to estimates.partner.example: certificate expired"),
            ("ERROR", "claims-service", "claim", "Photo estimate request failed for {clm}: vendor unreachable"),
            ("WARN", "fnol-service", "fnol", "Photo estimate unavailable for {clm}; scheduling field appraiser"),
        ],
    },
    {
        "key": "payout_batch_rejected",
        "title": "Claim payouts rejected after a payments deploy",
        "category": "platform",
        "signatures": ["payout batch rejected", "Payout failed", "payment-pending"],
        "root_cause": "payments-service v3.14.0 shipped an invalid bank originator configuration; "
                      "the bank gateway rejects every ACH payout batch",
        "business_impact": "Settled claims are not paid; customers wait for funds, raising complaint "
                           "and prompt-payment compliance risk",
        "owner": "payments-eng",
        "remediation": [
            "Roll back payments-service to v3.13.x",
            "Fix and validate the originator configuration in staging before redeploying",
            "Re-submit rejected payout batches and confirm with the bank gateway",
            "Have claims ops notify affected claimants of the delay",
        ],
        "events": [
            ("WARN", "payments-service", "payment", "Deploy payments-service v3.14.0 completed"),
            ("ERROR", "payments-service", "payment", "ACH payout batch rejected by bank gateway: invalid originator ID"),
            ("ERROR", "payments-service", "payment", "Payout failed for {clm} (${amt}): batch rejected"),
            ("ERROR", "claims-service", "claim", "Claim {clm} stuck in payment-pending for more than 24h"),
        ],
    },
    {
        "key": "catastrophe_fnol_surge",
        "title": "Catastrophe surge: hailstorm FNOL volume",
        "category": "business",
        "signatures": ["baseline in", "FNOL queue depth", "Adjuster assignment SLA breached", "callback backlog"],
        "root_cause": "A hailstorm is driving FNOL volume far above baseline in one region; intake and "
                      "adjuster capacity are saturated (a demand event, not a software fault)",
        "business_impact": "Claim acknowledgement and adjuster-contact SLAs are being missed in the "
                           "affected region; customer experience and regulatory timelines are at risk",
        "owner": "cat-response",
        "remediation": [
            "Activate the catastrophe (CAT) response plan for the affected region",
            "Scale FNOL intake and IVR capacity; promote self-service photo FNOL",
            "Enable virtual inspection / photo triage and deploy CAT adjusters",
            "Send proactive status updates to affected policyholders",
        ],
        "events": [
            ("WARN", "fnol-service", "fnol", "FNOL intake rate 14x baseline in {region} (hail event)"),
            ("ERROR", "fnol-service", "fnol", "FNOL queue depth 8,400 exceeds SLA threshold (1,000) in {region}"),
            ("ERROR", "claims-service", "claim", "Adjuster assignment SLA breached for {clm} ({region})"),
            ("ERROR", "fnol-service", "fnol", "IVR callback backlog 2,300 calls in {region}"),
        ],
    },
    {
        "key": "staged_accident_ring",
        "title": "Suspected staged-accident fraud ring",
        "category": "fraud",
        "signatures": ["staged-accident", "Link analysis", "SIU review"],
        "root_cause": "Link analysis found a cluster of claims sharing a repair shop and contact details "
                      "within 48h — a pattern consistent with an organized staged-accident ring",
        "business_impact": "Potential claims leakage across the linked claims; legitimate claimants "
                           "must not be delayed by false positives",
        "owner": "siu",
        "remediation": [
            "Refer the linked claims to SIU for investigation (a human decision — never auto-deny)",
            "Place a temporary payout hold on the linked claims, per SIU policy",
            "Preserve evidence: telematics, photos, repair invoices and call recordings",
            "Watch for new claims linked to the same repair shop",
        ],
        "events": [
            ("WARN", "fraud-scoring", "fraud", "Link analysis: 9 claims in 48h share repair shop {shop} and 3 contact numbers"),
            ("ERROR", "fraud-scoring", "fraud", "Suspected staged-accident ring: {clm}, {clm2} score 0.94 — SIU referral"),
            ("ERROR", "claims-service", "claim", "Payout hold placed on {clm} pending SIU review"),
        ],
    },
]

SCENARIOS_BY_KEY = {s["key"]: s for s in SCENARIOS}


def render_scenario(scenario: dict, region: Optional[str] = None) -> List[dict]:
    """One pass of a scenario's events with fresh synthetic IDs (region is fixed per incident)."""
    region = region or random.choice(REGIONS)
    return [make_event(level, service, tpl, etype, _ids(region))
            for level, service, etype, tpl in scenario["events"]]


def match_scenario(message: str) -> Optional[dict]:
    """Find the runbook whose signature appears in a log message."""
    for s in SCENARIOS:
        if any(sig.lower() in message.lower() for sig in s["signatures"]):
            return s
    return None


def suggest_fix(message: str) -> str:
    """Rule-based one-line suggestion (used by the non-LLM baseline agents)."""
    s = match_scenario(message)
    return s["remediation"][0] if s else "Investigate service logs and recent deployments."


def search_runbooks(query: str, limit: int = 3) -> List[dict]:
    """Keyword search over the runbooks — exposed to the LLM as a tool."""
    words = {w for w in re.findall(r"[a-z0-9-]+", query.lower()) if len(w) > 2}
    scored = []
    for s in SCENARIOS:
        hay = " ".join([s["title"], s["root_cause"], " ".join(s["signatures"]), s["category"],
                        " ".join(e[1] for e in s["events"])]).lower()
        score = sum(1 for w in words if w in hay)
        if score:
            scored.append((score, s))
    scored.sort(key=lambda x: -x[0])
    return [{"key": s["key"], "title": s["title"], "likely_root_cause": s["root_cause"],
             "business_impact": s["business_impact"], "owner": OWNERS[s["owner"]],
             "remediation": s["remediation"]} for _, s in scored[:limit]]


# --------------------------------------------------------------------------- #
# PII redaction — applied before any event text reaches an LLM
# --------------------------------------------------------------------------- #

_REDACTIONS = [
    (re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b"), "[EMAIL]"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[SSN]"),
    (re.compile(r"(?<!\w)(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b"), "[PHONE]"),
    (re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b"), "[VIN]"),
    (re.compile(r"\b(?:\d[ -]?){13,16}\b"), "[CARD]"),
]


def redact(text: str) -> str:
    for pattern, token in _REDACTIONS:
        text = pattern.sub(token, text)
    return text


def redact_event(event: dict) -> dict:
    return {k: (redact(v) if isinstance(v, str) else v) for k, v in event.items()}
