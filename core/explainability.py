"""
decode_sih / core / explainability.py
───────────────────────────────────────
Causal Explainability Engine.

Transforms a raw FraudAlert into a rich, structured explanation package
suitable for:
  - Regulators (plain English, Cypher query audit trail)
  - Dashboard display (structured JSON with highlighted accounts)
  - System logging / forensic records (full trace with timestamps)

This module has ZERO side effects — it is purely a formatter/enricher.
All inputs → deterministic outputs. Easy to unit test.
"""

from __future__ import annotations

import textwrap
from datetime import datetime, timezone

from core.models import FraudAlert, FraudPattern, RiskVerdict


# ─── Pattern-specific explanation templates ───────────────────────────────────

_PATTERN_TITLE = {
    FraudPattern.FAN_OUT:        "Fan-Out Money Dispersal",
    FraudPattern.FAN_IN:         "Fan-In Mule Collector",
    FraudPattern.SCATTER_GATHER: "Scatter-Gather (Smurfing)",
    FraudPattern.MULE_CHAIN:     "Mule Chain Forwarding",
    FraudPattern.VELOCITY_ABUSE: "Automated Velocity Abuse",
    FraudPattern.ROUND_TRIP:     "Round-Trip Layering",
}

_PATTERN_DESCRIPTION = {
    FraudPattern.FAN_OUT: (
        "A single account rapidly sends small amounts to many distinct recipients. "
        "This is a classic money laundering technique to disperse illicit funds "
        "before the transaction appears on regulatory radars."
    ),
    FraudPattern.FAN_IN: (
        "Multiple accounts funnel funds into a single collector account. "
        "The collector is typically a mule account used to aggregate proceeds "
        "from a coordinated fraud operation."
    ),
    FraudPattern.SCATTER_GATHER: (
        "Funds are split across multiple intermediary accounts and then "
        "re-converged at a single destination. This multi-hop structure is "
        "designed to break the audit trail and evade per-transaction reporting thresholds."
    ),
    FraudPattern.MULE_CHAIN: (
        "Funds are forwarded in a linear chain A→B→C→D, with each intermediary "
        "immediately forwarding to the next. This creates layering to distance "
        "the final destination from the origin."
    ),
    FraudPattern.VELOCITY_ABUSE: (
        "An account is issuing transactions at a rate far exceeding normal human "
        "behaviour. This is consistent with automated fraud tooling or bot-driven "
        "money movement."
    ),
    FraudPattern.ROUND_TRIP: (
        "Funds leave an account and return to the same origin through intermediaries. "
        "This is used to create the appearance of legitimate transaction volume "
        "and to launder funds through circular flows."
    ),
}

_VERDICT_COLOUR = {
    RiskVerdict.CLEAR: "green",
    RiskVerdict.FLAG:  "amber",
    RiskVerdict.BLOCK: "red",
}

_VERDICT_RECOMMENDED_ACTION = {
    RiskVerdict.CLEAR: "Allow transaction. Continue monitoring.",
    RiskVerdict.FLAG:  "Allow transaction, but queue for manual review within 24 hours. "
                       "Increase monitoring frequency for all implicated accounts.",
    RiskVerdict.BLOCK: "INTERCEPT transaction before settlement. Freeze implicated accounts "
                       "pending investigation. File STR (Suspicious Transaction Report) "
                       "with FIU-IND within 7 days as per PMLA, 2002.",
}


# ─── Rich Explanation Builder ─────────────────────────────────────────────────


def build_rich_explanation(alert: FraudAlert) -> dict:
    """
    Takes a FraudAlert and returns a structured dict containing:
    - title: short human-readable label
    - pattern_description: what this pattern means in plain English
    - causal_summary: the specific details of this alert instance
    - recommended_action: what the bank/regulator should do
    - audit_trail: machine-readable record for forensic logging
    - dashboard_payload: minimal JSON for UI rendering

    This is the authoritative explainability output. Use it everywhere.
    """
    title   = _PATTERN_TITLE.get(alert.pattern, alert.pattern)
    pattern = _PATTERN_DESCRIPTION.get(alert.pattern, "")
    colour  = _VERDICT_COLOUR[alert.verdict]
    action  = _VERDICT_RECOMMENDED_ACTION[alert.verdict]

    # ── Causal summary (instance-specific details) ────────────────────────────
    causal_summary = textwrap.dedent(f"""
        Alert ID          : {alert.alert_id}
        Triggered By      : Transaction {alert.triggered_by_txn}
        Pattern Detected  : {title}
        Risk Score        : {alert.risk_score:.4f} / 1.0000
        Verdict           : {alert.verdict} ({colour.upper()})
        Detection Latency : {alert.detection_latency_ms:.2f} ms
                            {'✅ Within SLA' if alert.within_sla else '⚠️  SLA BREACH'}
        Accounts Involved : {len(alert.implicated_accounts)}
        Transactions      : {len(alert.implicated_transactions)}
        Timestamp         : {alert.timestamp.isoformat()}

        Plain English Explanation:
        {textwrap.indent(alert.explanation_text, '  ')}

        Recommended Action:
        {textwrap.indent(action, '  ')}
    """).strip()

    # ── Audit trail (machine-readable, append to forensic log) ───────────────
    audit_trail = {
        "alert_id":                 alert.alert_id,
        "triggered_by_txn":        alert.triggered_by_txn,
        "pattern":                  alert.pattern,
        "verdict":                  alert.verdict,
        "risk_score":               alert.risk_score,
        "detection_latency_ms":     alert.detection_latency_ms,
        "within_sla":               alert.within_sla,
        "implicated_accounts":      alert.implicated_accounts,
        "implicated_transactions":  alert.implicated_transactions,
        "timestamp_utc":            alert.timestamp.isoformat(),
        "explanation_text":         alert.explanation_text,
        "explanation_cypher":       alert.explanation_cypher,
        "regulatory_obligation":    _regulatory_obligation(alert.verdict),
    }

    # ── Dashboard payload (minimal, for real-time UI rendering) ──────────────
    dashboard_payload = {
        "alert_id":          alert.alert_id,
        "pattern":           alert.pattern,
        "verdict":           alert.verdict,
        "colour":            colour,
        "score":             alert.risk_score,
        "latency_ms":        alert.detection_latency_ms,
        "within_sla":        alert.within_sla,
        "accounts":          alert.implicated_accounts[:10],
        "title":             title,
        "summary":           alert.explanation_text[:200] + "…"
                             if len(alert.explanation_text) > 200
                             else alert.explanation_text,
        "action":            action,
        "timestamp":         alert.timestamp.isoformat(),
    }

    return {
        "title":               title,
        "pattern_description": pattern,
        "causal_summary":      causal_summary,
        "recommended_action":  action,
        "audit_trail":         audit_trail,
        "dashboard_payload":   dashboard_payload,
    }


def format_audit_log_line(alert: FraudAlert) -> str:
    """
    Single-line structured log format for forensic log files.
    Designed to be parseable by SIEM tools (Splunk, Elastic).
    """
    sla = "OK" if alert.within_sla else "BREACH"
    return (
        f"[{alert.timestamp.isoformat()}] "
        f"FRAUD_ALERT "
        f"alert_id={alert.alert_id[:8]} "
        f"txn={alert.triggered_by_txn[:8]} "
        f"pattern={alert.pattern} "
        f"verdict={alert.verdict} "
        f"score={alert.risk_score:.4f} "
        f"latency_ms={alert.detection_latency_ms:.1f} "
        f"sla={sla} "
        f"accounts={len(alert.implicated_accounts)}"
    )


# ─── Private helpers ──────────────────────────────────────────────────────────

def _regulatory_obligation(verdict: RiskVerdict) -> str:
    if verdict == RiskVerdict.BLOCK:
        return (
            "Mandatory: File Suspicious Transaction Report (STR) with FIU-IND "
            "under Section 12 of PMLA, 2002, within 7 working days. "
            "Freeze account per Section 51A of UAPA, 1967 if terror financing suspected."
        )
    if verdict == RiskVerdict.FLAG:
        return (
            "Recommended: Flag transaction for Enhanced Due Diligence (EDD). "
            "Retain transaction records per RBI Master Direction on KYC, 2016."
        )
    return "No immediate regulatory obligation. Retain standard transaction records."
