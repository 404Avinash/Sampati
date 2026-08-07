# Data Retention Policy

The Sampati fraud detection engine processes high-velocity, sensitive financial data. Our data retention policy is designed to balance the need for real-time fraud detection with strict compliance to data privacy laws and the Prevention of Money Laundering Act (PMLA).

## Hot-Path Data (Redis)

- **Retention Period:** 60 seconds (configurable via `GRAPH_WINDOW_SECONDS`).
- **Data Stored:** Real-time transaction edges (`amount_paise`, `timestamp`, `txn_id`) and account nodes.
- **Purpose:** Fast, in-memory graph traversal for structural pattern detection. Data is automatically evicted from Redis after the window expires.

## Forensic Graph (Cold Storage)

- **Retention Period:** 5 years.
- **Data Stored:** Fraud alerts, structural explanations, and implicated account IDs.
- **Purpose:** Compliance with PMLA 2002 §12 record-keeping requirements for Suspicious Transaction Reports (STRs).

## Data Privacy & Anonymisation

- **No Raw PII:** The system **DOES NOT** store raw PAN, Aadhaar, or unmasked bank account numbers.
- **Anonymised Identifiers:** All nodes in the behavioral graph use securely anonymised account IDs (e.g., UUIDs or cryptographic hashes of VPAs).
- **Compliance:** This ensures full compliance with the Digital Personal Data Protection Act (DPDPA), minimizing the impact of any potential data exposure.
