import asyncio
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import time

router = APIRouter(prefix="/api/ai", tags=["ai"])

class SARRequest(BaseModel):
    pattern: str
    risk_score: float
    implicated_accounts: list[str]
    cypher_query: str

async def generate_sar_stream(req: SARRequest):
    """
    Simulates an LLM generating a Suspicious Activity Report (SAR) 
    compliant with FIU-IND formatting requirements.
    """
    # Create a highly detailed mock response based on the graph data
    lines = [
        "**[FIU-IND] SUSPICIOUS ACTIVITY REPORT (SAR) GENERATED**\n",
        f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')} (IST)\n",
        f"**Risk Severity:** {'CRITICAL (BLOCK)' if req.risk_score > 0.85 else 'HIGH (FLAG)'} | Score: {req.risk_score:.4f}\n\n",
        "### 1. Executive Summary\n",
        f"AI Graph Engine has detected a structural **{req.pattern}** topology indicative of coordinated money laundering or organized fraud. The network executed transactions rapidly, bypassing standard rule-based reporting thresholds (₹50,000). The funds were distributed among {len(req.implicated_accounts)} distinct virtual payment addresses (VPAs).\n\n",
        "### 2. Behavioral Topology Analysis\n",
        f"The network exhibits classic '{req.pattern.replace('_', ' ')}' traits. Rather than a direct peer-to-peer transfer, funds were structurally routed to evade detection. The primary node(s) rapidly exchanged liquidity with secondary mule accounts.\n\n",
        "**Implicated Network Nodes:**\n",
    ]
    
    for acc in req.implicated_accounts[:5]:
        lines.append(f"- `VPA: {acc}` (High Degree Centrality)\n")
    if len(req.implicated_accounts) > 5:
        lines.append(f"- ... and {len(req.implicated_accounts) - 5} additional accounts.\n")
        
    lines.extend([
        "\n### 3. Cryptographic Audit Trail (Cypher)\n",
        "The following structural query was used to isolate the sub-graph in real-time:\n",
        "```cypher\n",
        f"{req.cypher_query}\n",
        "```\n\n",
        "### 4. Recommended Action\n",
        "**IMMEDIATE FREEZE** recommended for the implicated accounts. A standard STR (Suspicious Transaction Report) template has been forwarded to the nodal officer via n8n integration.\n",
        "--- *End of AI Analysis* ---"
    ])

    # Simulate token-by-token streaming
    for line in lines:
        words = line.split(" ")
        for i, word in enumerate(words):
            yield word + (" " if i < len(words) - 1 else "")
            await asyncio.sleep(0.04)  # 40ms per token for realistic typing effect

@router.post("/generate-sar")
async def trigger_sar(request: SARRequest):
    return StreamingResponse(generate_sar_stream(request), media_type="text/event-stream")
