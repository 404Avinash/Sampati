"""
decode_sih / api / routers / investigate.py
─────────────────────────────────────────────
Endpoints for forensic analysis and investigator queries.
"""

from fastapi import APIRouter, HTTPException
from core.forensics import forensic_analyzer

router = APIRouter(prefix="/api/investigate", tags=["Investigation"])

@router.get("/cluster")
async def analyze_mule_cluster():
    """
    Triggers Memgraph MAGE algorithms (WCC & Betweenness Centrality)
    to find laundering network hubs and cluster metrics.
    """
    try:
        results = await forensic_analyzer.run_mule_cluster_analysis()
        if "error" in results:
            raise HTTPException(status_code=500, detail=results["error"])
        return {"status": "success", "analysis": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
