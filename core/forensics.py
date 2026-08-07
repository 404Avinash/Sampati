"""
decode_sih / core / forensics.py
──────────────────────────────────
Forensic Mule-Cluster Analysis using Memgraph MAGE algorithms.
"""

import logging
import os
from neo4j import AsyncGraphDatabase

logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

class ForensicAnalyzer:
    def __init__(self):
        self.driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD) if NEO4J_USER else None)
        
    async def run_mule_cluster_analysis(self):
        """
        Runs Weakly Connected Components (WCC) to find disjoint networks,
        and Betweenness Centrality to find the central 'hub' (mule collector).
        Requires Memgraph with the MAGE library installed.
        """
        wcc_query = """
        CALL weakly_connected_components.get() YIELD node, component_id
        RETURN node.id AS account_id, component_id
        """
        
        bc_query = """
        CALL betweenness_centrality.get() YIELD node, betweenness_centrality
        RETURN node.id AS account_id, betweenness_centrality AS centrality_score
        ORDER BY centrality_score DESC
        LIMIT 10
        """
        
        results = {
            "top_hubs": [],
            "components": {}
        }
        
        async with self.driver.session() as session:
            try:
                # 1. Find Central Hubs
                bc_result = await session.run(bc_query)
                async for record in bc_result:
                    results["top_hubs"].append({
                        "account_id": record["account_id"],
                        "centrality_score": record["centrality_score"]
                    })
                
                # 2. Count Components
                wcc_result = await session.run(wcc_query)
                components = {}
                async for record in wcc_result:
                    cid = record["component_id"]
                    components[cid] = components.get(cid, 0) + 1
                
                results["components"] = {
                    "total_clusters": len(components),
                    "largest_cluster_size": max(components.values()) if components else 0
                }
            except Exception as e:
                logger.error(f"Error running MAGE algorithms: {e}")
                results["error"] = str(e)
                
        return results

    async def close(self):
        await self.driver.close()

forensic_analyzer = ForensicAnalyzer()
