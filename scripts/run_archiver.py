#!/usr/bin/env python3
"""
decode_sih / scripts / run_archiver.py
────────────────────────────────────────
Cold Storage / Forensic Graph Archiver.
Consumes raw transactions from Kafka `txn.incoming` and verdicts from `txn.verdicts`,
and persists them to Memgraph/Neo4j for multi-hop forensic investigations.
"""

import asyncio
import json
import logging
import os
import signal
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from confluent_kafka import Consumer, KafkaError
from neo4j import AsyncGraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("archiver")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

class GraphArchiver:
    def __init__(self):
        self.running = True
        
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': 'archiver_group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['txn.incoming', 'txn.verdicts'])
        
        self.driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD) if NEO4J_USER else None)

    async def init_db(self):
        """Create constraints/indexes for performance."""
        async with self.driver.session() as session:
            try:
                await session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE")
                await session.run("CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.txn_id)")
            except Exception as e:
                logger.warning(f"Could not create constraints (maybe using older Neo4j version): {e}")

    async def _process_transaction(self, txn_data: dict):
        """Upsert nodes and create transaction edge."""
        query = """
        MERGE (s:Account {id: $sender_id})
        MERGE (r:Account {id: $receiver_id})
        CREATE (s)-[t:SENT {
            txn_id: $txn_id,
            amount_paise: $amount_paise,
            timestamp: $timestamp,
            is_synthetic: $is_synthetic
        }]->(r)
        """
        # Provide defaults for missing fields in case of different schemas
        txn_data.setdefault("is_synthetic", False)
        
        async with self.driver.session() as session:
            await session.run(query, **txn_data)

    async def _process_verdict(self, alert_data: dict):
        """Attach a fraud alert to the implicated accounts."""
        alert = alert_data.get("alert", {})
        if not alert:
            return
            
        query = """
        MERGE (a:FraudAlert {alert_id: $alert_id})
        SET a.pattern = $pattern,
            a.verdict = $verdict,
            a.risk_score = $risk_score,
            a.explanation_text = $explanation_text,
            a.timestamp = $timestamp
        
        WITH a
        UNWIND $implicated_accounts AS acc_id
        MERGE (acc:Account {id: acc_id})
        MERGE (acc)-[:IMPLICATED_IN]->(a)
        """
        async with self.driver.session() as session:
            await session.run(query, **alert)

    async def consume_loop(self):
        await self.init_db()
        logger.info(f"Archiver connected to Kafka at {KAFKA_BROKERS}")
        logger.info(f"Archiver connected to Memgraph/Neo4j at {NEO4J_URI}")
        logger.info("Listening on 'txn.incoming' and 'txn.verdicts'...")
        
        while self.running:
            msgs = await asyncio.to_thread(self.consumer.consume, num_messages=100, timeout=0.1)
            
            for msg in msgs:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                topic = msg.topic()
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    if topic == 'txn.incoming':
                        await self._process_transaction(data)
                    elif topic == 'txn.verdicts':
                        await self._process_verdict(data)
                except Exception as e:
                    logger.exception(f"Error processing message from {topic}")

    async def stop(self):
        logger.info("Shutting down archiver...")
        self.running = False
        self.consumer.close()
        await self.driver.close()

if __name__ == "__main__":
    archiver = GraphArchiver()
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(archiver.stop()))
    
    try:
        loop.run_until_complete(archiver.consume_loop())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Archiver crashed")
