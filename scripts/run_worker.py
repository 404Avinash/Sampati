#!/usr/bin/env python3
"""
decode_sih / scripts / run_worker.py
──────────────────────────────────────
Standalone worker process for the Detection Engine.
Consumes raw transactions from Kafka `txn.incoming`, processes them
through the Behavioral Graph Engine (Redis), and produces verdicts
to `txn.verdicts` and `graph.events`.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime

# Setup paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from confluent_kafka import Consumer, Producer, KafkaError
from config.settings import settings
from core.models import UPITransaction
from pipeline.stream_processor import StreamProcessor
from core.redis_store import RedisGraphStore
from prometheus_client import start_http_server

if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

    resource = Resource.create({"service.name": os.getenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=decode_worker").split("=")[1]})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    
    RedisInstrumentor().instrument()
    ConfluentKafkaInstrumentor().instrument()

# Basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("worker")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

class DetectionWorker:
    def __init__(self):
        self.running = True
        
        # Initialize Kafka Consumer
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': 'detection_group_2',
            'auto.offset.reset': 'latest'
        })
        self.consumer.subscribe(['txn.incoming'])
        
        # Initialize Kafka Producer
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKERS
        })
        
        # Initialize StreamProcessor (without internal emitters/broadcasters)
        store = RedisGraphStore(redis_url="redis://localhost:6379/0")
        self.processor = StreamProcessor(store=store)

        # Monkey-patch internal broadcasting to produce to Kafka instead
        self.processor._broadcast = self._kafka_broadcast
        # Add a dummy broadcaster so `if self._broadcasters:` evaluates to True
        async def dummy(payload): pass
        self.processor.add_broadcaster(dummy)

    async def _kafka_broadcast(self, payload: dict) -> None:
        """Pushes events to Kafka topics instead of internal WebSockets."""
        msg_type = payload.get("type")
        data_str = json.dumps(payload)
        
        if msg_type == "fraud_alert":
            self.producer.produce('txn.verdicts', value=data_str)
        else:
            # geo_tick, txn_tick
            self.producer.produce('graph.events', value=data_str)
            
        self.producer.poll(0) # trigger delivery callbacks

    async def consume_loop(self):
        logger.info(f"Worker connected to Kafka at {KAFKA_BROKERS}")
        logger.info("Listening on 'txn.incoming'...")
        
        # Start Prometheus metrics server
        start_http_server(8001)
        logger.info("Prometheus metrics server started on port 8001")
        
        # Start background processor tasks
        asyncio.create_task(self.processor._eviction_loop())
        asyncio.create_task(self.processor._metrics_broadcast_loop())
        
        while self.running:
            # Fetch up to 100 messages at once, blocking up to 0.1s
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
                
                # Deserialize and process
                try:
                    txn_data = json.loads(msg.value().decode('utf-8'))
                    if getattr(self, '_first_msg', True):
                        logger.info("Received first message from Kafka!")
                        self._first_msg = False
                        
                    txn = UPITransaction(**txn_data)
                    await self.processor._process_transaction(txn)
                except Exception as e:
                    logger.exception("Error processing transaction")

    def stop(self, signum=None, frame=None):
        logger.info("Shutting down worker...")
        self.running = False
        self.consumer.close()
        self.producer.flush()

if __name__ == "__main__":
    worker = DetectionWorker()
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)
    
    try:
        asyncio.run(worker.consume_loop())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Worker crashed")
