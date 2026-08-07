"""
decode_sih / tests / benchmark_stores.py
──────────────────────────────────────────
Standalone benchmark to prove the latency characteristics of Redis vs In-Memory
under high synthetic load, meeting the <200ms SLA requirements.
"""

import asyncio
import time
import uuid
import random
import numpy as np
from core.models import UPITransaction
from core.storage import InMemoryGraphStore
from core.redis_store import RedisGraphStore

async def generate_txns(num: int) -> list[UPITransaction]:
    txns = []
    for _ in range(num):
        sender = f"acc_{random.randint(1, 1000)}"
        receiver = f"acc_{random.randint(1, 1000)}"
        while receiver == sender:
            receiver = f"acc_{random.randint(1, 1000)}"
        
        txns.append(UPITransaction(
            txn_id=str(uuid.uuid4()),
            sender_id=sender,
            receiver_id=receiver,
            amount_paise=random.randint(100, 100000)
        ))
    return txns

async def benchmark_store(name: str, store, txns: list[UPITransaction], concurrency: int = 100):
    print(f"--- Benchmarking {name} ---")
    latencies = []
    
    semaphore = asyncio.Semaphore(concurrency)
    
    async def _add_txn(txn):
        async with semaphore:
            start = time.perf_counter()
            # Equivalent to the hot path: add edge and get out/in degree
            await store.add_edge(txn.sender_id, txn.receiver_id, txn.txn_id, txn.timestamp, txn.amount_paise)
            # The pattern detectors would query degree, so we add a read operation to simulate hot path accurately
            await store.get_out_edges(txn.sender_id)
            end = time.perf_counter()
            latencies.append((end - start) * 1000)

    # Warmup
    warmup_txns = txns[:1000]
    await asyncio.gather(*[_add_txn(t) for t in warmup_txns])
    latencies.clear() # clear warmup

    # Benchmark
    test_txns = txns[1000:]
    
    start_total = time.perf_counter()
    await asyncio.gather(*[_add_txn(t) for t in test_txns])
    end_total = time.perf_counter()
    
    total_time = end_total - start_total
    tps = len(test_txns) / total_time
    
    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    p999 = np.percentile(latencies, 99.9)
    
    print(f"Total Transactions: {len(test_txns)}")
    print(f"Concurrency level:  {concurrency}")
    print(f"Total Time:         {total_time:.2f} seconds")
    print(f"Throughput (TPS):   {tps:.2f} txn/sec")
    print(f"Latency p50:        {p50:.2f} ms")
    print(f"Latency p95:        {p95:.2f} ms")
    print(f"Latency p99:        {p99:.2f} ms")
    print(f"Latency p99.9:      {p999:.2f} ms")
    print(f"SLA (<200ms) Met?   {'YES' if p99 < 200 else 'NO'}\n")

async def main():
    num_txns = 11000 # 1000 warmup, 10000 benchmark
    txns = await generate_txns(num_txns)
    
    in_memory = InMemoryGraphStore()
    await benchmark_store("InMemoryGraphStore", in_memory, txns)
    
    try:
        redis_store = RedisGraphStore()
        await benchmark_store("RedisGraphStore", redis_store, txns)
    except Exception as e:
        print(f"Could not benchmark RedisGraphStore (is Redis running?): {e}")

if __name__ == "__main__":
    asyncio.run(main())
