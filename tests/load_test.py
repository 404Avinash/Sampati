"""
decode_sih / tests / load_test.py
───────────────────────────────────
Locust load test for the API ingestion endpoint.
Run with: locust -f tests/load_test.py
"""

import json
import random
import uuid
from locust import HttpUser, task, between

class FraudAPIUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def submit_transaction(self):
        txn_id = str(uuid.uuid4())
        sender_id = f"user_{random.randint(1, 1000)}"
        receiver_id = f"user_{random.randint(1, 1000)}"
        while receiver_id == sender_id:
            receiver_id = f"user_{random.randint(1, 1000)}"

        amount_paise = random.randint(1000, 100000)

        payload = {
            "txn_id": txn_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "amount_paise": amount_paise
        }
        
        headers = {"Content-Type": "application/json"}
        
        with self.client.post("/api/ingest", data=json.dumps(payload), headers=headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 429: # Rate limited
                response.success() 
            else:
                response.failure(f"Failed with status {response.status_code}")
