#!/usr/bin/env python3
"""
scripts/run_server.py
──────────────────────
Convenience launcher for the War Room API server.
Run from project root: python scripts/run_server.py

Reads host/port from config so you only need to set .env values.
"""
import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uvicorn
from config.settings import settings

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 UPI Fraud Prevention — War Room Server")
    print(f"   http://{settings.app.host}:{settings.app.port}")
    print(f"   API Docs: http://localhost:{settings.app.port}/docs")
    print("=" * 60)

    uvicorn.run(
        "api.main:app",
        host=settings.app.host,
        port=settings.app.port,
        reload=settings.app.env.value == "development",
        log_level=settings.app.log_level.value.lower(),
        access_log=True,
    )
