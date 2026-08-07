#!/bin/bash
pkill -f "run_server.py"
pkill -f "uvicorn"
sleep 1
.venv/bin/python scripts/run_server.py > server.log 2>&1 &
echo $! > server.pid

