import asyncio
import json
import requests
from datetime import datetime, UTC

import httpx

TOKEN_URL = "http://localhost:8080/auth/token"
AIRFLOW_API_BASE = "http://localhost:8080/api/v2"
DAG_ID = "dag_001" 

def get_token():
    response = requests.post(
        TOKEN_URL,
        json={"username": "airflow", "password": "airflow"},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()["access_token"]


async def create_and_wait(client):
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Create a dag run...
    logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
    data = json.dumps({"logical_date": logical_date})
    r = await client.post(f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns", headers=headers, data=data)
    run_id = r.json()["dag_run_id"]
    print("Created dag run:", run_id)

    logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
    config = {"conf": {"logical_date": logical_date}}
    data = json.dumps(config)
    async with client.stream(
        "GET",
        f"{AIRFLOW_API_BASE}/dags/{DAG_ID}/dagRuns/{run_id}/wait?interval=5",
        headers=headers, data=data,
    ) as r:
        async for line in r.aiter_lines():
            pass  # You can do progress report here instead.
    #print("Dag run state:", json.loads(line.strip())["state"])
    print("Dag run state:", json.loads(line.strip()))

async def main():
    async with httpx.AsyncClient() as client:
        await create_and_wait(client)

asyncio.run(main())