import json
from datetime import UTC, datetime

import requests

TOKEN_URL = "http://localhost:8080/auth/token"
BASE_URL = "http://localhost:8080/api/v2"
AUTH = ("airflow", "airflow")  # ajuste conforme sua autenticação
DAG_ID = "dag_001"

# 🕒 Cria um dag_run_id único baseado no horário atual
dag_run_id = f"manual__{datetime.now(UTC).isoformat().replace('+00:00', 'Z')}"

def get_token():
    response = requests.post(
        TOKEN_URL,
        json={"username": "airflow", "password": "airflow"},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    retorno  = response.json()["access_token"]
    #print("Token recebido:", retorno)
    return retorno

# ==========================
# 1️⃣ Dispara a DAG via API
# ==========================
token = get_token()
headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
data = json.dumps({"logical_date": logical_date})
trigger_resp = requests.post(
    f"{BASE_URL}/dags/{DAG_ID}/dagRuns",
    headers=headers,
    data=data
)

dag_run_id = trigger_resp.json()["dag_run_id"]

if trigger_resp.status_code not in (200, 201):
    print("❌ Erro ao disparar DAG:", trigger_resp.text)
    exit(1)

print(f"🚀 DAG '{DAG_ID}' disparada com dag_run_id='{dag_run_id}'")
print("Aguardando execução em tempo real...\n")

# ==============================
# 2️⃣ Espera o término da DAG Run
# ==============================
params = {"interval": 5}  # checa o status a cada 5 segundos

with requests.get(
    f"{BASE_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/wait",
    headers=headers,
    params=params,
    #data=data,
    stream=True,
) as resp:
    resp.raise_for_status()

    final_line = None
    for line in resp.iter_lines():
        if line:
            decoded = line.decode("utf-8")
            print(decoded)
            final_line = decoded  # guarda a última linha (estado final)

# ==========================
# 3️⃣ Mostra o resultado final
# ==========================
if final_line:
    result = json.loads(final_line)
    state = result.get("state")
    print(f"\n✅ DAG finalizada com estado: {state}")
else:
    print("\n⚠️ Nenhuma resposta final recebida.")
