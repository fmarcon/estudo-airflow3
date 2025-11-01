"""Auxiliar simples para disparar uma DAG do Airflow via API REST e aguardar sua conclusão.

Organizado em funções com ponto de entrada `main()` para facilitar reutilização e testes.

Exemplo de uso:
    python scripts/api_executar_dag_wait1.py --dag-id dag_001 --interval 5
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Optional

import requests


def get_token(session: requests.Session, token_url: str, username: str, password: str) -> str:
    """Obtem um access token do endpoint de autenticação.

    Levanta exceção em caso de falha (requests.HTTPError).
    """
    resp = session.post(
        token_url,
        json={"username": username, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("access_token", "")


def trigger_dag(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    token: str,
    logical_date: Optional[str] = None,
) -> str:
    """Dispara a DAG e retorna o dag_run_id criado.

    Levanta requests.HTTPError em caso de falha no POST.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if logical_date is None:
        logical_date = datetime.now(UTC).replace(microsecond=0).isoformat()
    payload = {"logical_date": logical_date}
    resp = session.post(
        f"{base_url}/dags/{dag_id}/dagRuns",
        headers=headers,
        data=json.dumps(payload),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("dag_run_id")


def wait_for_dag_run(
    session: requests.Session,
    base_url: str,
    dag_id: str,
    dag_run_id: str,
    token: str,
    interval: int = 5,
) -> Optional[dict]:
    """Abre um stream para o endpoint /wait e retorna o JSON final quando disponível.

    Retorna None se nenhum resultado final for recebido.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {"interval": interval}
    url = f"{base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/wait"
    with session.get(url, headers=headers, params=params, stream=True, timeout=(5, None)) as resp:
        resp.raise_for_status()
        final_line = None
        for raw in resp.iter_lines(decode_unicode=True):
            if raw:
                print(raw)
                final_line = raw
        if final_line:
            return json.loads(final_line)
    return None


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Trigger an Airflow DAG and wait for completion")
    p.add_argument("--token-url", default="http://localhost:8080/auth/token")
    p.add_argument("--base-url", default="http://localhost:8080/api/v2")
    p.add_argument("--username", default="airflow")
    p.add_argument("--password", default="airflow")
    p.add_argument("--dag-id", default="dag_001")
    p.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Polling interval for the /wait endpoint",
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    session = requests.Session()
    try:
        token = get_token(session, args.token_url, args.username, args.password)
    except requests.RequestException as exc:
        print("❌ Falha ao obter token:", exc)
        return 2

    try:
        dag_run_id = trigger_dag(session, args.base_url, args.dag_id, token)
    except requests.RequestException as exc:
        print("❌ Erro ao disparar DAG:", exc)
        return 3

    if not dag_run_id:
        print("❌ Nenhum dag_run_id retornado pelo endpoint")
        return 4

    print(f"🚀 DAG '{args.dag_id}' disparada com dag_run_id='{dag_run_id}'")
    print("Aguardando execução em tempo real...\n")

    try:
        result = wait_for_dag_run(
            session,
            args.base_url,
            args.dag_id,
            dag_run_id,
            token,
            interval=args.interval,
        )
    except requests.RequestException as exc:
        print("❌ Erro ao aguardar execução da DAG:", exc)
        return 5

    if result is None:
        print("\n⚠️ Nenhuma resposta final recebida.")
        return 6

    state = result.get("state")
    print(f"\n✅ DAG finalizada com estado: {state}")
    return 0 if state == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
