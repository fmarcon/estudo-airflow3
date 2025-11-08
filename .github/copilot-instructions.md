## Instruções rápidas para agentes AI — projeto estudo-airflow3

Objetivo: este repositório contém uma stack Airflow (3.x) em Docker Compose com DAGs de exemplo em `dags/`, scripts utilitários em `scripts/` e helpers de chamada de API na raiz.

Pontos essenciais (big picture)
- Arquitetura: serviços orquestrados via `docker-compose.yaml` (Postgres, Redis, Webserver, Scheduler, Worker). Rede Docker nomeada: `rede_airflow`.
- Fluxo principal: DAGs (em `dags/`) são carregadas pelo Airflow; alguns DAGs disparam outros via `TriggerDagRunOperator` (ex.: `dags/008_*`).
- Persistência: Postgres via volume `postgres-db-volume-airflow-3.1:` — evitar SQLite local (há histórico de `airflow.db` criado se init rodar antes do Postgres).

Padrões e convenções específicas do projeto
- Airflow 3: prefira `from airflow.task.trigger_rule import TriggerRule` (import antigo em `airflow.utils` é deprecated).
- Variáveis de ambiente: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (Airflow 3) é usada; `AIRFLOW__CORE__SQL_ALCHEMY_CONN` pode aparecer por compatibilidade.
- UID/GID host: compose usa `AIRFLOW_UID`/`AIRFLOW_GID` (veja `.env` gerado). Bind mounts `./dags`, `./logs`, `./plugins` são montados como o UID/GID configurado.
- TaskFlow & patterns: DAGs usam TaskFlow (`@task`, `.expand()` para mapping). Evitar passar grandes coleções via XCom — usar chunking/arquivos/S3. Exemplo: `dags/003.py` usa `inserir_pessoa_ldap.expand(pessoa=pessoas)`.

Operações comuns / comandos úteis
- Subir stack (na raiz do repo):
  - `export AIRFLOW_UID=$(id -u); export AIRFLOW_GID=$(id -g); docker compose up -d`
- Parar/remover containers:
  - `docker compose down`
- Forçar remoção do volume Postgres (cuidado: apaga dados):
  - `docker volume rm estudo-airflow3_postgres-db-volume-airflow-3.1`
- Ver logs do init / diagnosticar SQLite criado:
  - `docker compose logs --tail=200 airflow-init`
- Inspecionar env de um container:
  - `docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' airflow_init`
- Testar resolução DNS entre containers (abrir shell em um container saudável):
  - `docker compose run --rm airflow-webserver getent hosts postgres || ping -c1 postgres`

Padrões de código detectáveis (exemplos)
- Trigger de DAGs: `TriggerDagRunOperator(..., wait_for_completion=True, reset_dag_run=True)` em `dags/008_pai.py`.
- Pools: código usa `@task(pool="ldap_pool")` em `dags/003.py` — o pool deve existir no Airflow UI ou ser criado via CLI.
- Import modules: veja variação `from airflow.sdk import dag, task` vs `from airflow.decorators import dag, task` — manter padrão consistente por DAGs novos.

Integrações externas e pontos de atenção
- API de autenticação: `scripts/` e `executar_dag.sh` obtêm `access_token` via `POST /auth/token` e usam `Authorization: Bearer <token>`.
- XComs: cuidado com payloads grandes (1M registros) — preferir chunking em arquivo ou XCom-backend (S3/GCS) e expand por chunk.
- Dependências entre serviços: compose já define `healthcheck` no Postgres/Redis; porém `airflow-init` precisa aguardar `pg_isready` (ex.: wrapper `until pg_isready ...` já adicionado em compose em algumas revisões).

Onde olhar primeiro (arquivos-chave)
- `docker-compose.yaml` — definição de serviços, volumes, env vars.
- `dags/` — DAGs exemplares: `003.py` (TaskFlow + pool + expand), `007.py` (branching), `008_*` (TriggerDagRunOperator).
- `scripts/api_executar_dag_wait1.py` — utilitário refatorado para disparar DAGs via API e aguardar `/wait`.
- `executar_dag.sh`, `teste_api1.py` — exemplos rápidos de chamada da API REST.

Regras rápidas ao editar/gerar código
- Ao criar/alterar DAGs, mantenha compatibilidade com Airflow 3 imports (`airflow.task.trigger_rule`, `AIRFLOW__DATABASE__*`).
- Não infira que bind mounts têm permissões corretas; inclua instrução para ajustar `AIRFLOW_UID/GID` ou rodar `sudo chown -R $(id -u):$(id -g) ./dags ./logs ./plugins` se necessário.
- Evite salvar grandes estruturas em XComs; prefira chunking/arquivos/obj-storage e passe referências.

Se algo não estiver claro
- Peça para executar comandos de diagnóstico (logs, inspect, compose ps) e retornar as saídas. Ex.: `docker compose ps --all` e `docker compose logs airflow-init`.

Fim — peça feedback ao usuário sobre qualquer seção que esteja incompleta ou que deva ser expandida.
