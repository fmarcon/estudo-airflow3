FROM apache/airflow:3.1.0

USER root

# Instalar dependências adicionais do sistema, se necessário
RUN apt-get update && apt-get install -y \
    vim curl less \
    && apt-get clean

USER airflow

# Instalar pacotes Python adicionais, se existirem
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt || true

# Define o diretório de trabalho padrão
WORKDIR /opt/airflow
