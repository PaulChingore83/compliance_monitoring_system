FROM apache/airflow:2.11.0-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libpq-dev \
        curl \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directories and set permissions
RUN mkdir -p /opt/airflow/logs/scheduler /opt/airflow/logs/webserver \
    /opt/airflow/data/raw /opt/airflow/data/processed /opt/airflow/data/logs \
    && chown -R airflow:root /opt/airflow/logs \
    && chown -R airflow:root /opt/airflow/data \
    && chmod -R 775 /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/data

USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow