FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    DBT_PROFILES_DIR=/app/dbt

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

RUN mkdir -p /app/data/raw /app/duckdb /app/dbt/target /app/dbt/logs /app/dbt/dbt_packages \
    && sed -i 's/\r$//' /app/scripts/entrypoint.sh /app/scripts/ci_build.sh /app/scripts/validate_entrega.sh \
    && chmod +x /app/scripts/entrypoint.sh /app/scripts/ci_build.sh /app/scripts/validate_entrega.sh \
    && chmod -R a+rwX /app/data /app/duckdb /app/dbt/target /app/dbt/logs /app/dbt/dbt_packages

# bash (não ./entrypoint.sh): o volume ./scripts no Compose cobre o chmod da
# imagem, e checkout no Windows costuma vir sem bit +x — quebra o CI no Linux.
ENTRYPOINT ["bash", "/app/scripts/entrypoint.sh"]
CMD ["pipeline"]
