.PHONY: build pipeline pipeline-ui prefect-up dbt-run dbt-test dbt-docs check-nulls shell init-env validate

# Make é opcional — o README usa só docker compose (Windows/macOS/Linux).

init-env:
	@cp -n .env.example .env 2>/dev/null || true

build:
	docker compose build

pipeline:
	docker compose run --rm pipeline

up: pipeline

prefect-up:
	docker compose --profile prefect up prefect-server prefect-worker -d --build

pipeline-ui: prefect-up
	docker compose --profile prefect run --rm pipeline-prefect

dbt-run:
	docker compose run --rm pipeline dbt-run

dbt-test:
	docker compose run --rm pipeline dbt-test

dbt-docs:
	docker compose --profile docs up --build dbt-docs

check-nulls:
	docker compose run --rm pipeline shell -c "python scripts/check_nulls.py $${DUCKDB_PATH:-/app/duckdb/cnpj.duckdb}"

shell:
	docker compose run --rm pipeline shell

validate:
	docker compose run --rm --no-deps pipeline shell -c "bash scripts/ci_build.sh"
