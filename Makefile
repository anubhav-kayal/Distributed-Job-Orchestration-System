.PHONY: up down logs test test-unit lint fmt migrate demo-crash demo-pause verify chaos verify-cron

WORKERS ?= 3
# Short timings so crashes are detected and reclaimed within seconds (used by chaos/demos).
FAST := DJOS_LEASE_TTL_SECONDS=4 DJOS_HEARTBEAT_INTERVAL_SECONDS=1 DJOS_MONITOR_INTERVAL_SECONDS=1

up:  ## start the full stack
	docker compose up -d --build --wait --scale worker=$(WORKERS)

down:
	docker compose down

logs:
	docker compose logs -f worker monitor scheduler

test:  ## all tests (needs Postgres + Redis: make up, or docker compose up -d postgres redis)
	uv run pytest

test-unit:  ## tests that need no services
	uv run pytest -m "not integration"

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .

migrate:
	uv run alembic upgrade head

demo-crash:  ## SIGKILL the worker running a job; watch the monitor reclaim it
	./scripts/demo_worker_crash.sh

demo-pause:  ## freeze a worker mid-job; show its stale write rejected by fencing
	./scripts/demo_worker_pause.sh

verify:  ## 1000 jobs on $(WORKERS) workers; check every job has exactly one effect
	uv run python scripts/verify_exactly_once.py --jobs 1000 --workers $(WORKERS)

chaos:  ## same, while SIGKILLing a random worker every 4s
	$(FAST) docker compose up -d --build --wait --scale worker=$(WORKERS)
	uv run python scripts/verify_exactly_once.py --jobs 1000 --workers $(WORKERS) --kill-every 4 --timeout 600
	docker compose up -d --wait --scale worker=$(WORKERS)

verify-cron:  ## every-minute cron with 2 scheduler replicas: exactly one job per minute
	./scripts/verify_cron.sh 3
