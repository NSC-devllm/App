# Migrations

Run locally
1. Ensure Docker Compose is up (`docker compose up -d`).
1. Export `DATABASE_URL` if not using the default.
1. Apply migrations: `alembic upgrade head`.
1. Seed sample data: `python scripts/seed_local_db.py`.
