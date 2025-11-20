.PHONY: up down migrate

up:
	docker-compose up -d

down:
	docker-compose down

migrate:
	# This is a placeholder since we are using docker-entrypoint-initdb.d for now
	# In a real scenario, we would use a migration tool like golang-migrate
	@echo "Migrations are applied automatically on first run via docker-entrypoint-initdb.d"
