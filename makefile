pipeline:
	docker-compose run --rm pipeline

project:
	docker-compose up -d --build prefect-server
	START_YEAR=$(START_YEAR) END_YEAR=$(END_YEAR) docker-compose up --build pipeline
	docker compose up -d --build --no-deps evidence

dashboard:
	open http://localhost:3000

prefect-server:
	open http://localhost:4200

down:
	docker-compose down

clean:
	docker-compose down -v