pipeline:
	docker-compose run --rm pipeline

dashboard:
	docker-compose up evidence

all:
	docker-compose up --build --abort-on-container-exit --exit-code-from pipeline prefect-server pipeline
	docker compose up -d --build --no-deps evidence

ui:
	open http://localhost:4200

down:
	docker-compose down

clean:
	docker-compose down -v