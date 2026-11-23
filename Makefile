# Convenience targets. Everything here is a thin wrapper around docker compose
# and the two Python entry points.

BOOTSTRAP ?= localhost:9092
ORDERS    ?= orders
DLQ       ?= orders.DLQ
PARTITIONS?= 3

.PHONY: help up down topics produce produce-bad consume consume-faulty dlq test clean reset

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## Start Kafka, Schema Registry and Kafka UI
	docker compose up -d
	@echo "waiting for schema registry..."
	@until curl -sf http://localhost:8081/subjects >/dev/null; do sleep 2; done
	@$(MAKE) topics
	@echo "cluster ready: kafka=$(BOOTSTRAP) registry=http://localhost:8081 ui=http://localhost:8080"

down: ## Stop the cluster
	docker compose down

topics: ## Create the orders and DLQ topics
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic $(ORDERS) --partitions $(PARTITIONS) --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic $(DLQ) --partitions 1 --replication-factor 1
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

produce: ## Send 20 valid orders
	python src/producer.py --count 20 --interval 0.4

produce-bad: ## Send 25 orders, 25% invalid, plus 2 poison pills
	python src/producer.py --count 25 --interval 0.4 --bad-rate 0.25 --poison 2

consume: ## Consume with aggregation (no injected faults)
	python src/consumer.py

consume-faulty: ## Consume with a 30% transient failure rate to exercise retries
	python src/consumer.py --fault-rate 0.3 --stats-file stats.json

dlq: ## Print everything sitting in the dead letter queue
	python src/dlq_consumer.py

test: ## Run the broker-free unit tests
	pytest -q

reset: ## Delete topics and recreate them (fresh demo state)
	-docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $(ORDERS)
	-docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $(DLQ)
	@sleep 3
	@$(MAKE) topics

clean: ## Stop the cluster and remove volumes
	docker compose down -v
