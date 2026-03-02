.PHONY: infra-up infra-down infra-ps topics simulator bridge help

# ─────────────────────────────────────────────────────────────────
# Infrastructure
# ─────────────────────────────────────────────────────────────────

infra-up:
	@echo "🚀 Starting all infrastructure services..."
	docker compose -f infra/docker-compose.yml up -d
	@echo "⏳ Waiting 20s for services to initialize..."
	sleep 20
	@echo "📌 Creating Kafka topics..."
	bash scripts/create-kafka-topics.sh
	@echo ""
	@echo "✅ Infrastructure ready!"
	@make urls

infra-down:
	@echo "🛑 Stopping all services..."
	docker compose -f infra/docker-compose.yml down

infra-ps:
	docker compose -f infra/docker-compose.yml ps

infra-logs:
	docker compose -f infra/docker-compose.yml logs -f $(service)

topics:
	bash scripts/create-kafka-topics.sh

# ─────────────────────────────────────────────────────────────────
# Services
# ─────────────────────────────────────────────────────────────────

simulator:
	@echo "🚗 Starting GPS Simulator (100 Dhaka drivers)..."
	cd simulator && go run main.go

bridge:
	@echo "🌉 Starting MQTT→Kafka Bridge..."
	cd ingestion && go run main.go

# ─────────────────────────────────────────────────────────────────
# Dev helpers
# ─────────────────────────────────────────────────────────────────

kafka-console:
	docker exec -it kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic raw.gps.events \
		--from-beginning

clickhouse-cli:
	docker exec -it clickhouse clickhouse-client \
		--user geo_user \
		--password geo_pass \
		--database geodata

redis-cli:
	docker exec -it redis redis-cli

urls:
	@echo ""
	@echo "🌐 Service URLs:"
	@echo "   Kafka UI:       http://localhost:8090"
	@echo "   Flink UI:       http://localhost:8081"
	@echo "   ClickHouse:     http://localhost:8123/play"
	@echo "   MinIO:          http://localhost:9011  (minioadmin/minioadmin123)"
	@echo "   MLflow:         http://localhost:5000"
	@echo "   Grafana:        http://localhost:3001  (admin/admin123)"
	@echo "   Prometheus:     http://localhost:9090"
	@echo "   Bridge Metrics: http://localhost:2112/metrics"
	@echo ""

help:
	@echo ""
	@echo "Geospatial Pipeline - Available Commands"
	@echo "──────────────────────────────────────────"
	@echo "  make infra-up      Start all Docker services + create Kafka topics"
	@echo "  make infra-down    Stop all Docker services"
	@echo "  make infra-ps      Show service status"
	@echo "  make infra-logs    Tail logs (e.g. make infra-logs service=kafka)"
	@echo "  make topics        Create/recreate Kafka topics"
	@echo "  make simulator     Run GPS simulator (100 Dhaka drivers)"
	@echo "  make bridge        Run MQTT→Kafka bridge"
	@echo "  make kafka-console Watch raw GPS events in terminal"
	@echo "  make clickhouse-cli Open ClickHouse shell"
	@echo "  make redis-cli     Open Redis CLI"
	@echo "  make urls          Show all service URLs"
	@echo ""