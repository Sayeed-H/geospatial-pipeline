package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────

type Config struct {
	KafkaBrokers       string
	KafkaTopicRaw      string
	KafkaTopicEnriched string
	KafkaTopicTrips    string
	ClickHouseHost     string
	ClickHousePort     string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	RedisAddr          string
	RedisPassword      string
}

func loadConfig() *Config {
	return &Config{
		KafkaBrokers:       getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopicRaw:      getEnv("KAFKA_TOPIC_RAW", "raw.gps.events"),
		KafkaTopicEnriched: getEnv("KAFKA_TOPIC_ENRICHED", "enriched.gps.events"),
		KafkaTopicTrips:    getEnv("KAFKA_TOPIC_TRIPS", "trip.lifecycle.events"),
		ClickHouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickHousePort:     getEnv("CLICKHOUSE_PORT", "9000"),
		ClickHouseDB:       getEnv("CLICKHOUSE_DB", "geodata"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "geo_user"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", "geo_pass"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 Processing service starting...")

	cfg := loadConfig()

	// ── ClickHouse ───────────────────────────────────────────────
	chWriter, err := NewClickHouseWriter(cfg)
	if err != nil {
		log.Fatalf("❌ ClickHouse connection failed: %v", err)
	}
	log.Println("✅ ClickHouse connected")

	// ── Redis ────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis connection failed: %v", err)
	}
	log.Println("✅ Redis connected")

	// ── Processors ───────────────────────────────────────────────
	enrichment := NewEnrichmentProcessor(cfg, chWriter)
	tripDetector := NewTripDetector(cfg, rdb, chWriter)
	zoneAggregator := NewZoneAggregator(cfg, chWriter)

	// ── Run all processors concurrently ─────────────────────────
	procCtx, cancel := context.WithCancel(ctx)

	go enrichment.Run(procCtx)
	go tripDetector.Run(procCtx)
	go zoneAggregator.Run(procCtx)

	log.Println("✅ All processors running:")
	log.Println("   🔄 Enrichment  → raw.gps.events → enriched.gps.events + ClickHouse")
	log.Println("   🚗 Trip Detector → enriched.gps.events → trip.lifecycle.events + ClickHouse")
	log.Println("   📊 Zone Aggregator → enriched.gps.events → zone_metrics (5min windows)")

	// ── Graceful shutdown ────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down processing service...")
	cancel()
	rdb.Close()
	log.Println("✅ Shutdown complete")
}