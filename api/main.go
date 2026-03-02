package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/websocket/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	Port               string
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
		Port:               getEnv("API_PORT", "8080"),
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

func main() {
	cfg := loadConfig()

	// ── DB connections ───────────────────────────────────────────
	chDB, err := NewClickHouseDB(cfg)
	if err != nil {
		log.Fatalf("❌ ClickHouse failed: %v", err)
	}
	log.Println("✅ ClickHouse connected")

	rdb := NewRedisClient(cfg)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("❌ Redis failed: %v", err)
	}
	log.Println("✅ Redis connected")

	// ── Handlers ─────────────────────────────────────────────────
	h := NewHandlers(chDB, rdb)
	hub := NewWebSocketHub(rdb)
	go hub.Run()

	// ── Fiber app ────────────────────────────────────────────────
	app := fiber.New(fiber.Config{
		AppName:      "Geospatial Pipeline API",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	app.Use(recover.New())
	app.Use(logger.New(logger.Config{
		Format: "[${time}] ${status} ${method} ${path} ${latency}\n",
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept",
	}))

	// ── Routes ───────────────────────────────────────────────────
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "time": time.Now().UTC()})
	})

	v1 := app.Group("/api")
	v1.Get("/drivers/live", h.GetLiveDrivers)
	v1.Get("/zones/heatmap", h.GetHeatmap)
	v1.Get("/zones/:h3/stats", h.GetZoneStats)
	v1.Get("/trips", h.GetTrips)
	v1.Get("/trips/:id/route", h.GetTripRoute)
	v1.Get("/stats/summary", h.GetSummary)

	// WebSocket
	app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	app.Get("/ws/live", websocket.New(hub.HandleWS))

	// Prometheus on separate port (avoids Fiber/net/http conflict)
	go func() {
		log.Println("📡 Metrics server on :2113")
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2113", nil)
	}()

	// ── Start ────────────────────────────────────────────────────
	go func() {
		log.Printf("🚀 API server on :%s", cfg.Port)
		log.Printf("   REST:      http://localhost:%s/api", cfg.Port)
		log.Printf("   WebSocket: ws://localhost:%s/ws/live", cfg.Port)
		log.Printf("   Health:    http://localhost:%s/health", cfg.Port)
		log.Printf("   Metrics:   http://localhost:2113/metrics", )
		if err := app.Listen(":" + cfg.Port); err != nil {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down API...")
	app.Shutdown()
}