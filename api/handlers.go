package main

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

type Handlers struct {
	ch  *ClickHouseDB
	rdb *redis.Client
}

func NewHandlers(ch *ClickHouseDB, rdb *redis.Client) *Handlers {
	return &Handlers{ch: ch, rdb: rdb}
}

// ─────────────────────────────────────────────────────────────────
// GET /api/drivers/live
// Returns all live driver positions from Redis
// ─────────────────────────────────────────────────────────────────

type LiveDriver struct {
	DriverID string  `json:"driver_id"`
	Lat      float64 `json:"lat"`
	Lng      float64 `json:"lng"`
}

func (h *Handlers) GetLiveDrivers(c *fiber.Ctx) error {
	ctx := context.Background()

	result, err := h.rdb.HGetAll(ctx, "drivers:live").Result()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	drivers := make([]LiveDriver, 0, len(result))
	for driverID, coords := range result {
		parts := strings.Split(coords, ",")
		if len(parts) != 2 {
			continue
		}
		lat, err1 := strconv.ParseFloat(parts[0], 64)
		lng, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		drivers = append(drivers, LiveDriver{
			DriverID: driverID,
			Lat:      lat,
			Lng:      lng,
		})
	}

	return c.JSON(fiber.Map{
		"count":   len(drivers),
		"drivers": drivers,
	})
}

// ─────────────────────────────────────────────────────────────────
// GET /api/zones/heatmap
// Returns H3 zones with driver density for heatmap rendering
// ─────────────────────────────────────────────────────────────────

func (h *Handlers) GetHeatmap(c *fiber.Ctx) error {
	ctx := context.Background()

	zones, err := h.ch.GetHeatmap(ctx)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if zones == nil {
		zones = []HeatmapZone{}
	}

	return c.JSON(fiber.Map{
		"count": len(zones),
		"zones": zones,
	})
}

// ─────────────────────────────────────────────────────────────────
// GET /api/zones/:h3/stats
// Returns stats for a specific H3 zone
// ─────────────────────────────────────────────────────────────────

func (h *Handlers) GetZoneStats(c *fiber.Ctx) error {
	h3Cell := c.Params("h3")
	if h3Cell == "" {
		return c.Status(400).JSON(fiber.Map{"error": "h3 cell required"})
	}

	ctx := context.Background()
	stats, err := h.ch.GetZoneStats(ctx, h3Cell)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(stats)
}

// ─────────────────────────────────────────────────────────────────
// GET /api/trips?limit=20
// Returns recent completed trips
// ─────────────────────────────────────────────────────────────────

func (h *Handlers) GetTrips(c *fiber.Ctx) error {
	limit, _ := strconv.Atoi(c.Query("limit", "20"))
	ctx := context.Background()

	trips, err := h.ch.GetTrips(ctx, limit)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if trips == nil {
		trips = []Trip{}
	}

	return c.JSON(fiber.Map{
		"count": len(trips),
		"trips": trips,
	})
}

// ─────────────────────────────────────────────────────────────────
// GET /api/trips/:id/route
// Returns GPS breadcrumbs for trip replay
// ─────────────────────────────────────────────────────────────────

func (h *Handlers) GetTripRoute(c *fiber.Ctx) error {
	tripID := c.Params("id")
	if tripID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "trip id required"})
	}

	ctx := context.Background()

	// Get trip metadata first
	trips, err := h.ch.GetTrips(ctx, 100)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	var trip *Trip
	for i := range trips {
		if trips[i].TripID == tripID {
			trip = &trips[i]
			break
		}
	}

	if trip == nil {
		return c.Status(404).JSON(fiber.Map{"error": "trip not found"})
	}

	startTime, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", trip.StartTime)
	endTime, _ := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", trip.EndTime)

	// Fallback time parse
	if startTime.IsZero() {
		startTime = time.Now().Add(-1 * time.Hour)
	}
	if endTime.IsZero() {
		endTime = time.Now()
	}

	points, err := h.ch.GetTripRoute(ctx, trip.DriverID, startTime, endTime)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if points == nil {
		points = []GPSPoint{}
	}

	return c.JSON(fiber.Map{
		"trip_id":   tripID,
		"driver_id": trip.DriverID,
		"count":     len(points),
		"route":     points,
	})
}

// ─────────────────────────────────────────────────────────────────
// GET /api/stats/summary
// Returns overall pipeline statistics
// ─────────────────────────────────────────────────────────────────

func (h *Handlers) GetSummary(c *fiber.Ctx) error {
	ctx := context.Background()

	summary, err := h.ch.GetSummary(ctx)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Add live driver count from Redis
	liveCount, _ := h.rdb.HLen(ctx, "drivers:live").Result()
	
	return c.JSON(fiber.Map{
		"total_gps_events":      summary.TotalGPSEvents,
		"total_trips":           summary.TotalTrips,
		"avg_trip_distance_km":  summary.AvgTripDistance,
		"avg_trip_duration_secs": summary.AvgTripDuration,
		"active_zones":          summary.ActiveZones,
		"live_drivers":          liveCount,
	})
}