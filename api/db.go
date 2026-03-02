package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseDB struct {
	conn driver.Conn
}

func NewClickHouseDB(cfg *Config) (*ClickHouseDB, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Settings:        clickhouse.Settings{"max_execution_time": 30},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return &ClickHouseDB{conn: conn}, nil
}

// ─────────────────────────────────────────────────────────────────
// Heatmap — active drivers per H3 zone (last 5 minutes)
// ─────────────────────────────────────────────────────────────────

type HeatmapZone struct {
	H3Cell        string  `json:"h3_cell"`
	ActiveDrivers uint32  `json:"active_drivers"`
	AvgSpeed      float32 `json:"avg_speed"`
	DemandScore   float32 `json:"demand_score"`
	WindowStart   string  `json:"window_start"`
}

func (db *ClickHouseDB) GetHeatmap(ctx context.Context) ([]HeatmapZone, error) {
	rows, err := db.conn.Query(ctx, `
		SELECT h3_cell, active_drivers, avg_speed, demand_score, toString(window_start)
		FROM zone_metrics
		WHERE window_start >= now() - INTERVAL 2 HOUR
		ORDER BY active_drivers DESC
		LIMIT 500
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var zones []HeatmapZone
	for rows.Next() {
		var z HeatmapZone
		if err := rows.Scan(&z.H3Cell, &z.ActiveDrivers, &z.AvgSpeed, &z.DemandScore, &z.WindowStart); err != nil {
			continue
		}
		zones = append(zones, z)
	}
	return zones, nil
}

// ─────────────────────────────────────────────────────────────────
// Zone Stats — detail for a specific H3 cell
// ─────────────────────────────────────────────────────────────────

type ZoneStats struct {
	H3Cell          string  `json:"h3_cell"`
	TotalPings      uint64  `json:"total_pings"`
	AvgSpeed        float32 `json:"avg_speed"`
	MaxActiveDrivers uint32 `json:"max_active_drivers"`
	TotalTrips      uint64  `json:"total_trips"`
}

func (db *ClickHouseDB) GetZoneStats(ctx context.Context, h3Cell string) (*ZoneStats, error) {
	var stats ZoneStats
	stats.H3Cell = h3Cell

	// Zone metrics
	err := db.conn.QueryRow(ctx, `
		SELECT 
			sum(total_pings),
			avg(avg_speed),
			max(active_drivers)
		FROM zone_metrics
		WHERE h3_cell = ?
		AND window_start >= now() - INTERVAL 1 HOUR
	`, h3Cell).Scan(&stats.TotalPings, &stats.AvgSpeed, &stats.MaxActiveDrivers)
	if err != nil {
		return nil, err
	}

	// Trip count for this zone
	db.conn.QueryRow(ctx, `
		SELECT count()
		FROM trips
		WHERE start_h3 = ? OR end_h3 = ?
		AND start_time >= now() - INTERVAL 1 HOUR
	`, h3Cell, h3Cell).Scan(&stats.TotalTrips)

	return &stats, nil
}

// ─────────────────────────────────────────────────────────────────
// Trips — recent trip list
// ─────────────────────────────────────────────────────────────────

type Trip struct {
	TripID       string  `json:"trip_id"`
	DriverID     string  `json:"driver_id"`
	StartLat     float64 `json:"start_lat"`
	StartLng     float64 `json:"start_lng"`
	EndLat       float64 `json:"end_lat"`
	EndLng       float64 `json:"end_lng"`
	StartTime    string  `json:"start_time"`
	EndTime      string  `json:"end_time"`
	DurationSecs uint32  `json:"duration_secs"`
	DistanceKm   float32 `json:"distance_km"`
	StartH3      string  `json:"start_h3"`
	EndH3        string  `json:"end_h3"`
}

func (db *ClickHouseDB) GetTrips(ctx context.Context, limit int) ([]Trip, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := db.conn.Query(ctx, `
		SELECT 
			trip_id, driver_id,
			start_lat, start_lng, end_lat, end_lng,
			toString(start_time), toString(end_time),
			duration_secs, distance_km,
			start_h3, end_h3
		FROM trips
		ORDER BY start_time DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trips []Trip
	for rows.Next() {
		var t Trip
		if err := rows.Scan(
			&t.TripID, &t.DriverID,
			&t.StartLat, &t.StartLng, &t.EndLat, &t.EndLng,
			&t.StartTime, &t.EndTime,
			&t.DurationSecs, &t.DistanceKm,
			&t.StartH3, &t.EndH3,
		); err != nil {
			continue
		}
		trips = append(trips, t)
	}
	return trips, nil
}

// ─────────────────────────────────────────────────────────────────
// Trip Route — GPS breadcrumbs for a specific trip
// ─────────────────────────────────────────────────────────────────

type GPSPoint struct {
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Speed     float32 `json:"speed"`
	Timestamp string  `json:"timestamp"`
}

func (db *ClickHouseDB) GetTripRoute(ctx context.Context, driverID string, startTime, endTime time.Time) ([]GPSPoint, error) {
	rows, err := db.conn.Query(ctx, `
		SELECT lat, lng, speed, toString(timestamp)
		FROM raw_gps_events
		WHERE driver_id = ?
		AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp ASC
	`, driverID, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var points []GPSPoint
	for rows.Next() {
		var p GPSPoint
		if err := rows.Scan(&p.Lat, &p.Lng, &p.Speed, &p.Timestamp); err != nil {
			continue
		}
		points = append(points, p)
	}
	return points, nil
}

// ─────────────────────────────────────────────────────────────────
// Summary stats
// ─────────────────────────────────────────────────────────────────

type Summary struct {
	TotalGPSEvents  uint64  `json:"total_gps_events"`
	TotalTrips      uint64  `json:"total_trips"`
	AvgTripDistance float32 `json:"avg_trip_distance_km"`
	AvgTripDuration float32 `json:"avg_trip_duration_secs"`
	ActiveZones     uint64  `json:"active_zones"`
}

func (db *ClickHouseDB) GetSummary(ctx context.Context) (*Summary, error) {
	var s Summary

	db.conn.QueryRow(ctx, `SELECT count() FROM raw_gps_events`).Scan(&s.TotalGPSEvents)
	db.conn.QueryRow(ctx, `SELECT count(), avg(distance_km), avg(duration_secs) FROM trips`).
		Scan(&s.TotalTrips, &s.AvgTripDistance, &s.AvgTripDuration)
	db.conn.QueryRow(ctx, `
		SELECT count(DISTINCT h3_cell) FROM zone_metrics
		WHERE window_start >= now() - INTERVAL 2 HOUR
	`).Scan(&s.ActiveZones)

	return &s, nil
}