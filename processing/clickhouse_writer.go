package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseWriter struct {
	conn driver.Conn
}

func NewClickHouseWriter(cfg *Config) (*ClickHouseWriter, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Settings: clickhouse.Settings{"max_execution_time": 60},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	log.Println("✅ ClickHouse connected")
	return &ClickHouseWriter{conn: conn}, nil
}

func (w *ClickHouseWriter) WriteGPSEvents(ctx context.Context, events []EnrichedGPSEvent) error {
	batch, err := w.conn.PrepareBatch(ctx,
		"INSERT INTO raw_gps_events (driver_id, lat, lng, speed, heading, accuracy, timestamp, h3_cell, area_name, road_snapped_lat, road_snapped_lng)",
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, e := range events {
		ts := e.Timestamp
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		if err := batch.Append(
			e.DriverID, e.Lat, e.Lng,
			float32(e.Speed), float32(e.Heading), float32(e.Accuracy),
			ts, e.H3Cell, e.AreaName, e.Lat, e.Lng,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (w *ClickHouseWriter) WriteTrip(ctx context.Context, state *DriverState, endEvent EnrichedGPSEvent, endTime time.Time) error {
	duration := endTime.Sub(state.TripStartTime)
	return w.conn.Exec(ctx, `
		INSERT INTO trips (trip_id, driver_id, start_lat, start_lng, end_lat, end_lng, start_time, end_time, duration_secs, distance_km, start_h3, end_h3, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		state.CurrentTripID, state.DriverID,
		state.TripStartLat, state.TripStartLng,
		endEvent.Lat, endEvent.Lng,
		state.TripStartTime, endTime,
		uint32(duration.Seconds()),
		float32(state.TotalDistKm),
		getH3Cell(state.TripStartLat, state.TripStartLng),
		endEvent.H3Cell, "completed",
	)
}

type ZoneMetricRow struct {
	H3Cell        string
	WindowStart   time.Time
	ActiveDrivers uint32
	AvgSpeed      float32
	TotalPings    uint32
}

func (w *ClickHouseWriter) WriteZoneMetrics(ctx context.Context, metrics []ZoneMetricRow) error {
	batch, err := w.conn.PrepareBatch(ctx,
		"INSERT INTO zone_metrics (window_start, h3_cell, active_drivers, avg_speed, total_pings, demand_score)",
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, m := range metrics {
		if err := batch.Append(
			m.WindowStart, m.H3Cell,
			m.ActiveDrivers, m.AvgSpeed,
			m.TotalPings, float32(m.ActiveDrivers)*0.5,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}