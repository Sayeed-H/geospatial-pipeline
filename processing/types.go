package main

import "time"

// ─────────────────────────────────────────────────────────────────
// Raw GPS event from Kafka (raw.gps.events)
// ─────────────────────────────────────────────────────────────────

type RawGPSEvent struct {
	DriverID   string    `json:"driver_id"`
	Lat        float64   `json:"lat"`
	Lng        float64   `json:"lng"`
	Speed      float64   `json:"speed"`
	Heading    float64   `json:"heading"`
	Accuracy   float64   `json:"accuracy"`
	Timestamp  time.Time `json:"timestamp"`
	TripID     string    `json:"trip_id"`
	Status     string    `json:"status"`
	IngestedAt string    `json:"ingested_at"`
}

// ─────────────────────────────────────────────────────────────────
// Enriched GPS event → enriched.gps.events
// ─────────────────────────────────────────────────────────────────

type EnrichedGPSEvent struct {
	RawGPSEvent
	H3Cell   string `json:"h3_cell"`
	AreaName string `json:"area_name"`
}

// ─────────────────────────────────────────────────────────────────
// Trip event → trip.lifecycle.events
// ─────────────────────────────────────────────────────────────────

type TripEvent struct {
	TripID       string    `json:"trip_id"`
	DriverID     string    `json:"driver_id"`
	EventType    string    `json:"event_type"` // trip_start, trip_end
	Lat          float64   `json:"lat"`
	Lng          float64   `json:"lng"`
	H3Cell       string    `json:"h3_cell"`
	Timestamp    time.Time `json:"timestamp"`
	DurationSecs int64     `json:"duration_secs,omitempty"`
	DistanceKm   float64   `json:"distance_km,omitempty"`
}

// ─────────────────────────────────────────────────────────────────
// Driver state tracked in Redis
// ─────────────────────────────────────────────────────────────────

type DriverState struct {
	DriverID      string    `json:"driver_id"`
	LastLat       float64   `json:"last_lat"`
	LastLng       float64   `json:"last_lng"`
	LastSeen      time.Time `json:"last_seen"`
	CurrentTripID string    `json:"current_trip_id"`
	TripStartTime time.Time `json:"trip_start_time"`
	TripStartLat  float64   `json:"trip_start_lat"`
	TripStartLng  float64   `json:"trip_start_lng"`
	TotalDistKm   float64   `json:"total_dist_km"`
	Status        string    `json:"status"`
}

// ─────────────────────────────────────────────────────────────────
// Zone aggregation window
// ─────────────────────────────────────────────────────────────────

type ZoneWindow struct {
	H3Cell        string
	WindowStart   time.Time
	DriverSpeeds  map[string][]float64 // driver_id → speeds
}