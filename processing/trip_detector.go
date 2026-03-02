package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const (
	tripIdleThreshold = 2 * time.Minute  // gap before declaring trip end
	driverStateTTL    = 10 * time.Minute // Redis key expiry
)

// ─────────────────────────────────────────────────────────────────
// Trip Detector
// ─────────────────────────────────────────────────────────────────

type TripDetector struct {
	reader    *kafka.Reader
	writer    *kafka.Writer
	redis     *redis.Client
	chWriter  *ClickHouseWriter
	processed int64
	errors    int64
}

func NewTripDetector(cfg *Config, rdb *redis.Client, chWriter *ClickHouseWriter) *TripDetector {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaBrokers},
		Topic:          cfg.KafkaTopicEnriched,
		GroupID:        "trip-detector",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers),
		Topic:        cfg.KafkaTopicTrips,
		Balancer:     &kafka.Hash{},
		BatchSize:    50,
		BatchTimeout: 50 * time.Millisecond,
	}

	return &TripDetector{
		reader:   reader,
		writer:   writer,
		redis:    rdb,
		chWriter: chWriter,
	}
}

func (t *TripDetector) Run(ctx context.Context) {
	log.Println("🚗 Trip detector started")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := t.reader.FetchMessage(ctxTimeout)
			cancel()

			if err != nil {
				continue
			}

			if err := t.processEvent(ctx, msg.Value); err != nil {
				t.errors++
			} else {
				t.processed++
			}

			t.reader.CommitMessages(ctx, msg)

			if t.processed%500 == 0 {
				log.Printf("✅ Trip Detector | Processed: %d | Errors: %d", t.processed, t.errors)
			}
		}
	}
}

func (t *TripDetector) processEvent(ctx context.Context, raw []byte) error {
	var event EnrichedGPSEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	// Get current driver state from Redis
	stateKey := fmt.Sprintf("driver:state:%s", event.DriverID)
	state, err := t.getDriverState(ctx, stateKey)
	if err != nil {
		// New driver — initialize state
		state = &DriverState{
			DriverID: event.DriverID,
			Status:   "idle",
		}
	}

	now := event.Timestamp
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// Update live position in Redis (for API use)
	t.redis.HSet(ctx, "drivers:live",
		event.DriverID, fmt.Sprintf("%.6f,%.6f", event.Lat, event.Lng),
	)
	t.redis.Expire(ctx, "drivers:live", 30*time.Second)

	// ── Trip state machine ──────────────────────────────────────
	switch {
	case event.Status == "moving" && state.Status != "moving":
		// TRIP START
		tripEvent := TripEvent{
			TripID:    event.TripID,
			DriverID:  event.DriverID,
			EventType: "trip_start",
			Lat:       event.Lat,
			Lng:       event.Lng,
			H3Cell:    event.H3Cell,
			Timestamp: now,
		}
		t.publishTripEvent(ctx, tripEvent)

		state.CurrentTripID = event.TripID
		state.TripStartTime = now
		state.TripStartLat = event.Lat
		state.TripStartLng = event.Lng
		state.TotalDistKm = 0
		state.Status = "moving"

		log.Printf("🟢 Trip START | Driver: %s | Zone: %s", event.DriverID, event.H3Cell)

	case event.Status == "idle" && state.Status == "moving":
		// TRIP END
		duration := now.Sub(state.TripStartTime)

		// Add last segment distance
		if state.LastLat != 0 {
			state.TotalDistKm += haversineKm(state.LastLat, state.LastLng, event.Lat, event.Lng)
		}

		tripEvent := TripEvent{
			TripID:       state.CurrentTripID,
			DriverID:     event.DriverID,
			EventType:    "trip_end",
			Lat:          event.Lat,
			Lng:          event.Lng,
			H3Cell:       event.H3Cell,
			Timestamp:    now,
			DurationSecs: int64(duration.Seconds()),
			DistanceKm:   state.TotalDistKm,
		}
		t.publishTripEvent(ctx, tripEvent)

		// Write completed trip to ClickHouse
		t.chWriter.WriteTrip(ctx, state, event, now)

		state.Status = "idle"
		state.CurrentTripID = ""
		state.TotalDistKm = 0

		log.Printf("🔴 Trip END | Driver: %s | Duration: %.0fs | Dist: %.2fkm",
			event.DriverID, duration.Seconds(), tripEvent.DistanceKm)

	case event.Status == "moving" && state.Status == "moving":
		// ONGOING TRIP — accumulate distance
		if state.LastLat != 0 {
			dist := haversineKm(state.LastLat, state.LastLng, event.Lat, event.Lng)
			// Filter GPS jumps > 1km in 3s (impossible, likely noise)
			if dist < 1.0 {
				state.TotalDistKm += dist
			}
		}
	}

	// Update state
	state.LastLat = event.Lat
	state.LastLng = event.Lng
	state.LastSeen = now

	return t.saveDriverState(ctx, stateKey, state)
}

func (t *TripDetector) publishTripEvent(ctx context.Context, event TripEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	t.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.DriverID),
		Value: payload,
	})
}

func (t *TripDetector) getDriverState(ctx context.Context, key string) (*DriverState, error) {
	val, err := t.redis.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	var state DriverState
	if err := json.Unmarshal([]byte(val), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (t *TripDetector) saveDriverState(ctx context.Context, key string, state *DriverState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return t.redis.Set(ctx, key, data, driverStateTTL).Err()
}