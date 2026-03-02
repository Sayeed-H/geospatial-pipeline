package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const windowDuration = 5 * time.Minute

// ─────────────────────────────────────────────────────────────────
// Zone Aggregator
// ─────────────────────────────────────────────────────────────────

type ZoneAggregator struct {
	reader    *kafka.Reader
	chWriter  *ClickHouseWriter
	windows   map[string]*ZoneWindow // h3_cell → current window
	mu        sync.Mutex
	processed int64
	errors    int64
}

func NewZoneAggregator(cfg *Config, chWriter *ClickHouseWriter) *ZoneAggregator {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaBrokers},
		Topic:          cfg.KafkaTopicEnriched,
		GroupID:        "zone-aggregator",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &ZoneAggregator{
		reader:   reader,
		chWriter: chWriter,
		windows:  make(map[string]*ZoneWindow),
	}
}

func (z *ZoneAggregator) Run(ctx context.Context) {
	log.Println("📊 Zone aggregator started")

	// Window flush ticker — every 5 minutes
	flushTicker := time.NewTicker(windowDuration)
	defer flushTicker.Stop()

	// Stats ticker
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			z.flushWindows(ctx)
			return

		case <-flushTicker.C:
			z.flushWindows(ctx)

		case <-statsTicker.C:
			z.mu.Lock()
			activeZones := len(z.windows)
			z.mu.Unlock()
			log.Printf("✅ Zone Aggregator | Processed: %d | Active Zones: %d", z.processed, activeZones)

		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := z.reader.FetchMessage(ctxTimeout)
			cancel()

			if err != nil {
				continue
			}

			if err := z.processEvent(msg.Value); err != nil {
				z.errors++
			} else {
				z.processed++
			}

			z.reader.CommitMessages(ctx, msg)
		}
	}
}

func (z *ZoneAggregator) processEvent(raw []byte) error {
	var event EnrichedGPSEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if event.H3Cell == "" {
		return nil
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	window, exists := z.windows[event.H3Cell]
	if !exists {
		window = &ZoneWindow{
			H3Cell:       event.H3Cell,
			WindowStart:  time.Now().UTC().Truncate(windowDuration),
			DriverSpeeds: make(map[string][]float64),
		}
		z.windows[event.H3Cell] = window
	}

	window.DriverSpeeds[event.DriverID] = append(
		window.DriverSpeeds[event.DriverID],
		event.Speed,
	)

	return nil
}

func (z *ZoneAggregator) flushWindows(ctx context.Context) {
	z.mu.Lock()
	// Snapshot and reset
	snapshot := z.windows
	z.windows = make(map[string]*ZoneWindow)
	z.mu.Unlock()

	if len(snapshot) == 0 {
		return
	}

	metrics := make([]ZoneMetricRow, 0, len(snapshot))
	for _, window := range snapshot {
		totalPings := uint32(0)
		totalSpeed := 0.0

		for _, speeds := range window.DriverSpeeds {
			for _, s := range speeds {
				totalSpeed += s
				totalPings++
			}
		}

		avgSpeed := float32(0)
		if totalPings > 0 {
			avgSpeed = float32(totalSpeed / float64(totalPings))
		}

		metrics = append(metrics, ZoneMetricRow{
			H3Cell:        window.H3Cell,
			WindowStart:   window.WindowStart,
			ActiveDrivers: uint32(len(window.DriverSpeeds)),
			AvgSpeed:      avgSpeed,
			TotalPings:    totalPings,
		})
	}

	if err := z.chWriter.WriteZoneMetrics(ctx, metrics); err != nil {
		log.Printf("❌ Zone metrics write error: %v", err)
		z.errors += int64(len(metrics))
	} else {
		log.Printf("💾 Flushed %d zone metrics to ClickHouse (window: %s)",
			len(metrics), time.Now().UTC().Truncate(windowDuration).Format("15:04"))
	}
}