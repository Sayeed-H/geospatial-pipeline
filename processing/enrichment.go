package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/segmentio/kafka-go"
)

// ─────────────────────────────────────────────────────────────────
// Dhaka area definitions for reverse zone lookup
// ─────────────────────────────────────────────────────────────────

type AreaZone struct {
	Name   string
	Lat    float64
	Lng    float64
	RadiusKm float64
}

var dhakaAreas = []AreaZone{
	{"Motijheel", 23.7337, 90.4176, 1.0},
	{"Gulshan-1", 23.7808, 90.4148, 0.8},
	{"Gulshan-2", 23.7934, 90.4143, 0.8},
	{"Banani", 23.7937, 90.4066, 0.8},
	{"Dhanmondi", 23.7461, 90.3742, 1.2},
	{"Mirpur-10", 23.8069, 90.3674, 1.0},
	{"Uttara", 23.8759, 90.3795, 1.5},
	{"Mohakhali", 23.7784, 90.4007, 0.8},
	{"Farmgate", 23.7579, 90.3893, 0.6},
	{"Shahbag", 23.7388, 90.3960, 0.6},
	{"Bashundhara", 23.8142, 90.4267, 1.2},
	{"Rampura", 23.7652, 90.4294, 0.8},
	{"Badda", 23.7783, 90.4294, 0.8},
	{"Airport", 23.8520, 90.4070, 1.5},
	{"Sadarghat", 23.7185, 90.4072, 0.8},
	{"Old Dhaka", 23.7104, 90.4074, 1.5},
	{"Tejgaon", 23.7677, 90.3959, 0.8},
	{"Khilgaon", 23.7452, 90.4294, 0.8},
	{"Wari", 23.7205, 90.4176, 0.6},
	{"Lalbagh", 23.7176, 90.3876, 0.8},
}

// getAreaName returns the name of the nearest Dhaka area within radius
func getAreaName(lat, lng float64) string {
	for _, area := range dhakaAreas {
		dist := haversineKm(lat, lng, area.Lat, area.Lng)
		if dist <= area.RadiusKm {
			return area.Name
		}
	}
	return "Dhaka"
}

// getH3Cell returns a cell string equivalent to H3 resolution 8 (~460m)
// Uses quantized lat/lng with ~0.004 degree grid spacing (≈460m at Dhaka latitude)
func getH3Cell(lat, lng float64) string {
	// Resolution 8 equivalent: ~0.004 degrees ≈ 460m at 23°N
	const resolution = 0.004
	cellLat := math.Floor(lat/resolution) * resolution
	cellLng := math.Floor(lng/resolution) * resolution
	// Encode as hex-like string matching H3 format visually
	latInt := int64((cellLat + 90) * 10000)
	lngInt := int64((cellLng + 180) * 10000)
	return fmt.Sprintf("8a%08x%08x", latInt, lngInt)
}

func haversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180
	dLng := (lng2 - lng1) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLng/2)*math.Sin(dLng/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// ─────────────────────────────────────────────────────────────────
// Enrichment Processor
// ─────────────────────────────────────────────────────────────────

type EnrichmentProcessor struct {
	reader      *kafka.Reader
	writer      *kafka.Writer
	chWriter    *ClickHouseWriter
	batchSize   int
	batch       []EnrichedGPSEvent
	processed   int64
	errors      int64
}

func NewEnrichmentProcessor(cfg *Config, chWriter *ClickHouseWriter) *EnrichmentProcessor {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaBrokers},
		Topic:          cfg.KafkaTopicRaw,
		GroupID:        "enrichment-processor",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers),
		Topic:        cfg.KafkaTopicEnriched,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
	}

	return &EnrichmentProcessor{
		reader:    reader,
		writer:    writer,
		chWriter:  chWriter,
		batchSize: 200,
		batch:     make([]EnrichedGPSEvent, 0, 200),
	}
}

func (p *EnrichmentProcessor) Run(ctx context.Context) {
	log.Println("🔄 Enrichment processor started")

	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.flush(ctx)
			return

		case <-flushTicker.C:
			if len(p.batch) > 0 {
				p.flush(ctx)
			}

		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := p.reader.FetchMessage(ctxTimeout)
			cancel()

			if err != nil {
				continue
			}

			enriched, err := p.enrich(msg.Value)
			if err != nil {
				p.errors++
				p.reader.CommitMessages(ctx, msg)
				continue
			}

			// Publish to enriched.gps.events
			payload, _ := json.Marshal(enriched)
			p.writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(enriched.DriverID),
				Value: payload,
			})

			p.batch = append(p.batch, enriched)
			p.processed++

			if len(p.batch) >= p.batchSize {
				p.flush(ctx)
			}

			p.reader.CommitMessages(ctx, msg)

			if p.processed%500 == 0 {
				log.Printf("✅ Enrichment | Processed: %d | Errors: %d", p.processed, p.errors)
			}
		}
	}
}

func (p *EnrichmentProcessor) enrich(raw []byte) (EnrichedGPSEvent, error) {
	var event RawGPSEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return EnrichedGPSEvent{}, fmt.Errorf("unmarshal: %w", err)
	}

	enriched := EnrichedGPSEvent{
		RawGPSEvent: event,
		H3Cell:      getH3Cell(event.Lat, event.Lng),
		AreaName:    getAreaName(event.Lat, event.Lng),
	}

	return enriched, nil
}

func (p *EnrichmentProcessor) flush(ctx context.Context) {
	if len(p.batch) == 0 {
		return
	}
	if err := p.chWriter.WriteGPSEvents(ctx, p.batch); err != nil {
		log.Printf("❌ ClickHouse write error: %v", err)
		p.errors += int64(len(p.batch))
	} else {
		log.Printf("💾 Flushed %d GPS events to ClickHouse", len(p.batch))
	}
	p.batch = p.batch[:0]
}