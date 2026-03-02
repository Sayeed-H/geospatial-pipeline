package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// ─────────────────────────────────────────────────────────────────
// Dhaka city bounding box
// ─────────────────────────────────────────────────────────────────
const (
	DhakaMinLat = 23.6850
	DhakaMaxLat = 23.9000
	DhakaMinLng = 90.3300
	DhakaMaxLng = 90.5100
)

// Dhaka hotspot zones (popular areas)
var dhakaHotspots = []struct {
	Name string
	Lat  float64
	Lng  float64
}{
	{"Motijheel", 23.7337, 90.4176},
	{"Gulshan-1", 23.7808, 90.4148},
	{"Gulshan-2", 23.7934, 90.4143},
	{"Banani", 23.7937, 90.4066},
	{"Dhanmondi", 23.7461, 90.3742},
	{"Mirpur-10", 23.8069, 90.3674},
	{"Uttara", 23.8759, 90.3795},
	{"Mohakhali", 23.7784, 90.4007},
	{"Farmgate", 23.7579, 90.3893},
	{"Shahbag", 23.7388, 90.3960},
	{"Bashundhara", 23.8142, 90.4267},
	{"Rampura", 23.7652, 90.4294},
	{"Badda", 23.7783, 90.4294},
	{"Airport", 23.8520, 90.4070},
	{"Sadarghat", 23.7185, 90.4072},
}

// ─────────────────────────────────────────────────────────────────
// Data structures
// ─────────────────────────────────────────────────────────────────

type GPSEvent struct {
	DriverID  string    `json:"driver_id"`
	Lat       float64   `json:"lat"`
	Lng       float64   `json:"lng"`
	Speed     float64   `json:"speed"`      // km/h
	Heading   float64   `json:"heading"`    // degrees 0-360
	Accuracy  float64   `json:"accuracy"`   // meters
	Timestamp time.Time `json:"timestamp"`
	TripID    string    `json:"trip_id"`
	Status    string    `json:"status"` // idle, moving, trip_active
}

type Driver struct {
	ID          string
	Lat         float64
	Lng         float64
	Heading     float64
	Speed       float64
	Status      string
	TripID      string
	TargetLat   float64
	TargetLng   float64
	WaypointIdx int
	IdleTimer   int // seconds remaining idle
}

// ─────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────

type Config struct {
	MQTTBroker    string
	MQTTTopic     string
	NumDrivers    int
	EmitIntervalS int
}

func loadConfig() Config {
	broker := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	topic := getEnv("MQTT_TOPIC", "gps/drivers")
	numDrivers, _ := strconv.Atoi(getEnv("NUM_DRIVERS", "100"))
	interval, _ := strconv.Atoi(getEnv("EMIT_INTERVAL_S", "3"))

	return Config{
		MQTTBroker:    broker,
		MQTTTopic:     topic,
		NumDrivers:    numDrivers,
		EmitIntervalS: interval,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─────────────────────────────────────────────────────────────────
// Movement Engine
// ─────────────────────────────────────────────────────────────────

func newDriver(id string) *Driver {
	hotspot := dhakaHotspots[rand.Intn(len(dhakaHotspots))]
	lat := hotspot.Lat + (rand.Float64()-0.5)*0.02
	lng := hotspot.Lng + (rand.Float64()-0.5)*0.02

	d := &Driver{
		ID:      id,
		Lat:     clamp(lat, DhakaMinLat, DhakaMaxLat),
		Lng:     clamp(lng, DhakaMinLng, DhakaMaxLng),
		Heading: rand.Float64() * 360,
		Speed:   0,
		Status:  "idle",
	}
	d.pickNewTarget()
	return d
}

func (d *Driver) pickNewTarget() {
	// 70% chance to target a hotspot, 30% random
	if rand.Float64() < 0.70 {
		hotspot := dhakaHotspots[rand.Intn(len(dhakaHotspots))]
		d.TargetLat = hotspot.Lat + (rand.Float64()-0.5)*0.005
		d.TargetLng = hotspot.Lng + (rand.Float64()-0.5)*0.005
	} else {
		d.TargetLat = DhakaMinLat + rand.Float64()*(DhakaMaxLat-DhakaMinLat)
		d.TargetLng = DhakaMinLng + rand.Float64()*(DhakaMaxLng-DhakaMinLng)
	}
}

func (d *Driver) update(deltaSeconds float64) {
	switch d.Status {
	case "idle":
		d.Speed = 0
		d.IdleTimer--
		if d.IdleTimer <= 0 {
			d.Status = "moving"
			d.TripID = uuid.New().String()
			d.pickNewTarget()
		}

	case "moving", "trip_active":
		// Calculate bearing to target
		bearing := bearingTo(d.Lat, d.Lng, d.TargetLat, d.TargetLng)
		d.Heading = bearing

		// Speed: 10-60 km/h, simulate traffic with noise
		targetSpeed := 20.0 + rand.Float64()*40.0
		d.Speed += (targetSpeed - d.Speed) * 0.3 // smooth acceleration
		d.Speed = clamp(d.Speed, 5, 65)

		// Occasionally inject anomaly (for ML training data)
		if rand.Float64() < 0.002 {
			d.Speed = 90 + rand.Float64()*50 // speed anomaly
		}

		// Move toward target
		speedKmPerSec := d.Speed / 3600.0
		distToTarget := haversine(d.Lat, d.Lng, d.TargetLat, d.TargetLng)

		if distToTarget < 0.1 { // within 100m of target
			d.Lat = d.TargetLat
			d.Lng = d.TargetLng
			d.Status = "idle"
			d.TripID = ""
			d.Speed = 0
			d.IdleTimer = rand.Intn(60) + 10 // idle 10-70 seconds
			d.pickNewTarget()
		} else {
			// Move fraction of distance
			fraction := (speedKmPerSec * deltaSeconds) / distToTarget
			fraction = math.Min(fraction, 1.0)

			d.Lat += (d.TargetLat - d.Lat) * fraction
			d.Lng += (d.TargetLng - d.Lng) * fraction

			// Keep within Dhaka bounds
			d.Lat = clamp(d.Lat, DhakaMinLat, DhakaMaxLat)
			d.Lng = clamp(d.Lng, DhakaMinLng, DhakaMaxLng)
		}
	}
}

func (d *Driver) toEvent() GPSEvent {
	// Add GPS noise (realistic 3-8m accuracy)
	noise := 0.00005
	return GPSEvent{
		DriverID:  d.ID,
		Lat:       d.Lat + (rand.Float64()-0.5)*noise,
		Lng:       d.Lng + (rand.Float64()-0.5)*noise,
		Speed:     math.Round(d.Speed*10) / 10,
		Heading:   math.Round(d.Heading*10) / 10,
		Accuracy:  3.0 + rand.Float64()*5.0,
		Timestamp: time.Now().UTC(),
		TripID:    d.TripID,
		Status:    d.Status,
	}
}

// ─────────────────────────────────────────────────────────────────
// Simulator
// ─────────────────────────────────────────────────────────────────

type Simulator struct {
	config  Config
	client  mqtt.Client
	drivers []*Driver
	mu      sync.RWMutex
	stats   SimStats
}

type SimStats struct {
	TotalPublished uint64
	Errors         uint64
	ActiveDrivers  int
	mu             sync.Mutex
}

func newSimulator(config Config) *Simulator {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.MQTTBroker)
	opts.SetClientID("gps-simulator-" + uuid.New().String()[:8])
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	opts.OnConnect = func(c mqtt.Client) {
		log.Println("✅ Connected to MQTT broker")
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("⚠️  MQTT connection lost: %v", err)
	}

	client := mqtt.NewClient(opts)

	// Initialize drivers
	drivers := make([]*Driver, config.NumDrivers)
	for i := 0; i < config.NumDrivers; i++ {
		driverID := fmt.Sprintf("DRV-%04d", i+1)
		drivers[i] = newDriver(driverID)
		// Stagger initial idle timers
		drivers[i].IdleTimer = rand.Intn(30)
		if rand.Float64() > 0.3 {
			drivers[i].Status = "moving"
			drivers[i].TripID = uuid.New().String()
		}
	}

	return &Simulator{
		config:  config,
		client:  client,
		drivers: drivers,
	}
}

func (s *Simulator) connect() error {
	log.Printf("🔌 Connecting to MQTT broker: %s", s.config.MQTTBroker)
	token := s.client.Connect()
	token.Wait()
	return token.Error()
}

func (s *Simulator) run() {
	ticker := time.NewTicker(time.Duration(s.config.EmitIntervalS) * time.Second)
	statsTicker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer statsTicker.Stop()

	deltaSeconds := float64(s.config.EmitIntervalS)

	log.Printf("🚗 Starting simulation: %d drivers in Dhaka, emitting every %ds",
		s.config.NumDrivers, s.config.EmitIntervalS)

	for {
		select {
		case <-ticker.C:
			s.tick(deltaSeconds)

		case <-statsTicker.C:
			s.printStats()
		}
	}
}

func (s *Simulator) tick(deltaSeconds float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	activeCount := 0
	var wg sync.WaitGroup

	for _, driver := range s.drivers {
		wg.Add(1)
		go func(d *Driver) {
			defer wg.Done()

			d.update(deltaSeconds)
			event := d.toEvent()

			payload, err := json.Marshal(event)
			if err != nil {
				s.stats.mu.Lock()
				s.stats.Errors++
				s.stats.mu.Unlock()
				return
			}

			topic := fmt.Sprintf("%s/%s", s.config.MQTTTopic, d.ID)
			token := s.client.Publish(topic, 1, false, payload)
			token.Wait()

			if token.Error() != nil {
				s.stats.mu.Lock()
				s.stats.Errors++
				s.stats.mu.Unlock()
			} else {
				s.stats.mu.Lock()
				s.stats.TotalPublished++
				s.stats.mu.Unlock()
			}
		}(driver)

		if driver.Status != "idle" {
			activeCount++
		}
	}

	wg.Wait()
	s.stats.mu.Lock()
	s.stats.ActiveDrivers = activeCount
	s.stats.mu.Unlock()
}

func (s *Simulator) printStats() {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	log.Printf("📊 Stats | Published: %d | Errors: %d | Active Drivers: %d/%d",
		s.stats.TotalPublished,
		s.stats.Errors,
		s.stats.ActiveDrivers,
		s.config.NumDrivers,
	)
}

// ─────────────────────────────────────────────────────────────────
// Math helpers
// ─────────────────────────────────────────────────────────────────

func haversine(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180
	dLng := (lng2 - lng1) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLng/2)*math.Sin(dLng/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func bearingTo(lat1, lng1, lat2, lng2 float64) float64 {
	dLng := (lng2 - lng1) * math.Pi / 180
	lat1R := lat1 * math.Pi / 180
	lat2R := lat2 * math.Pi / 180
	y := math.Sin(dLng) * math.Cos(lat2R)
	x := math.Cos(lat1R)*math.Sin(lat2R) - math.Sin(lat1R)*math.Cos(lat2R)*math.Cos(dLng)
	bearing := math.Atan2(y, x) * 180 / math.Pi
	return math.Mod(bearing+360, 360)
}

func clamp(val, min, max float64) float64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// ─────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	config := loadConfig()
	sim := newSimulator(config)

	if err := sim.connect(); err != nil {
		log.Fatalf("❌ Failed to connect to MQTT: %v", err)
	}

	go sim.run()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down simulator...")
	sim.client.Disconnect(500)
}