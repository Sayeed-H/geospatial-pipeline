package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

// ─────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────

type Config struct {
	MQTTBroker        string
	MQTTTopic         string
	KafkaBrokers      string
	KafkaTopicRaw     string
	WorkerCount       int
	ChannelBufferSize int
}

func loadConfig() Config {
	workers, _ := strconv.Atoi(getEnv("WORKER_COUNT", "10"))
	bufSize, _ := strconv.Atoi(getEnv("CHANNEL_BUFFER", "10000"))
	return Config{
		MQTTBroker:        getEnv("MQTT_BROKER", "tcp://localhost:1883"),
		MQTTTopic:         getEnv("MQTT_TOPIC", "gps/drivers/#"),
		KafkaBrokers:      getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopicRaw:     getEnv("KAFKA_TOPIC_RAW", "raw.gps.events"),
		WorkerCount:       workers,
		ChannelBufferSize: bufSize,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─────────────────────────────────────────────────────────────────
// Metrics
// ─────────────────────────────────────────────────────────────────

var (
	messagesReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bridge_mqtt_messages_received_total",
		Help: "Total MQTT messages received",
	})
	messagesPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bridge_kafka_messages_published_total",
		Help: "Total Kafka messages published",
	})
	publishErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bridge_kafka_publish_errors_total",
		Help: "Total Kafka publish errors",
	})
	channelDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_channel_depth",
		Help: "Current depth of message channel",
	})
)

func init() {
	prometheus.MustRegister(messagesReceived, messagesPublished, publishErrors, channelDepth)
}

// ─────────────────────────────────────────────────────────────────
// Message
// ─────────────────────────────────────────────────────────────────

type RawMessage struct {
	Topic   string
	Payload []byte
}

// ─────────────────────────────────────────────────────────────────
// Bridge
// ─────────────────────────────────────────────────────────────────

type Bridge struct {
	config    Config
	mqttCli   mqtt.Client
	writer    *kafka.Writer
	msgChan   chan RawMessage
	processed atomic.Int64
	errors    atomic.Int64
}

func newBridge(config Config) *Bridge {
	// Pure Go Kafka writer — no CGO needed
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.KafkaBrokers),
		Topic:        config.KafkaTopicRaw,
		Balancer:     &kafka.Hash{}, // same key → same partition
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		ErrorLogger:  kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("❌ Kafka error: "+msg, args...)
			publishErrors.Inc()
		}),
	}

	b := &Bridge{
		config:  config,
		writer:  writer,
		msgChan: make(chan RawMessage, config.ChannelBufferSize),
	}

	// MQTT client setup
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.MQTTBroker)
	opts.SetClientID("mqtt-kafka-bridge-" + uuid.New().String()[:8])
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetDefaultPublishHandler(b.mqttHandler)
	opts.OnConnect = func(c mqtt.Client) {
		log.Println("✅ MQTT connected, subscribing to", config.MQTTTopic)
		token := c.Subscribe(config.MQTTTopic, 1, nil)
		token.Wait()
		if token.Error() != nil {
			log.Printf("❌ Subscribe error: %v", token.Error())
		} else {
			log.Printf("✅ Subscribed to %s", config.MQTTTopic)
		}
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("⚠️  MQTT connection lost: %v", err)
	}

	b.mqttCli = mqtt.NewClient(opts)
	return b
}

func (b *Bridge) mqttHandler(_ mqtt.Client, msg mqtt.Message) {
	messagesReceived.Inc()
	select {
	case b.msgChan <- RawMessage{Topic: msg.Topic(), Payload: msg.Payload()}:
	default:
		log.Println("⚠️  Channel full, dropping message")
		b.errors.Add(1)
	}
}

func (b *Bridge) startWorkers(ctx context.Context) {
	for i := 0; i < b.config.WorkerCount; i++ {
		go b.worker(ctx)
	}
	log.Printf("🔧 Started %d Kafka publisher workers", b.config.WorkerCount)
}

func (b *Bridge) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-b.msgChan:
			channelDepth.Set(float64(len(b.msgChan)))
			b.publishToKafka(ctx, msg)
		}
	}
}

func (b *Bridge) publishToKafka(ctx context.Context, msg RawMessage) {
	// Enrich with metadata
	enriched := map[string]interface{}{}
	if err := json.Unmarshal(msg.Payload, &enriched); err != nil {
		b.errors.Add(1)
		return
	}
	enriched["ingested_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	enriched["mqtt_topic"] = msg.Topic

	payload, err := json.Marshal(enriched)
	if err != nil {
		b.errors.Add(1)
		return
	}

	// Use driver_id as partition key
	var key []byte
	if driverID, ok := enriched["driver_id"].(string); ok {
		key = []byte(driverID)
	}

	err = b.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: payload,
	})

	if err != nil {
		publishErrors.Inc()
		b.errors.Add(1)
	} else {
		messagesPublished.Inc()
		b.processed.Add(1)
	}
}

func (b *Bridge) startStatsLogger(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Printf("📊 Bridge Stats | Processed: %d | Errors: %d | Channel: %d/%d",
					b.processed.Load(),
					b.errors.Load(),
					len(b.msgChan),
					b.config.ChannelBufferSize,
				)
			}
		}
	}()
}

func (b *Bridge) run(ctx context.Context) error {
	// Connect MQTT
	token := b.mqttCli.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}

	b.startWorkers(ctx)
	b.startStatsLogger(ctx)

	// Metrics + health server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
		log.Println("📡 Metrics server on :2112")
		http.ListenAndServe(":2112", nil)
	}()

	<-ctx.Done()
	return nil
}

func (b *Bridge) shutdown() {
	log.Println("🛑 Shutting down bridge...")
	b.mqttCli.Disconnect(1000)
	b.writer.Close()
	log.Println("✅ Bridge shutdown complete")
}

// ─────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────

func main() {
	config := loadConfig()

	log.Printf("🌉 MQTT→Kafka Bridge starting...")
	log.Printf("   MQTT:  %s → topic: %s", config.MQTTBroker, config.MQTTTopic)
	log.Printf("   Kafka: %s → topic: %s", config.KafkaBrokers, config.KafkaTopicRaw)

	bridge := newBridge(config)

	ctx, cancel := context.WithCancel(context.Background())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		cancel()
	}()

	if err := bridge.run(ctx); err != nil {
		log.Fatalf("❌ Bridge error: %v", err)
	}

	bridge.shutdown()
}