package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/websocket/v2"
	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────
// WebSocket Hub
// Broadcasts live driver positions to all connected clients
// ─────────────────────────────────────────────────────────────────

type WSClient struct {
	conn *websocket.Conn
	send chan []byte
}

type WebSocketHub struct {
	clients    map[*WSClient]bool
	broadcast  chan []byte
	register   chan *WSClient
	unregister chan *WSClient
	mu         sync.RWMutex
	rdb        *redis.Client
}

func NewWebSocketHub(rdb *redis.Client) *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[*WSClient]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *WSClient),
		unregister: make(chan *WSClient),
		rdb:        rdb,
	}
}

func (h *WebSocketHub) Run() {
	// Ticker to push live positions every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("🔌 WS client connected. Total: %d", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("🔌 WS client disconnected. Total: %d", len(h.clients))

		case msg := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- msg:
				default:
					// Client too slow, drop message
				}
			}
			h.mu.RUnlock()

		case <-ticker.C:
			h.mu.RLock()
			clientCount := len(h.clients)
			h.mu.RUnlock()

			if clientCount == 0 {
				continue
			}

			// Fetch live positions from Redis and broadcast
			payload := h.buildLivePayload()
			if payload != nil {
				h.broadcast <- payload
			}
		}
	}
}

func (h *WebSocketHub) buildLivePayload() []byte {
	ctx := context.Background()

	result, err := h.rdb.HGetAll(ctx, "drivers:live").Result()
	if err != nil || len(result) == 0 {
		return nil
	}

	type DriverPos struct {
		DriverID string  `json:"driver_id"`
		Lat      float64 `json:"lat"`
		Lng      float64 `json:"lng"`
	}

	drivers := make([]DriverPos, 0, len(result))
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
		drivers = append(drivers, DriverPos{
			DriverID: driverID,
			Lat:      lat,
			Lng:      lng,
		})
	}

	payload, err := json.Marshal(map[string]interface{}{
		"type":      "live_positions",
		"count":     len(drivers),
		"drivers":   drivers,
		"timestamp": time.Now().UTC(),
	})
	if err != nil {
		return nil
	}
	return payload
}

// HandleWS — WebSocket connection handler
func (h *WebSocketHub) HandleWS(c *websocket.Conn) {
	client := &WSClient{
		conn: c,
		send: make(chan []byte, 64),
	}
	h.register <- client

	// Write pump — sends messages to client
	go func() {
		defer func() {
			h.unregister <- client
			c.Close()
		}()
		for msg := range client.send {
			if err := c.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	// Read pump — keeps connection alive, handles client messages
	for {
		_, _, err := c.ReadMessage()
		if err != nil {
			break
		}
	}
	h.unregister <- client
}