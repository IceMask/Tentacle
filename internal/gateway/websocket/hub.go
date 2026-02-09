package websocket

import (
	"context"
	"log/slog"
	"sync"

	"mcp_for_appium/internal/storage/redis"
	"mcp_for_appium/internal/telemetry"
)

const (
	// Queue size per client (drop-oldest when full)
	clientQueueSize = 256
	// Alert threshold for consecutive drops
	dropAlertThreshold = 3
	// Events channel prefix
	eventsChannelPrefix = "events:"
)

type Hub struct {
	cache         *redis.Cache
	subscriptions map[string]*subscription // traceID -> subscription
	clients       map[*Client]bool
	register      chan *Client
	unregister    chan *Client
	mu            sync.RWMutex
	logger        *slog.Logger
	stopCh        chan struct{}
}

type subscription struct {
	traceID string
	clients map[*Client]bool
	done    chan struct{} // closed when last client unsubscribes; signals listenToTrace to exit
}

type Client struct {
	hub       *Hub
	traceID   string
	send      chan []byte
	drops     int
	channelID string
}

func NewHub(cache *redis.Cache) *Hub {
	return &Hub{
		cache:         cache,
		subscriptions: make(map[string]*subscription),
		clients:       make(map[*Client]bool),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		logger:        telemetry.Logger(),
		stopCh:        make(chan struct{}),
	}
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.stopCh:
			return
		case client := <-h.register:
			h.registerClient(ctx, client)
		case client := <-h.unregister:
			h.unregisterClient(client)
		}
	}
}

func (h *Hub) Stop() {
	close(h.stopCh)
	h.logger.Info("websocket hub stopped")
}

func (h *Hub) registerClient(ctx context.Context, client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[client] = true

	// Get or create subscription for this traceID
	sub, exists := h.subscriptions[client.traceID]
	if !exists {
		// Create new subscription and start listening to Redis PubSub
		sub = &subscription{
			traceID: client.traceID,
			clients: make(map[*Client]bool),
			done:    make(chan struct{}),
		}
		h.subscriptions[client.traceID] = sub

		// Start Redis PubSub listener for this trace
		go h.listenToTrace(ctx, client.traceID, sub.done)

		h.logger.InfoContext(ctx, "new trace subscription created",
			"trace_id", client.traceID)
	}

	sub.clients[client] = true

	h.logger.InfoContext(ctx, "websocket client registered",
		"trace_id", client.traceID,
		"channel_id", client.channelID,
		"total_clients", len(sub.clients))
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)

		// Remove from subscription
		if sub, exists := h.subscriptions[client.traceID]; exists {
			delete(sub.clients, client)

			// If no more clients for this trace, signal PubSub listener to exit and cleanup
			if len(sub.clients) == 0 {
				close(sub.done)
				delete(h.subscriptions, client.traceID)
				h.logger.Info("trace subscription removed",
					"trace_id", client.traceID)
			}
		}

		h.logger.Info("websocket client unregistered",
			"trace_id", client.traceID,
			"channel_id", client.channelID)
	}
}

// listenToTrace subscribes to Redis PubSub for a specific trace and forwards events.
// It exits when ctx is cancelled, the hub stops, or done is closed (last client left).
func (h *Hub) listenToTrace(ctx context.Context, traceID string, done <-chan struct{}) {
	channel := eventsChannelPrefix + traceID
	pubsub := h.cache.Subscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	h.logger.Info("listening to trace events", "trace_id", traceID, "channel", channel)

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.stopCh:
			return
		case <-done:
			return
		case msg, ok := <-ch:
			if !ok {
				h.logger.Warn("pubsub channel closed", "trace_id", traceID)
				return
			}

			// Broadcast to all clients subscribed to this trace
			h.broadcastToTrace(traceID, []byte(msg.Payload))
		}
	}
}

// broadcastToTrace sends a message to all clients subscribed to a trace
func (h *Hub) broadcastToTrace(traceID string, message []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	sub, exists := h.subscriptions[traceID]
	if !exists {
		return
	}

	for client := range sub.clients {
		telemetry.WSQueueLen.Observe(float64(len(client.send)))
		// Try to send with drop-oldest strategy
		select {
		case client.send <- message:
			// Sent successfully
			client.drops = 0
		default:
			// Queue full, drop oldest and try again
			select {
			case <-client.send: // Drop oldest
			default:
			}

			client.drops++
			if client.drops >= dropAlertThreshold {
				h.logger.Warn("slow websocket client",
					"trace_id", traceID,
					"channel_id", client.channelID,
					"consecutive_drops", client.drops)
				telemetry.WebsocketDrops.WithLabelValues("", traceID).Inc()
			}

			// Try to send again
			select {
			case client.send <- message:
			default:
				// Still full, disconnect slow client
				h.logger.Error("disconnecting slow client",
					"trace_id", traceID,
					"channel_id", client.channelID)
				h.unregister <- client
			}
		}
	}
}

// Subscribe subscribes a client to a trace
func (h *Hub) Subscribe(ctx context.Context, traceID, channelID string) *Client {
	client := &Client{
		hub:       h,
		traceID:   traceID,
		channelID: channelID,
		send:      make(chan []byte, clientQueueSize),
	}
	h.register <- client
	return client
}

// Unsubscribe unsubscribes a client
func (h *Hub) Unsubscribe(client *Client) {
	h.unregister <- client
}
