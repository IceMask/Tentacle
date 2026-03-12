package websocket

import (
	"net/http"

	"github.com/gorilla/websocket"
)

type Handler struct {
	hub      *Hub
	upgrader websocket.Upgrader
}

// NewHandler executes this operation.
func NewHandler(hub *Hub) *Handler {
	return &Handler{
		hub: hub,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// ServeHTTP executes this operation.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	traceID := r.URL.Query().Get("traceId")
	if traceID == "" {
		http.Error(w, "traceId required", http.StatusBadRequest)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	client := h.hub.Subscribe(r.Context(), traceID, conn.RemoteAddr().String())
	defer h.hub.Unsubscribe(client)

	done := make(chan struct{})
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				close(done)
				return
			}
		}
	}()

	for {
		select {
		case msg, ok := <-client.send:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}
