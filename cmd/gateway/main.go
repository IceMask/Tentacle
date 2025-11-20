package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/mcp/mobile-worker/internal/gateway/jsonrpc"
	"github.com/mcp/mobile-worker/internal/gateway/rest"
	"github.com/mcp/mobile-worker/internal/gateway/websocket"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	router := gin.Default()

	// Initialize components
	wsHub := websocket.NewHub()
	go wsHub.Run()

	rpcHandler := jsonrpc.NewHandler(wsHub)
	restHandler := rest.NewHandler()

	// Routes
	router.POST("/jsonrpc", rpcHandler.Handle)
	router.POST("/sessions", restHandler.CreateSession)
	router.GET("/capabilities", restHandler.GetCapabilities)
	router.GET("/ws/plan-events", func(c *gin.Context) {
		websocket.ServeWs(wsHub, c.Writer, c.Request)
	})

	// Health check
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	log.Printf("Gateway starting on port %s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
