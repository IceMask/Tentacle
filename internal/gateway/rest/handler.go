package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) CreateSession(c *gin.Context) {
	// Placeholder for session creation logic
	c.JSON(http.StatusCreated, gin.H{"sessionId": "mock-session-id"})
}

func (h *Handler) GetCapabilities(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"capabilities": map[string]interface{}{
			"platformName": "Android",
			"automationName": "UiAutomator2",
		},
	})
}
