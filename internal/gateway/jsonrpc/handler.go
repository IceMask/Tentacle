package jsonrpc

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mcp/mobile-worker/internal/gateway/websocket"
)

type Handler struct {
	wsHub *websocket.Hub
}

func NewHandler(wsHub *websocket.Hub) *Handler {
	return &Handler{wsHub: wsHub}
}

type Request struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      interface{} `json:"id"`
}

type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (h *Handler) Handle(c *gin.Context) {
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, Response{
			JSONRPC: "2.0",
			Error:   &Error{Code: -32700, Message: "Parse error"},
			ID:      nil,
		})
		return
	}

	var res Response
	res.JSONRPC = "2.0"
	res.ID = req.ID

	switch req.Method {
	case "healthCheck":
		res.Result = map[string]string{"status": "healthy"}
	default:
		res.Error = &Error{Code: -32601, Message: "Method not found"}
	}

	c.JSON(http.StatusOK, res)
}
