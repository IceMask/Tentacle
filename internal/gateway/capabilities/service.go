package capabilities

import (
	"context"
	"mcp_for_appium/internal/config"
)

type Service struct {
	cfg config.GatewayConfig
}

func NewService(cfg config.GatewayConfig) *Service {
	return &Service{cfg: cfg}
}

func (s *Service) List(ctx context.Context) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "4.2.0",
		"capabilities": map[string]interface{}{
			"planOps": []string{"startSession", "executePlan", "getSemanticSnapshot"},
			"features": map[string]string{
				"jsonrpc": "stable",
				"a2a":     "beta",
			},
		},
	}
}
