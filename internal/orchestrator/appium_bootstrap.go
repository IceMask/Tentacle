// appium_bootstrap.go centralizes orchestrator-side Appium readiness checks and managed local Appium shutdown behavior.
package orchestrator

import (
	"context"
	"strings"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/startup"
)

// ensureAppiumReady verifies that the orchestrator's configured Appium endpoint is reachable before one new Appium session is created.
func (s *Service) ensureAppiumReady(ctx context.Context) error {
	if s == nil { // Reject nil services because no Appium readiness work can be performed without one concrete orchestrator instance.
		return errors.New(errors.CodeConfigInvalid, "orchestrator service is nil") // Surface nil-service misuse as a stable config error instead of panicking on the receiver.
	}
	if strings.TrimSpace(s.appiumURL) == "" { // Reject missing Appium URLs because neither remote reachability checks nor local auto-start can run without one concrete endpoint.
		return errors.New(errors.CodeConfigMissing, "appium_url is empty") // Surface the missing Appium URL explicitly so callers see the real configuration problem.
	}
	initialErr := startup.CheckAppiumReachability(ctx, s.appiumURL) // Probe the configured Appium endpoint once so missing local servers can be auto-started while existing unhealthy endpoints keep their original session-start semantics.
	if initialErr == nil {                                          // Stop early when the configured Appium endpoint is already reachable before any StartSession call begins.
		return nil // Return success immediately because Appium is already healthy.
	}
	if !startup.IsLocalAppiumMissingServerError(initialErr) { // Defer non-missing-server failures to the real Appium new-session request so callers still see Appium timeout and protocol errors directly.
		return nil // Return success here because StartSession should continue into the real Appium request path for existing but unhealthy endpoints.
	}
	if s.appiumRuntime != nil { // Reuse the service-owned Appium supervisor when one exists so monolith StartSession can auto-start one local loopback Appium dependency on demand.
		return s.appiumRuntime.EnsureReady(ctx) // Delegate readiness and optional local auto-start to the shared supervisor so concurrent session creation stays serialized.
	}

	return initialErr // Fall back to the original missing-server reachability error when no local Appium supervisor has been constructed.
}

// stopManagedAppium stops the optional service-owned local Appium supervisor during orchestrator shutdown.
func (s *Service) stopManagedAppium() error {
	if s == nil || s.appiumRuntime == nil { // Treat missing services and missing supervisors as already stopped because there is no owned child process to tear down.
		return nil // Return success because no managed local Appium lifecycle exists to stop.
	}

	return s.appiumRuntime.Stop() // Delegate shutdown to the shared supervisor so any owned local Appium child process exits before the service returns.
}
