// devicefarm_runtime.go wires Device Farm remote-access sessions into the orchestrator session lifecycle so MCP actions can target live AWS-hosted devices through Appium.
package orchestrator

import (
	"context"
	stdErrors "errors"
	"strings"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/devicefarm"
	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/storage/postgres"
	"mcp_for_appium/internal/worker/appium"

	goredis "github.com/redis/go-redis/v9"
)

// deviceFarmRuntimeClient captures the Device Farm operations that the orchestrator actually needs across run_api and remote-access flows.
type deviceFarmRuntimeClient interface {
	ScheduleRun(ctx context.Context, req devicefarm.ScheduleRunRequest) (map[string]interface{}, error)                             // ScheduleRun keeps the existing run_api flow available through the abstracted client contract.
	CreateUpload(ctx context.Context, req devicefarm.CreateUploadRequest) (map[string]interface{}, error)                           // CreateUpload keeps the existing run_api upload flow available through the abstracted client contract.
	GetUpload(ctx context.Context, uploadARN string) (map[string]interface{}, error)                                                // GetUpload keeps the existing run_api upload-status flow available through the abstracted client contract.
	GetRun(ctx context.Context, runARN string) (map[string]interface{}, error)                                                      // GetRun keeps the existing run_api run-status flow available through the abstracted client contract.
	CreateRemoteAccessSession(ctx context.Context, req devicefarm.CreateRemoteAccessSessionRequest) (map[string]interface{}, error) // CreateRemoteAccessSession provisions one live Device Farm remote-access Appium endpoint for interactive MCP sessions.
	GetRemoteAccessSession(ctx context.Context, remoteAccessSessionARN string) (map[string]interface{}, error)                      // GetRemoteAccessSession reads back one remote-access session so readiness and diagnostics can be surfaced consistently.
	StopRemoteAccessSession(ctx context.Context, remoteAccessSessionARN string) error                                               // StopRemoteAccessSession releases one live Device Farm remote-access session when the platform session ends or setup fails.
}

// startSessionTarget describes the concrete Appium endpoint and cleanup metadata that one new platform session should use.
type startSessionTarget struct {
	appiumURL              string                 // appiumURL is the concrete Appium base URL that the new platform session should connect to.
	caps                   map[string]interface{} // caps is the sanitized Appium capability map after repository-only Device Farm keys have been stripped.
	remoteAccessSessionARN string                 // remoteAccessSessionARN is populated only when the session is backed by one Device Farm remote-access reservation.
}

// normalizeDeviceFarmMode canonicalizes the configured Device Farm mode while treating the legacy test_grid selector as a compatibility alias for remote_access.
func normalizeDeviceFarmMode(rawMode string) string {
	mode := strings.ToLower(strings.TrimSpace(rawMode)) // Normalize the incoming mode string once so later runtime branches stay deterministic and case-insensitive.
	if mode == "test_grid" {                            // Translate the historical selector into the real remote-access runtime mode so the repository stops advertising the wrong AWS primitive.
		return "remote_access" // Return the canonical remote-access selector so later runtime checks do not need to special-case the legacy spelling again.
	}
	return mode // Return the normalized mode unchanged when it is already one canonical selector such as disabled, run_api, or remote_access.
}

// resolvedDeviceFarmProjectARN selects the best available default Device Farm project ARN, preferring the generic project_arn field over the historical test_grid_project_arn fallback.
func resolvedDeviceFarmProjectARN(cfg config.DeviceFarmConfig) string {
	projectARN := strings.TrimSpace(cfg.ProjectARN) // Prefer the generic Device Farm project ARN because both run_api and remote_access now use this field semantically.
	if projectARN != "" {                           // Stop once the canonical project ARN field already provides one concrete value.
		return projectARN // Return the canonical project ARN immediately so callers never need to inspect the legacy fallback field.
	}
	return strings.TrimSpace(cfg.TestGridProjectARN) // Fall back to the historical field only for compatibility with existing configurations that still populate it.
}

// resolveStartSessionTarget determines whether the new platform session should use the local Appium endpoint or one freshly provisioned Device Farm remote-access endpoint.
func (s *Service) resolveStartSessionTarget(ctx context.Context, projectID string, caps map[string]interface{}) (*startSessionTarget, error) {
	if s.dfMode != "remote_access" { // Keep the historical local-Appium behavior whenever Device Farm remote access is not the configured runtime mode.
		return &startSessionTarget{appiumURL: s.appiumURL, caps: cloneCapabilities(caps)}, nil // Return the local Appium endpoint together with one shallow capability copy so later repository normalization never mutates the caller payload.
	}

	sanitizedCaps, request, err := extractDeviceFarmRemoteAccessRequest(caps, s.dfProjectARN, projectID) // Parse and strip repository-only Device Farm keys before any Appium or AWS request is attempted.
	if err != nil {                                                                                      // Stop immediately when the custom capability payload is malformed or incomplete for remote-access mode.
		return nil, err // Preserve the precise validation error produced by the capability parser.
	}
	if s.deviceFarm == nil { // Reject the remote-access flow when the Device Farm client never initialized successfully.
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm client not initialized") // Surface the missing Device Farm client before Appium session setup starts.
	}
	if strings.TrimSpace(request.DeviceARN) == "" { // Require one concrete Device Farm device ARN in remote-access mode because the AWS session cannot be provisioned without a target device.
		return nil, errors.New(errors.CodePlanInvalid, "devicefarm:deviceArn is required when devicefarm.mode=remote_access") // Surface the missing device ARN through one stable validation error.
	}

	remoteAccessSession, err := s.deviceFarm.CreateRemoteAccessSession(ctx, request) // Provision one Device Farm remote-access reservation and wait for its Appium endpoint before starting the Appium session.
	if err != nil {                                                                  // Stop immediately when AWS cannot provision the remote-access session or expose the Appium endpoint in time.
		return nil, err // Preserve the wrapped Device Farm provisioning error for the caller.
	}

	appiumURL, _ := remoteAccessSession["remoteDriverEndpoint"].(string) // Read the ready Appium endpoint from the normalized remote-access payload returned by the Device Farm client.
	if strings.TrimSpace(appiumURL) == "" {                              // Reject empty endpoint payloads because Appium session creation cannot proceed without one concrete base URL.
		return nil, errors.New(errors.CodeInternal, "device farm remote access session returned empty remoteDriverEndpoint") // Surface the missing endpoint through one stable internal error.
	}
	remoteAccessSessionARN, _ := remoteAccessSession["sessionArn"].(string) // Read the created remote-access session ARN so later cleanup can stop the reservation promptly.

	return &startSessionTarget{appiumURL: appiumURL, caps: sanitizedCaps, remoteAccessSessionARN: strings.TrimSpace(remoteAccessSessionARN)}, nil // Return the resolved remote Appium endpoint together with the sanitized capabilities and cleanup metadata.
}

// cloneCapabilities returns one shallow copy of the Appium capability map so repository helpers can normalize or inspect it without mutating the caller's original payload.
func cloneCapabilities(caps map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(caps)) // Allocate the destination map with the same capacity so the shallow clone remains efficient for typical capability payload sizes.
	for key, value := range caps {                    // Copy every top-level capability key and value once because the repository only needs one shallow clone of the input map.
		cloned[key] = value // Preserve each original capability value so local session setup keeps the exact caller payload semantics.
	}
	return cloned // Return the shallow-cloned capability map for safe repository-side normalization.
}

// extractDeviceFarmRemoteAccessRequest strips repository-only Device Farm keys from one Appium capability map and turns them into one remote-access provisioning request.
func extractDeviceFarmRemoteAccessRequest(caps map[string]interface{}, fallbackProjectARN string, projectID string) (map[string]interface{}, devicefarm.CreateRemoteAccessSessionRequest, error) {
	sanitizedCaps := make(map[string]interface{}, len(caps)) // Allocate the destination capability map so repository-only Device Farm keys can be removed before Appium sees them.
	request := devicefarm.CreateRemoteAccessSessionRequest{  // Start one zero-value remote-access request that will be populated from custom capabilities and fallbacks.
		ProjectARN: strings.TrimSpace(fallbackProjectARN), // Seed the request with the configuration default project ARN so callers only need to override it when targeting another project.
		Name:       strings.TrimSpace(projectID),          // Seed the remote-access session name with the repository project identifier so AWS console entries remain recognizable by default.
	}

	for key, value := range caps { // Walk every incoming capability so repository-only keys can be split from the actual Appium capability payload.
		normalizedKey := strings.TrimSpace(key) // Normalize the capability key once so the repository-only key matcher stays exact and whitespace-tolerant.
		switch normalizedKey {                  // Route repository-only Device Farm keys into the remote-access request while preserving all other Appium capabilities unchanged.
		case "devicefarm:projectArn": // Allow callers to override the default Device Farm project ARN on a per-session basis through the capability payload.
			request.ProjectARN = deviceFarmStringValue(value) // Copy the caller-supplied project ARN into the remote-access request once it is normalized to one string value.
		case "devicefarm:deviceArn": // Require callers to supply the concrete Device Farm device ARN that should host the interactive Appium session.
			request.DeviceARN = deviceFarmStringValue(value) // Copy the caller-supplied device ARN into the remote-access request once it is normalized to one string value.
		case "devicefarm:appArn": // Allow callers to request preinstallation of one uploaded application package before Appium connects.
			request.AppARN = deviceFarmStringValue(value) // Copy the caller-supplied app upload ARN into the remote-access request once it is normalized to one string value.
		case "devicefarm:sessionName": // Allow callers to override the default remote-access session display name shown in the Device Farm console.
			request.Name = deviceFarmStringValue(value) // Copy the caller-supplied session name into the remote-access request once it is normalized to one string value.
		default: // Preserve every non-repository capability so Appium still receives the intended W3C and appium:* capability payload.
			sanitizedCaps[normalizedKey] = value // Copy the capability through unchanged because Appium still needs to see this key and value.
		}
	}

	if request.AppARN == "" { // Infer the Device Farm application upload ARN from the conventional app capability only when the caller did not supply the explicit repository key.
		if appValue, ok := sanitizedCaps["app"]; ok { // Inspect the preserved app capability because some callers may already pass one Device Farm upload ARN here.
			appARN := deviceFarmStringValue(appValue)             // Normalize the conventional app capability to one string so ARN inference can be checked consistently.
			if strings.HasPrefix(appARN, "arn:aws:devicefarm:") { // Treat Device Farm upload ARNs specially because Appium itself cannot consume an ARN as a filesystem path or bundle identifier.
				request.AppARN = appARN      // Reuse the inferred upload ARN as the application package to install into the remote-access session before Appium connects.
				delete(sanitizedCaps, "app") // Remove the ARN-valued app capability so the later Appium new-session request does not send an invalid filesystem path or bundle identifier.
			}
		}
	}

	if strings.TrimSpace(request.ProjectARN) == "" { // Reject missing project ARNs once all explicit overrides and configuration fallbacks have been applied.
		return nil, devicefarm.CreateRemoteAccessSessionRequest{}, errors.New(errors.CodeConfigMissing, "devicefarm project arn is required for remote access sessions") // Surface the missing project ARN through one stable config error.
	}

	return sanitizedCaps, request, nil // Return the sanitized Appium capability payload together with the normalized remote-access provisioning request.
}

// deviceFarmStringValue normalizes one repository-only Device Farm capability value into one trimmed string representation.
func deviceFarmStringValue(value interface{}) string {
	if asString, ok := value.(string); ok { // Prefer the exact incoming string when the capability payload already provides one string value.
		return strings.TrimSpace(asString) // Return the trimmed capability string so later request-shaping logic never sees leading or trailing whitespace.
	}
	return "" // Fall back to the empty string for every non-string capability value because repository-only Device Farm keys require one explicit string payload.
}

// restoreAppiumClient rebuilds one Appium client for the supplied platform session from persisted Redis mappings when the in-memory client cache is empty.
func (s *Service) restoreAppiumClient(ctx context.Context, sessionID string) (*appium.Client, error) {
	if s.cache == nil { // Reject restore attempts when Redis-backed session mappings are unavailable because no persisted endpoint or session id can be recovered safely.
		return nil, errors.New(errors.CodeStoreRead, "appium session cache is unavailable") // Surface the missing cache dependency through one stable storage-read error.
	}

	appiumSessionID, err := s.cache.Get(ctx, appiumSessionKeyPrefix+sessionID) // Load the persisted Appium session id mapping needed to reattach one client after process restart or cache eviction.
	if err != nil {                                                            // Distinguish missing Redis keys from other cache failures before the caller receives one error.
		if err.Error() == "redis: nil" { // Translate missing mappings into the repository's stable session-not-found contract instead of leaking Redis-specific wording.
			return nil, errors.New(errors.CodeSessionNotFound, "session not found") // Surface the missing session mapping through the standard session-not-found error.
		}
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to load appium session mapping", err) // Surface Redis lookup failures through the stable storage-read error contract.
	}
	if strings.TrimSpace(appiumSessionID) == "" { // Reject empty mapping values because they cannot identify any live Appium session to restore.
		return nil, errors.New(errors.CodeSessionNotFound, "session not found") // Surface empty mappings through the same stable session-not-found contract.
	}

	appiumURL := s.appiumURL                                                                                                       // Start from the configured default Appium base URL so legacy locally managed sessions restore exactly as before when no per-session override exists.
	if cachedURL, err := s.cache.Get(ctx, appiumSessionURLKeyPrefix+sessionID); err == nil && strings.TrimSpace(cachedURL) != "" { // Prefer one persisted per-session Appium URL when the session was created against a remote Device Farm endpoint.
		appiumURL = strings.TrimSpace(cachedURL) // Reattach to the exact original Appium endpoint so restored clients keep talking to the same live automation session.
	}

	restored := appium.NewClient(appiumURL) // Construct one Appium client bound to the resolved per-session Appium endpoint.
	restored.AttachSession(appiumSessionID) // Reattach the client to the persisted Appium session id so later calls continue the existing automation session.
	return restored, nil                    // Return the restored client so callers can cache or use it immediately.
}

// persistSessionAutomationState stores the session-specific Appium endpoint and optional Device Farm remote-access reservation ARN needed for later session reuse and cleanup.
func (s *Service) persistSessionAutomationState(ctx context.Context, platformSessionID string, appiumSessionID string, target *startSessionTarget) error {
	if err := s.cache.Set(ctx, appiumSessionKeyPrefix+platformSessionID, appiumSessionID, appiumSessionKeyTTL); err != nil { // Persist the platform-to-Appium session id mapping so later calls can reattach to the live automation session.
		return err // Preserve the cache write failure so callers can log and degrade gracefully without silently losing the required mapping.
	}
	if err := s.cache.Set(ctx, appiumSessionURLKeyPrefix+platformSessionID, target.appiumURL, appiumSessionKeyTTL); err != nil { // Persist the concrete Appium base URL so restored clients reconnect to the same local or remote endpoint after restart.
		return err // Preserve the cache write failure so callers can log and degrade gracefully without silently losing the endpoint mapping.
	}
	if strings.TrimSpace(target.remoteAccessSessionARN) != "" { // Persist the Device Farm remote-access reservation ARN only for sessions that are actually backed by one AWS remote-access slot.
		if err := s.cache.Set(ctx, deviceFarmRemoteKeyPrefix+platformSessionID, target.remoteAccessSessionARN, appiumSessionKeyTTL); err != nil { // Persist the reservation ARN so EndSession can stop the Device Farm slot even after process restart.
			return err // Preserve the cache write failure so callers can log and degrade gracefully without silently losing the cleanup handle.
		}
	}
	return nil // Return success once every session-specific automation mapping has been persisted.
}

// cleanupFailedStartSession stops one Device Farm remote-access reservation that was provisioned successfully but never reached a persisted platform-session state.
func (s *Service) cleanupFailedStartSession(ctx context.Context, target *startSessionTarget) {
	if target == nil { // Skip cleanup entirely when no target metadata was produced before failure.
		return // Return immediately because there is nothing to release.
	}
	if s.deviceFarm == nil { // Skip cleanup when the Device Farm client is unavailable because only local Appium was used or initialization already failed.
		return // Return immediately because no remote-access reservation can be stopped without a live Device Farm client.
	}
	if strings.TrimSpace(target.remoteAccessSessionARN) == "" { // Skip cleanup for local Appium targets because only remote-access sessions allocate AWS device slots.
		return // Return immediately because no Device Farm reservation was created for this target.
	}
	stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)                                          // Bound cleanup time so failed session setup does not hang indefinitely while trying to release the AWS reservation.
	defer cancel()                                                                                       // Ensure cleanup timeout resources are always released before the helper returns.
	if err := s.deviceFarm.StopRemoteAccessSession(stopCtx, target.remoteAccessSessionARN); err != nil { // Ask AWS to release the remote-access slot that was created before Appium session setup failed.
		s.logger.WarnContext(ctx, "failed to stop device farm remote access session after startSession error", "remote_access_session_arn", target.remoteAccessSessionARN, "error", err) // Surface best-effort cleanup failures for later operator diagnosis.
	}
}

// releaseSessionAutomation closes one Appium session, stops its Device Farm reservation, and deletes persisted handles only after external cleanup succeeds.
func (s *Service) releaseSessionAutomation(ctx context.Context, sessionID string, client *appium.Client) error {
	if client == nil { // Restore the Appium handle only when no live in-memory client survived until cleanup.
		restoredClient, restoreErr := s.restoreAppiumClient(ctx, sessionID)              // Rebuild the handle from Redis so cleanup remains restart-safe.
		if restoreErr != nil && !errors.IsCode(restoreErr, errors.CodeSessionNotFound) { // Treat missing mappings as an already-cleaned retry while preserving real cache failures.
			return restoreErr // Keep the session ending and its remaining handles intact on Redis read failures.
		}
		client = restoredClient // Use the restored client when available, or nil when a previous cleanup already removed the mapping.
	}
	if client != nil { // Close the live Appium session before deleting the only persisted reattachment handle.
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)                                           // Bound Appium cleanup while allowing it to finish after caller cancellation.
		deleteErr := client.DeleteSession(cleanupContext)                                                                                   // Ask Appium to release the concrete automation session exactly once per retry attempt.
		cancel()                                                                                                                            // Release the Appium cleanup timer immediately after the request returns.
		if deleteErr != nil && !errors.IsCode(deleteErr, errors.CodeSessionDead) && !errors.IsCode(deleteErr, errors.CodeSessionNotFound) { // Treat an already-dead backend session as successful idempotent cleanup.
			return errors.WrapPreservingCode("failed to delete appium session", deleteErr) // Keep all Redis handles available for a later retry when Appium cleanup is ambiguous or failed.
		}
	}

	if s.cache == nil { // Refuse to report cleanup success when persisted handles cannot be inspected or deleted.
		return errors.New(errors.CodeStoreRead, "session automation cache is unavailable") // Leave the session ending so cleanup can retry after Redis recovery.
	}
	remoteAccessSessionARN, remoteLookupErr := s.cache.Get(ctx, deviceFarmRemoteKeyPrefix+sessionID) // Read the Device Farm handle before deleting any persisted session mapping.
	if remoteLookupErr != nil && !stdErrors.Is(remoteLookupErr, goredis.Nil) {                       // Ignore a missing handle because local sessions and completed retries legitimately have none.
		return errors.Wrap(errors.CodeStoreRead, "failed to load device farm cleanup handle", remoteLookupErr) // Preserve retryability when Redis cannot determine whether a reservation exists.
	}
	if strings.TrimSpace(remoteAccessSessionARN) != "" { // Stop the persisted AWS reservation before deleting its only durable handle.
		if s.deviceFarm == nil { // Refuse to discard the reservation handle when no runtime client can release it.
			return errors.New(errors.CodeConfigMissing, "device farm client is unavailable for session cleanup") // Keep the session ending until the dependency recovers.
		}
		stopContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)                  // Bound AWS cleanup while allowing it to finish after caller cancellation.
		stopErr := s.deviceFarm.StopRemoteAccessSession(stopContext, strings.TrimSpace(remoteAccessSessionARN)) // Release the exact persisted Device Farm reservation.
		cancel()                                                                                                // Release the Device Farm cleanup timer immediately after the request returns.
		if stopErr != nil {                                                                                     // Keep the ARN and all other mappings when AWS does not confirm cleanup.
			return errors.WrapPreservingCode("failed to stop device farm remote access session", stopErr) // Preserve the provider failure for a later retry.
		}
	}

	var deletionErrors []error                                                 // Collect Redis deletion failures after external resources are confirmed closed so partial key deletion remains retryable.
	if err := s.cache.Del(ctx, appiumSessionKeyPrefix+sessionID); err != nil { // Delete the Appium session id only after Appium and AWS cleanup succeed.
		deletionErrors = append(deletionErrors, errors.Wrap(errors.CodeStoreWrite, "failed to delete appium session mapping", err)) // Preserve the failed key deletion for caller retry.
	}
	if err := s.cache.Del(ctx, appiumSessionURLKeyPrefix+sessionID); err != nil { // Delete the endpoint mapping after no future reattachment is required.
		deletionErrors = append(deletionErrors, errors.Wrap(errors.CodeStoreWrite, "failed to delete appium session url mapping", err)) // Preserve the failed key deletion for caller retry.
	}
	if err := s.cache.Del(ctx, deviceFarmRemoteKeyPrefix+sessionID); err != nil { // Delete the AWS handle only after the provider confirmed reservation cleanup.
		deletionErrors = append(deletionErrors, errors.Wrap(errors.CodeStoreWrite, "failed to delete device farm remote access mapping", err)) // Preserve the failed key deletion for caller retry.
	}
	return stdErrors.Join(deletionErrors...) // Return nil when every handle was deleted or a joined retryable storage error otherwise.
}

// cachedSessionAppiumURL returns the persisted per-session Appium endpoint, falling back to the configured local default when no session-specific mapping exists.
func (s *Service) cachedSessionAppiumURL(ctx context.Context, sessionID string) string {
	if s.cache != nil { // Attempt to read the per-session Appium endpoint mapping only when Redis-backed session state is available.
		if cachedURL, err := s.cache.Get(ctx, appiumSessionURLKeyPrefix+sessionID); err == nil && strings.TrimSpace(cachedURL) != "" { // Prefer the cached per-session endpoint when it exists and is non-empty.
			return strings.TrimSpace(cachedURL) // Return the exact session-specific Appium endpoint so restored clients reconnect to the same backend.
		}
	}
	return s.appiumURL // Fall back to the configured default Appium endpoint so legacy local sessions continue restoring exactly as before.
}

// sessionUsesDeviceFarmRemoteAccess reports whether one persisted platform session is backed by one Device Farm remote-access reservation.
func (s *Service) sessionUsesDeviceFarmRemoteAccess(ctx context.Context, session *postgres.Session) bool {
	if session == nil || s.cache == nil { // Reject nil sessions or missing cache wiring because the remote-access mapping lookup cannot run safely without both.
		return false // Return false so callers keep the local-Appium path when the session metadata or cache is unavailable.
	}
	remoteAccessSessionARN, err := s.cache.Get(ctx, deviceFarmRemoteKeyPrefix+session.ID) // Look up the persisted remote-access reservation ARN for the supplied platform session.
	if err != nil {                                                                       // Treat missing or failing cache lookups as non-remote-access so callers degrade to local/default behavior safely.
		return false // Return false because no confirmed remote-access reservation mapping is available for this session.
	}
	return strings.TrimSpace(remoteAccessSessionARN) != "" // Return true only when a non-empty remote-access reservation ARN is still persisted for the session.
}
