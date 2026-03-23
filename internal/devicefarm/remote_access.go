// remote_access.go wraps the AWS Device Farm remote-access-session APIs so the repository can create live Appium endpoints for interactive MCP actions.
package devicefarm

import (
	"context"
	"strings"
	"time"

	"mcp_for_appium/internal/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdevicefarm "github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

const (
	defaultRemoteAccessPollInterval = 2 * time.Second // defaultRemoteAccessPollInterval defines how often the repository polls AWS for one remote-access session endpoint.
	defaultRemoteAccessWaitTimeout  = 2 * time.Minute // defaultRemoteAccessWaitTimeout bounds remote-access session provisioning so StartSession does not wait forever for Device Farm.
)

// CreateRemoteAccessSessionRequest defines the input for creating one Device Farm remote-access session whose remoteDriverEndpoint can back Appium actions.
type CreateRemoteAccessSessionRequest struct {
	ProjectARN string // ProjectARN selects the Device Farm project that owns the remote-access session.
	DeviceARN  string // DeviceARN identifies the physical device that should host the interactive Appium session.
	AppARN     string // AppARN optionally preinstalls one uploaded application on the remote-access device before Appium connects.
	Name       string // Name optionally overrides the generated remote-access session display name.
}

// CreateRemoteAccessSession creates one remote-access session and waits until AWS returns one WebDriver-compatible remoteDriverEndpoint.
func (c *Client) CreateRemoteAccessSession(ctx context.Context, req CreateRemoteAccessSessionRequest) (map[string]interface{}, error) {
	projectARN := strings.TrimSpace(req.ProjectARN) // Prefer the request-scoped project ARN so one client can target multiple projects safely.
	if projectARN == "" {                           // Fall back to the configuration default only when the caller omitted the project ARN explicitly.
		projectARN = strings.TrimSpace(c.cfg.ProjectARN) // Reuse the configured Device Farm project as the default remote-access target.
	}
	if projectARN == "" { // Reject missing project ARNs before any AWS call because remote-access creation cannot succeed without one owning project.
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm project arn is required") // Surface the missing project ARN as one stable config error for callers.
	}

	deviceARN := strings.TrimSpace(req.DeviceARN) // Normalize the target device ARN once so later AWS requests and responses use the same exact string.
	if deviceARN == "" {                          // Reject missing device ARNs before any AWS call because remote-access creation cannot succeed without a concrete device.
		return nil, errors.New(errors.CodePlanInvalid, "deviceArn is required") // Surface the missing device ARN as one stable plan-validation error for callers.
	}

	sessionName := strings.TrimSpace(req.Name) // Prefer the caller-supplied remote-access session name when one is provided.
	if sessionName == "" {                     // Fall back to one deterministic timestamped name when the caller omits an explicit session name.
		sessionName = "mcp-remote-" + time.Now().UTC().Format("20060102-150405") // Generate one readable UTC timestamped name for later Device Farm console diagnosis.
	}

	input := &awsdevicefarm.CreateRemoteAccessSessionInput{ // Build the AWS request with the normalized project, device, and session-name values.
		ProjectArn: aws.String(projectARN),  // Bind the owning project ARN required by the Device Farm remote-access API.
		DeviceArn:  aws.String(deviceARN),   // Bind the physical device ARN required by the Device Farm remote-access API.
		Name:       aws.String(sessionName), // Bind the operator-visible remote-access session name for later console diagnosis.
	}
	if strings.TrimSpace(req.AppARN) != "" { // Forward the optional uploaded application ARN only when the caller provided one explicitly.
		input.Configuration = &types.CreateRemoteAccessSessionConfiguration{
			AuxiliaryApps: []string{strings.TrimSpace(req.AppARN)}, // Ask Device Farm to install the requested application package before Appium connects.
		}
	}

	created, err := c.sdk.CreateRemoteAccessSession(ctx, input) // Create the remote-access session through the AWS SDK before polling for one ready Appium endpoint.
	if err != nil {                                             // Stop immediately when AWS rejects or cannot create the remote-access session request.
		return nil, errors.Wrap(errors.CodeInternal, "failed to create device farm remote access session", err) // Surface the AWS failure through one stable internal error contract.
	}
	if created.RemoteAccessSession == nil { // Reject empty AWS payloads because callers need the remote-access session ARN for later cleanup and polling.
		return nil, errors.New(errors.CodeInternal, "device farm returned empty remote access session payload") // Surface empty payloads as one stable internal error.
	}

	sessionARN := strings.TrimSpace(aws.ToString(created.RemoteAccessSession.Arn)) // Normalize the created remote-access session ARN before it is echoed or polled.
	if sessionARN == "" {                                                          // Reject missing session ARNs because later polling and cleanup both depend on this identifier.
		return nil, errors.New(errors.CodeInternal, "device farm remote access session arn is missing") // Surface missing ARNs as one stable internal error.
	}

	return c.waitForRemoteDriverEndpoint(ctx, projectARN, deviceARN, sessionName, sessionARN) // Poll the created session until AWS exposes one Appium-compatible remoteDriverEndpoint or the wait fails.
}

// GetRemoteAccessSession fetches one live or completed Device Farm remote-access session and normalizes its interactive and Appium endpoints.
func (c *Client) GetRemoteAccessSession(ctx context.Context, remoteAccessSessionARN string) (map[string]interface{}, error) {
	sessionARN := strings.TrimSpace(remoteAccessSessionARN) // Normalize the target remote-access session ARN once so later AWS requests and response mapping use the same exact string.
	if sessionARN == "" {                                   // Reject missing ARNs before any AWS call because remote-access lookup cannot succeed without one identifier.
		return nil, errors.New(errors.CodePlanInvalid, "remoteAccessSessionArn is required") // Surface the missing ARN as one stable plan-validation error for callers.
	}

	out, err := c.sdk.GetRemoteAccessSession(ctx, &awsdevicefarm.GetRemoteAccessSessionInput{Arn: aws.String(sessionARN)}) // Ask AWS for the latest remote-access session state, including its Appium endpoint metadata.
	if err != nil {                                                                                                        // Stop immediately when AWS rejects or cannot fulfill the remote-access lookup request.
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to get device farm remote access session", err) // Surface lookup failures through one stable storage-read error contract.
	}
	if out.RemoteAccessSession == nil { // Reject empty payloads because callers need the session state and endpoint metadata to continue.
		return nil, errors.New(errors.CodeTraceNotFound, "device farm remote access session not found") // Surface missing payloads as one stable not-found error.
	}

	return normalizeRemoteAccessSessionPayload(out.RemoteAccessSession), nil // Return the normalized payload so higher layers can consume stable field names independent of AWS SDK shape.
}

// StopRemoteAccessSession ends one Device Farm remote-access session so interactive device slots are released promptly after the platform session ends.
func (c *Client) StopRemoteAccessSession(ctx context.Context, remoteAccessSessionARN string) error {
	sessionARN := strings.TrimSpace(remoteAccessSessionARN) // Normalize the target remote-access session ARN once so the stop request uses one stable identifier.
	if sessionARN == "" {                                   // Reject missing ARNs before any AWS call because stopping a remote-access session requires one identifier.
		return errors.New(errors.CodePlanInvalid, "remoteAccessSessionArn is required") // Surface the missing ARN as one stable plan-validation error for callers.
	}

	if _, err := c.sdk.StopRemoteAccessSession(ctx, &awsdevicefarm.StopRemoteAccessSessionInput{Arn: aws.String(sessionARN)}); err != nil { // Ask AWS to terminate the remote-access session so the project stops consuming the interactive device slot.
		return errors.Wrap(errors.CodeInternal, "failed to stop device farm remote access session", err) // Surface stop failures through one stable internal error contract.
	}

	return nil // Return success once AWS has accepted the remote-access stop request.
}

// waitForRemoteDriverEndpoint polls one created remote-access session until AWS exposes a ready Appium endpoint or the wait times out.
func (c *Client) waitForRemoteDriverEndpoint(ctx context.Context, projectARN string, deviceARN string, sessionName string, sessionARN string) (map[string]interface{}, error) {
	waitCtx := ctx                                      // Start from the caller context so explicit upstream deadlines still take precedence over the repository default wait timeout.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline { // Apply the repository default wait timeout only when the caller did not already provide one explicit deadline.
		var cancel context.CancelFunc                                              // Keep the timeout cancel handle so timer resources are released once polling stops.
		waitCtx, cancel = context.WithTimeout(ctx, defaultRemoteAccessWaitTimeout) // Bound remote-access provisioning so StartSession never waits forever for one Appium endpoint.
		defer cancel()                                                             // Ensure timeout resources are always released before the function returns.
	}

	ticker := time.NewTicker(defaultRemoteAccessPollInterval) // Poll on one steady cadence so AWS session status changes are observed without a tight loop.
	defer ticker.Stop()                                       // Release ticker resources once polling ends through success, timeout, or cancellation.

	for { // Continue polling until AWS exposes one Appium endpoint, the session fails, or the wait context ends.
		result, err := c.GetRemoteAccessSession(waitCtx, sessionARN) // Fetch the latest remote-access session state from AWS for readiness inspection.
		if err != nil {                                              // Stop immediately when the poll lookup itself fails because readiness cannot be determined safely.
			return nil, err // Preserve the wrapped AWS lookup failure for the caller.
		}

		if remoteDriverEndpoint, _ := result["remoteDriverEndpoint"].(string); strings.TrimSpace(remoteDriverEndpoint) != "" { // Treat the remoteDriverEndpoint as the readiness signal because Appium clients need exactly this URL.
			result["projectArn"] = projectARN // Add the resolved project ARN so callers can persist one stable ownership context for later cleanup.
			result["deviceArn"] = deviceARN   // Add the resolved device ARN so callers can surface the exact provisioned device in logs and diagnostics.
			result["name"] = sessionName      // Add the final session name so higher layers can surface it consistently in tool output or logs.
			result["sessionArn"] = sessionARN // Add the session ARN under a stable repository key alongside the AWS-shaped payload.
			return result, nil                // Return the ready remote-access payload as soon as AWS exposes the Appium endpoint.
		}

		if status, _ := result["status"].(string); isTerminalRemoteAccessStatus(status) { // Stop early when AWS has already moved the session to one terminal state without exposing an Appium endpoint.
			return nil, errors.New(errors.CodeInternal, "device farm remote access session became terminal before remoteDriverEndpoint was ready") // Surface terminal-without-endpoint as one stable provisioning failure.
		}

		select {
		case <-waitCtx.Done(): // Stop polling once the caller deadline or the repository default wait timeout expires.
			return nil, errors.Wrap(errors.CodeAppTimeout, "timed out waiting for device farm remote access session", waitCtx.Err()) // Surface timeouts through the stable Appium-timeout code used elsewhere by session setup.
		case <-ticker.C: // Wait for the next poll tick before asking AWS for the updated session state again.
		}
	}
}

// normalizeRemoteAccessSessionPayload maps one AWS remote-access session into the stable repository response shape used by orchestrator and tests.
func normalizeRemoteAccessSessionPayload(session *types.RemoteAccessSession) map[string]interface{} {
	result := map[string]interface{}{ // Start one normalized payload map that flattens the AWS SDK shape into stable repository keys.
		"sessionArn": aws.ToString(session.Arn),  // Preserve the remote-access session ARN because later cleanup and diagnostics depend on it.
		"name":       aws.ToString(session.Name), // Preserve the session display name so callers can surface it in logs and tool output.
		"status":     string(session.Status),     // Preserve the AWS remote-access session status so callers can reason about readiness and terminal states.
		"created":    session.Created,            // Preserve the creation timestamp for diagnostics and operator visibility.
		"started":    session.Started,            // Preserve the start timestamp when AWS has begun provisioning or running the session.
		"stopped":    session.Stopped,            // Preserve the stop timestamp when the remote-access session has already ended.
	}
	if session.Device != nil { // Flatten the nested device payload only when AWS has already populated the provisioned device metadata.
		result["deviceArn"] = aws.ToString(session.Device.Arn) // Preserve the concrete provisioned device ARN for diagnostics and later session ownership checks.
	}
	if session.Endpoints != nil { // Flatten the nested endpoint structure only when AWS has returned the current interactive and WebDriver endpoints.
		result["interactiveEndpoint"] = aws.ToString(session.Endpoints.InteractiveEndpoint)   // Preserve the browser-facing interactive endpoint for human operators.
		result["remoteDriverEndpoint"] = aws.ToString(session.Endpoints.RemoteDriverEndpoint) // Preserve the Appium/WebDriver endpoint required by repository automation clients.
	} else if session.Endpoint != nil { // Fall back to the deprecated single interactive endpoint field when AWS has not populated the newer nested endpoints object yet.
		result["interactiveEndpoint"] = aws.ToString(session.Endpoint) // Preserve the deprecated browser-facing endpoint so operators still see one usable interactive URL.
	}
	return result // Return the fully normalized remote-access session payload to the caller.
}

// isTerminalRemoteAccessStatus reports whether one AWS remote-access session status indicates that provisioning or execution has already ended.
func isTerminalRemoteAccessStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) { // Normalize the AWS status value once so terminal detection stays case-insensitive and stable.
	case "COMPLETED", "COMPLETING", "STOPPING", "STOPPED", "ERRORED", "FAILED": // Treat ended or failing remote-access states as terminal because no later Appium endpoint will become available for the original session.
		return true // Return true so polling stops once AWS has already ended or irreversibly failed the remote-access session.
	default: // Treat every other status as non-terminal because the session may still surface a usable Appium endpoint later.
		return false // Return false so polling continues while AWS is still provisioning or actively running the remote-access session.
	}
}
