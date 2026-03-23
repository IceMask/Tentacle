// devicefarm_remote_access_test.go verifies that remote-access-backed sessions use the AWS-provisioned Appium endpoint instead of the local Appium dependency and that cleanup stops the remote reservation.
package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"mcp_for_appium/internal/devicefarm"
)

// fakeRemoteAccessClient records remote-access lifecycle calls so tests can assert that the orchestrator provisions and stops Device Farm reservations correctly.
type fakeRemoteAccessClient struct {
	mu                   sync.Mutex
	createRequests       []devicefarm.CreateRemoteAccessSessionRequest
	stopRequests         []string
	remoteDriverEndpoint string
	remoteSessionARN     string
}

// ScheduleRun is not used by these tests and therefore returns one explicit panic when invoked unexpectedly.
func (f *fakeRemoteAccessClient) ScheduleRun(ctx context.Context, req devicefarm.ScheduleRunRequest) (map[string]interface{}, error) {
	panic("unexpected ScheduleRun call in remote-access session test") // Fail fast because the test exercises only interactive remote-access lifecycle paths.
}

// CreateUpload is not used by these tests and therefore returns one explicit panic when invoked unexpectedly.
func (f *fakeRemoteAccessClient) CreateUpload(ctx context.Context, req devicefarm.CreateUploadRequest) (map[string]interface{}, error) {
	panic("unexpected CreateUpload call in remote-access session test") // Fail fast because the test exercises only interactive remote-access lifecycle paths.
}

// GetUpload is not used by these tests and therefore returns one explicit panic when invoked unexpectedly.
func (f *fakeRemoteAccessClient) GetUpload(ctx context.Context, uploadARN string) (map[string]interface{}, error) {
	panic("unexpected GetUpload call in remote-access session test") // Fail fast because the test exercises only interactive remote-access lifecycle paths.
}

// GetRun is not used by these tests and therefore returns one explicit panic when invoked unexpectedly.
func (f *fakeRemoteAccessClient) GetRun(ctx context.Context, runARN string) (map[string]interface{}, error) {
	panic("unexpected GetRun call in remote-access session test") // Fail fast because the test exercises only interactive remote-access lifecycle paths.
}

// CreateRemoteAccessSession records the request and returns one ready remote-access payload so StartSession can open Appium against the remote endpoint immediately.
func (f *fakeRemoteAccessClient) CreateRemoteAccessSession(ctx context.Context, req devicefarm.CreateRemoteAccessSessionRequest) (map[string]interface{}, error) {
	f.mu.Lock()                                      // Serialize access to the recorded request slice because StartSession may run concurrently in other tests.
	defer f.mu.Unlock()                              // Release the request-recording mutex before the fake client returns to the orchestrator.
	f.createRequests = append(f.createRequests, req) // Preserve the incoming request so the test can assert the resolved project, device, and app fields.
	return map[string]interface{}{                   // Return one normalized ready payload that matches the repository's Device Farm client contract.
		"sessionArn":           f.remoteSessionARN,
		"remoteDriverEndpoint": f.remoteDriverEndpoint,
	}, nil
}

// GetRemoteAccessSession is not used by these tests because the fake create path already returns one ready endpoint.
func (f *fakeRemoteAccessClient) GetRemoteAccessSession(ctx context.Context, remoteAccessSessionARN string) (map[string]interface{}, error) {
	panic("unexpected GetRemoteAccessSession call in remote-access session test") // Fail fast because this test injects one already-ready remote-access payload.
}

// StopRemoteAccessSession records the remote-access session ARN so the test can assert that EndSession releases the AWS reservation.
func (f *fakeRemoteAccessClient) StopRemoteAccessSession(ctx context.Context, remoteAccessSessionARN string) error {
	f.mu.Lock()                                                     // Serialize access to the recorded stop slice because EndSession may run concurrently in other tests.
	defer f.mu.Unlock()                                             // Release the stop-recording mutex before the fake client returns to the orchestrator.
	f.stopRequests = append(f.stopRequests, remoteAccessSessionARN) // Preserve the stop request so the test can assert the exact reservation ARN used during cleanup.
	return nil                                                      // Return success so the orchestrator cleanup path can continue normally.
}

// remoteAccessAppiumProbe records the Appium session create/delete calls that the orchestrator sends to the fake remote endpoint.
type remoteAccessAppiumProbe struct {
	mu               sync.Mutex
	startSessionHits int
	deleteHits       int
}

// handle routes fake Appium new-session and delete-session calls while rejecting every unexpected path explicitly.
func (p *remoteAccessAppiumProbe) handle(response http.ResponseWriter, request *http.Request) {
	p.mu.Lock()         // Serialize updates to the call counters because the Appium client may issue concurrent HTTP requests in future test expansions.
	defer p.mu.Unlock() // Release the probe mutex before the HTTP handler returns to the caller.

	if request.Method == http.MethodPost && request.URL.Path == "/session" { // Detect the Appium new-session call that StartSession should send to the Device Farm remote endpoint.
		p.startSessionHits++                                                             // Record the new-session call so the test can assert that the orchestrator used the remote Appium endpoint.
		response.Header().Set("Content-Type", "application/json")                        // Return JSON so the production Appium client can decode the fake response.
		_, _ = response.Write([]byte(`{"value":{"sessionId":"remote-appium-session"}}`)) // Return one valid Appium new-session payload so StartSession can proceed normally.
		return                                                                           // Stop after writing the fake new-session response.
	}
	if request.Method == http.MethodDelete && request.URL.Path == "/session/remote-appium-session" { // Detect the Appium delete-session call that EndSession should send to the Device Farm remote endpoint.
		p.deleteHits++                                            // Record the delete-session call so the test can assert that EndSession closes the remote Appium session.
		response.Header().Set("Content-Type", "application/json") // Return JSON so the production Appium client can decode the fake response cleanly.
		_, _ = response.Write([]byte(`{"value":null}`))           // Return one valid Appium delete-session payload so EndSession can proceed normally.
		return                                                    // Stop after writing the fake delete-session response.
	}
	http.NotFound(response, request) // Reject every unexpected Appium path so the test surfaces repository routing regressions immediately.
}

// TestStartAndEndSessionUseDeviceFarmRemoteAccess verifies that StartSession provisions one remote-access endpoint instead of depending on local Appium and that EndSession releases the reservation.
func TestStartAndEndSessionUseDeviceFarmRemoteAccess(t *testing.T) {
	harness := newTraceTerminalHarness(t) // Start one isolated orchestrator harness so the production persistence and Redis mapping paths are exercised for real.

	appiumProbe := &remoteAccessAppiumProbe{}                                // Construct one fake remote Appium endpoint probe so the test can observe StartSession and EndSession HTTP calls.
	appiumServer := httptest.NewServer(http.HandlerFunc(appiumProbe.handle)) // Start one in-memory Appium-compatible HTTP server that represents the Device Farm remoteDriverEndpoint.
	defer appiumServer.Close()                                               // Ensure the fake remote Appium endpoint always stops after the test completes.

	deviceFarmClient := &fakeRemoteAccessClient{ // Construct one fake Device Farm client that returns the fake remote Appium endpoint and records cleanup calls.
		remoteDriverEndpoint: appiumServer.URL,
		remoteSessionARN:     "arn:aws:devicefarm:remote-access-session:test",
	}
	harness.service.dfMode = "remote_access"                         // Force the production service into remote-access runtime mode so StartSession resolves one AWS-backed Appium endpoint.
	harness.service.dfProjectARN = "arn:aws:devicefarm:project:test" // Seed one default project ARN so the test can assert the service-side fallback behavior.
	harness.service.deviceFarm = deviceFarmClient                    // Inject the fake Device Farm runtime client so the test can observe remote-access provisioning and cleanup.

	session, err := harness.service.StartSession(context.Background(), "project-remote", map[string]interface{}{ // Start one production platform session whose capability payload selects one Device Farm remote-access device.
		"platformName":          "Android",
		"appium:automationName": "UiAutomator2",
		"devicefarm:deviceArn":  "arn:aws:devicefarm:device:test",
		"devicefarm:appArn":     "arn:aws:devicefarm:upload:app",
	})
	if err != nil { // Stop immediately when the remote-access-backed StartSession path fails because the rest of the assertions depend on one live platform session.
		t.Fatalf("expected remote-access StartSession to succeed, got error: %v", err) // Surface the unexpected failure so remote-access lifecycle regressions are obvious.
	}

	deviceFarmClient.mu.Lock()                     // Lock the fake Device Farm client so the recorded create requests can be inspected deterministically.
	if len(deviceFarmClient.createRequests) != 1 { // Reject missing or duplicate create calls because StartSession should provision exactly one remote-access reservation.
		deviceFarmClient.mu.Unlock()                                                                        // Release the fake-client mutex before failing the test.
		t.Fatalf("expected one remote-access create request, got %d", len(deviceFarmClient.createRequests)) // Surface the unexpected provisioning count so lifecycle regressions are obvious.
	}
	createRequest := deviceFarmClient.createRequests[0] // Read back the single recorded create request so the resolved project and device fields can be asserted directly.
	deviceFarmClient.mu.Unlock()                        // Release the fake-client mutex once the recorded create request has been copied for later assertions.

	if createRequest.ProjectARN != "arn:aws:devicefarm:project:test" { // Reject incorrect project resolution because remote-access creation should fall back to the configured default project ARN.
		t.Fatalf("expected fallback Device Farm project ARN, got %q", createRequest.ProjectARN) // Surface the unexpected project ARN so fallback regressions are obvious.
	}
	if createRequest.DeviceARN != "arn:aws:devicefarm:device:test" { // Reject incorrect device propagation because remote-access creation requires the exact caller-selected device ARN.
		t.Fatalf("expected device ARN to be forwarded, got %q", createRequest.DeviceARN) // Surface the unexpected device ARN so capability-parsing regressions are obvious.
	}

	cachedAppiumURL, err := harness.cache.Get(context.Background(), appiumSessionURLKeyPrefix+session.ID) // Read the persisted per-session Appium endpoint so the remote Driver URL mapping can be asserted directly.
	if err != nil {                                                                                       // Stop immediately when the service failed to persist the session-specific Appium endpoint mapping.
		t.Fatalf("expected cached remote appium url, got error: %v", err) // Surface the missing cache mapping so restore-path regressions are obvious.
	}
	if cachedAppiumURL != appiumServer.URL { // Reject incorrect endpoint persistence because later restore and screenshot paths depend on reconnecting to the same remote endpoint.
		t.Fatalf("expected cached remote appium url %q, got %q", appiumServer.URL, cachedAppiumURL) // Surface the unexpected endpoint mapping so restore-path regressions are obvious.
	}

	harness.service.appiumMu.Lock()                    // Clear the in-memory Appium client cache so the next lookup must restore from the persisted Redis mappings.
	delete(harness.service.appiumMap, session.ID)      // Remove the cached Appium client entry so getAppiumClient exercises the restore path.
	delete(harness.service.appiumLastUsed, session.ID) // Remove the cached last-used timestamp so the restore path rewrites it from scratch.
	harness.service.appiumMu.Unlock()                  // Release the Appium cache mutex before the test asks the service to restore the client.

	restoredClient, err := harness.service.getAppiumClient(context.Background(), session.ID) // Restore one production Appium client from the persisted remote endpoint and Appium session id mappings.
	if err != nil {                                                                          // Stop immediately when the service cannot restore the session-specific remote Appium client.
		t.Fatalf("expected remote appium client restore to succeed, got error: %v", err) // Surface the restore failure so remote session reuse regressions are obvious.
	}
	if restoredClient == nil { // Reject nil restored clients because the restore path should always return one usable client on success.
		t.Fatal("expected restored remote appium client") // Surface the nil restore result so later remote Appium actions are not attempted against one nil client.
	}

	if err := harness.service.EndSession(context.Background(), session.ID); err != nil { // End the production platform session so the remote Appium session and Device Farm reservation cleanup paths both run.
		t.Fatalf("expected remote-access EndSession to succeed, got error: %v", err) // Surface the unexpected failure so cleanup regressions are obvious.
	}

	appiumProbe.mu.Lock()                                                 // Lock the fake Appium probe so the recorded HTTP call counts can be inspected deterministically.
	if appiumProbe.startSessionHits != 1 || appiumProbe.deleteHits != 1 { // Reject unexpected Appium call counts because StartSession and EndSession should each hit the remote endpoint exactly once.
		appiumProbe.mu.Unlock()                                                                                                                   // Release the probe mutex before failing the test.
		t.Fatalf("expected one create and one delete Appium call, got create=%d delete=%d", appiumProbe.startSessionHits, appiumProbe.deleteHits) // Surface the unexpected counts so lifecycle regressions are obvious.
	}
	appiumProbe.mu.Unlock() // Release the probe mutex once the Appium lifecycle assertions have completed.

	deviceFarmClient.mu.Lock()                                                                                            // Lock the fake Device Farm client so the recorded stop requests can be inspected deterministically.
	if len(deviceFarmClient.stopRequests) != 1 || deviceFarmClient.stopRequests[0] != deviceFarmClient.remoteSessionARN { // Reject missing or incorrect stop calls because EndSession should release the exact AWS reservation it created earlier.
		deviceFarmClient.mu.Unlock()                                                                                                          // Release the fake-client mutex before failing the test.
		t.Fatalf("expected one remote-access stop request for %q, got %#v", deviceFarmClient.remoteSessionARN, deviceFarmClient.stopRequests) // Surface the unexpected stop payload so cleanup regressions are obvious.
	}
	deviceFarmClient.mu.Unlock() // Release the fake-client mutex once the recorded cleanup assertions have completed.
}

// TestRemoteAccessModeRequiresDeviceARN verifies that remote-access mode rejects StartSession calls that omit the required target device ARN.
func TestRemoteAccessModeRequiresDeviceARN(t *testing.T) {
	harness := newTraceTerminalHarness(t)                            // Start one isolated orchestrator harness so the production StartSession validation path runs with real persistence dependencies.
	harness.service.dfMode = "remote_access"                         // Force the production service into remote-access runtime mode so the custom Device Farm validation path is active.
	harness.service.dfProjectARN = "arn:aws:devicefarm:project:test" // Seed one default project ARN so only the missing device ARN should fail.
	harness.service.deviceFarm = &fakeRemoteAccessClient{}           // Inject one fake Device Farm client so the test can verify validation fails before any AWS provisioning call is attempted.

	_, err := harness.service.StartSession(context.Background(), "project-remote", map[string]interface{}{ // Attempt to start one remote-access session without the required device ARN so validation should fail before any Appium or AWS call.
		"platformName":          "Android",
		"appium:automationName": "UiAutomator2",
	})
	if err == nil { // Reject missing validation failures because remote-access sessions cannot be provisioned without one target device ARN.
		t.Fatal("expected missing devicefarm:deviceArn to fail in remote_access mode") // Surface the missing failure because callers depend on fast feedback for malformed capability payloads.
	}
	if payload, marshalErr := json.Marshal(err.Error()); marshalErr != nil || len(payload) == 0 { // Assert that the returned error remains serializable so transport layers can safely surface it.
		t.Fatalf("expected remote-access validation error to be serializable, got marshalErr=%v err=%v", marshalErr, err) // Surface any unexpected serialization issue because transports rely on stringifiable errors.
	}
}
