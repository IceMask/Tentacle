// client_test.go verifies Device Farm client validation, request shaping, and response mapping behavior without calling real AWS services.
package devicefarm

import (
	"context"
	"strings"
	"testing"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdevicefarm "github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

// fakeDeviceFarmAPI implements the narrow Device Farm SDK surface used by the repository client so tests can inject deterministic responses.
type fakeDeviceFarmAPI struct {
	scheduleRunInput      *awsdevicefarm.ScheduleRunInput
	scheduleRunOutput     *awsdevicefarm.ScheduleRunOutput
	scheduleRunErr        error
	getRunOutput          *awsdevicefarm.GetRunOutput
	getRunErr             error
	createUploadInput     *awsdevicefarm.CreateUploadInput
	createUploadOutput    *awsdevicefarm.CreateUploadOutput
	createUploadErr       error
	getUploadOutput       *awsdevicefarm.GetUploadOutput
	getUploadErr          error
	listDevicePoolsInput  *awsdevicefarm.ListDevicePoolsInput
	listDevicePoolsOutput *awsdevicefarm.ListDevicePoolsOutput
	listDevicePoolsErr    error
	createRemoteInput     *awsdevicefarm.CreateRemoteAccessSessionInput
	createRemoteOutput    *awsdevicefarm.CreateRemoteAccessSessionOutput
	createRemoteErr       error
	getRemoteInput        *awsdevicefarm.GetRemoteAccessSessionInput
	getRemoteOutput       *awsdevicefarm.GetRemoteAccessSessionOutput
	getRemoteErr          error
	stopRemoteInput       *awsdevicefarm.StopRemoteAccessSessionInput
	stopRemoteOutput      *awsdevicefarm.StopRemoteAccessSessionOutput
	stopRemoteErr         error
}

// ScheduleRun records the incoming request and returns the preconfigured fake output for deterministic client tests.
func (f *fakeDeviceFarmAPI) ScheduleRun(ctx context.Context, params *awsdevicefarm.ScheduleRunInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.ScheduleRunOutput, error) {
	_ = ctx                                      // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = optFns                                   // Ignore AWS option functions because this fake SDK records only the normalized repository request shape.
	f.scheduleRunInput = params                  // Preserve the request payload so the test can assert repository-side normalization and fallbacks.
	return f.scheduleRunOutput, f.scheduleRunErr // Return the preconfigured fake output so repository response mapping can be asserted directly.
}

// GetRun returns the preconfigured fake run lookup result for deterministic client tests.
func (f *fakeDeviceFarmAPI) GetRun(ctx context.Context, params *awsdevicefarm.GetRunInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.GetRunOutput, error) {
	_ = ctx                            // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = params                         // Ignore the request payload because this fake test focuses on repository response mapping.
	_ = optFns                         // Ignore AWS option functions because this fake SDK returns one preconfigured result immediately.
	return f.getRunOutput, f.getRunErr // Return the preconfigured fake output so repository response mapping can be asserted directly.
}

// CreateUpload records the incoming request and returns the preconfigured fake output for deterministic client tests.
func (f *fakeDeviceFarmAPI) CreateUpload(ctx context.Context, params *awsdevicefarm.CreateUploadInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.CreateUploadOutput, error) {
	_ = ctx                                        // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = optFns                                     // Ignore AWS option functions because this fake SDK records only the normalized repository request shape.
	f.createUploadInput = params                   // Preserve the request payload so the test can assert repository-side normalization and fallbacks.
	return f.createUploadOutput, f.createUploadErr // Return the preconfigured fake output so repository response mapping can be asserted directly.
}

// GetUpload returns the preconfigured fake upload lookup result for deterministic client tests.
func (f *fakeDeviceFarmAPI) GetUpload(ctx context.Context, params *awsdevicefarm.GetUploadInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.GetUploadOutput, error) {
	_ = ctx                                  // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = params                               // Ignore the request payload because this fake test focuses on repository response mapping.
	_ = optFns                               // Ignore AWS option functions because this fake SDK returns one preconfigured result immediately.
	return f.getUploadOutput, f.getUploadErr // Return the preconfigured fake output so repository response mapping can be asserted directly.
}

// ListDevicePools records the incoming request and returns the preconfigured fake output for deterministic client tests.
func (f *fakeDeviceFarmAPI) ListDevicePools(ctx context.Context, params *awsdevicefarm.ListDevicePoolsInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.ListDevicePoolsOutput, error) {
	_ = ctx                                              // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = optFns                                           // Ignore AWS option functions because this fake SDK records only the normalized repository request shape.
	f.listDevicePoolsInput = params                      // Preserve the request payload so the test can assert repository-side fallback behavior.
	return f.listDevicePoolsOutput, f.listDevicePoolsErr // Return the preconfigured fake output so repository fallback handling can be asserted directly.
}

// CreateRemoteAccessSession records the incoming request and returns the preconfigured fake output for deterministic remote-access tests.
func (f *fakeDeviceFarmAPI) CreateRemoteAccessSession(ctx context.Context, params *awsdevicefarm.CreateRemoteAccessSessionInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.CreateRemoteAccessSessionOutput, error) {
	_ = ctx                                        // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = optFns                                     // Ignore AWS option functions because this fake SDK records only the normalized repository request shape.
	f.createRemoteInput = params                   // Preserve the request payload so the test can assert repository-side remote-access normalization and fallbacks.
	return f.createRemoteOutput, f.createRemoteErr // Return the preconfigured fake output so repository remote-access response mapping can be asserted directly.
}

// GetRemoteAccessSession records the incoming request and returns the preconfigured fake output for deterministic remote-access tests.
func (f *fakeDeviceFarmAPI) GetRemoteAccessSession(ctx context.Context, params *awsdevicefarm.GetRemoteAccessSessionInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.GetRemoteAccessSessionOutput, error) {
	_ = ctx                                  // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = optFns                               // Ignore AWS option functions because this fake SDK records only the normalized repository request shape.
	f.getRemoteInput = params                // Preserve the request payload so the test can assert repository remote-access polling behavior.
	return f.getRemoteOutput, f.getRemoteErr // Return the preconfigured fake output so repository remote-access response mapping can be asserted directly.
}

// StopRemoteAccessSession records the incoming request and returns the preconfigured fake output for deterministic remote-access tests.
func (f *fakeDeviceFarmAPI) StopRemoteAccessSession(ctx context.Context, params *awsdevicefarm.StopRemoteAccessSessionInput, optFns ...func(*awsdevicefarm.Options)) (*awsdevicefarm.StopRemoteAccessSessionOutput, error) {
	_ = ctx                                    // Ignore the context because this fake SDK returns one preconfigured in-memory result immediately.
	_ = optFns                                 // Ignore AWS option functions because this fake SDK records only the normalized repository request shape.
	f.stopRemoteInput = params                 // Preserve the request payload so the test can assert repository remote-access cleanup behavior.
	return f.stopRemoteOutput, f.stopRemoteErr // Return the preconfigured fake output so repository remote-access cleanup behavior can be asserted directly.
}

// TestScheduleRunValidatesRequiredFields verifies that ScheduleRun fails fast on missing local configuration before any SDK call is needed.
func TestScheduleRunValidatesRequiredFields(t *testing.T) {
	client := &Client{cfg: config.DeviceFarmConfig{Mode: "run_api"}}                          // Construct one client without an SDK because this test exercises only the local input-validation path.
	if _, err := client.ScheduleRun(context.Background(), ScheduleRunRequest{}); err == nil { // Attempt to schedule one run with no project, app, or test package so the local validation path fails before SDK usage.
		t.Fatal("expected missing project ARN to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed requests.
	} else if !errors.IsCode(err, errors.CodeConfigMissing) { // Fail the test when the returned error does not preserve the stable missing-config code.
		t.Fatalf("expected config missing error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

// TestCreateUploadValidatesNameAndType verifies that CreateUpload rejects missing required local fields before any SDK call is attempted.
func TestCreateUploadValidatesNameAndType(t *testing.T) {
	client := &Client{cfg: config.DeviceFarmConfig{ProjectARN: "arn:aws:devicefarm:project"}}   // Construct one client with a fallback project ARN so the local upload field validation path can be exercised directly.
	if _, err := client.CreateUpload(context.Background(), CreateUploadRequest{}); err == nil { // Attempt to create one upload with no name or type so the local validation path fails before SDK usage.
		t.Fatal("expected missing upload name to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed requests.
	} else if !errors.IsCode(err, errors.CodePlanInvalid) { // Fail the test when the returned error does not preserve the stable invalid-plan code.
		t.Fatalf("expected plan invalid error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

// TestGetRunValidatesARN verifies that GetRun rejects an empty ARN before any SDK call is attempted.
func TestGetRunValidatesARN(t *testing.T) {
	client := &Client{}                                                // Construct one client without an SDK because this test exercises only the local ARN-validation path.
	if _, err := client.GetRun(context.Background(), ""); err == nil { // Attempt to read one run with an empty ARN so the local validation path fails before SDK usage.
		t.Fatal("expected empty run ARN to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed requests.
	} else if !errors.IsCode(err, errors.CodePlanInvalid) { // Fail the test when the returned error does not preserve the stable invalid-plan code.
		t.Fatalf("expected plan invalid error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}

// TestScheduleRunFallsBackToFirstPoolAndMapsResult verifies that the client selects the first available pool when none is supplied and normalizes the returned run payload.
func TestScheduleRunFallsBackToFirstPoolAndMapsResult(t *testing.T) {
	fakeSDK := &fakeDeviceFarmAPI{ // Preconfigure one fake SDK that returns a single pool and a successful run payload so repository request shaping and response mapping can both be asserted.
		listDevicePoolsOutput: &awsdevicefarm.ListDevicePoolsOutput{DevicePools: []types.DevicePool{{Arn: aws.String("arn:pool:first")}}},
		scheduleRunOutput:     &awsdevicefarm.ScheduleRunOutput{Run: &types.Run{Arn: aws.String("arn:run:1"), Name: aws.String("run-name"), Status: types.ExecutionStatusCompleted, Result: types.ExecutionResultPassed}},
	}
	client := &Client{sdk: fakeSDK, cfg: config.DeviceFarmConfig{ProjectARN: "arn:project:fallback"}} // Construct one repository client around the fake SDK so fallback behavior can be asserted deterministically.

	result, err := client.ScheduleRun(context.Background(), ScheduleRunRequest{AppARN: "arn:app:1", TestPackageARN: "arn:testpkg:1"}) // Schedule one run without explicit project, pool, or run name so fallback resolution paths are exercised directly.
	if err != nil {                                                                                                                   // Fail the test when the repository client cannot schedule the fake run successfully.
		t.Fatalf("expected schedule run to succeed, got error: %v", err) // Surface the unexpected error so request-shaping regressions are obvious.
	}
	if aws.ToString(fakeSDK.listDevicePoolsInput.Arn) != "arn:project:fallback" { // Fail the test when first-pool resolution does not use the configured project fallback.
		t.Fatalf("expected pool lookup to use fallback project ARN, got %q", aws.ToString(fakeSDK.listDevicePoolsInput.Arn)) // Surface the unexpected project ARN so fallback regressions are obvious.
	}
	if aws.ToString(fakeSDK.scheduleRunInput.DevicePoolArn) != "arn:pool:first" { // Fail the test when the repository client does not forward the discovered first pool into ScheduleRun.
		t.Fatalf("expected first pool ARN to be used, got %q", aws.ToString(fakeSDK.scheduleRunInput.DevicePoolArn)) // Surface the unexpected pool ARN so fallback regressions are obvious.
	}
	if fakeSDK.scheduleRunInput.Test == nil || fakeSDK.scheduleRunInput.Test.Type != types.TestTypeAppiumNode { // Fail the test when the repository client does not default the test type to APPIUM_NODE.
		t.Fatalf("expected default test type APPIUM_NODE, got %#v", fakeSDK.scheduleRunInput.Test) // Surface the unexpected test payload so request-shaping regressions are obvious.
	}
	if result["runArn"] != "arn:run:1" || result["projectArn"] != "arn:project:fallback" || result["devicePoolArn"] != "arn:pool:first" { // Fail the test when the repository client does not normalize key run fields into the documented response shape.
		t.Fatalf("expected normalized run payload, got %#v", result) // Surface the unexpected payload so response-mapping regressions are obvious.
	}
	if !strings.HasPrefix(aws.ToString(fakeSDK.scheduleRunInput.Name), "mcp-run-") { // Fail the test when the repository client does not generate the documented timestamped fallback run name.
		t.Fatalf("expected generated fallback run name, got %q", aws.ToString(fakeSDK.scheduleRunInput.Name)) // Surface the unexpected run name so fallback regressions are obvious.
	}
	if _, err := time.Parse("20060102-150405", strings.TrimPrefix(aws.ToString(fakeSDK.scheduleRunInput.Name), "mcp-run-")); err != nil { // Fail the test when the generated fallback run name does not carry the documented UTC timestamp format.
		t.Fatalf("expected parseable fallback run timestamp, got %q", aws.ToString(fakeSDK.scheduleRunInput.Name)) // Surface the unexpected run name so fallback regressions are obvious.
	}
}

// TestGetRunFillsMissingCountersWithZero verifies that the repository client normalizes nil Device Farm counters into explicit zero values.
func TestGetRunFillsMissingCountersWithZero(t *testing.T) {
	fakeSDK := &fakeDeviceFarmAPI{getRunOutput: &awsdevicefarm.GetRunOutput{Run: &types.Run{Arn: aws.String("arn:run:2"), Name: aws.String("run-two"), Status: types.ExecutionStatusRunning, Result: types.ExecutionResultPending}}} // Preconfigure one fake SDK run response with nil counters so the repository normalization path can be asserted directly.
	client := &Client{sdk: fakeSDK}                                                                                                                                                                                                  // Construct one repository client around the fake SDK so response mapping can be asserted deterministically.

	result, err := client.GetRun(context.Background(), "arn:run:2") // Fetch one fake run so the repository response-mapping path can normalize the nil counter payload.
	if err != nil {                                                 // Fail the test when the repository client cannot map the fake run successfully.
		t.Fatalf("expected get run to succeed, got error: %v", err) // Surface the unexpected error so response-mapping regressions are obvious.
	}
	counters, ok := result["counters"].(map[string]int32) // Decode the normalized counters payload so the zero-fill behavior can be asserted directly.
	if !ok {                                              // Fail the test when the repository client does not return the documented counters shape.
		t.Fatalf("expected counters map, got %#v", result["counters"]) // Surface the unexpected counters payload so response-mapping regressions are obvious.
	}
	if counters["passed"] != 0 || counters["total"] != 0 { // Fail the test when nil AWS counters do not normalize to zero values.
		t.Fatalf("expected zeroed counters, got %#v", counters) // Surface the unexpected counters so normalization regressions are obvious.
	}
}

// TestCreateUploadUsesFallbackProjectAndUppercasesType verifies that the repository client applies project fallback and uppercases upload types before calling AWS.
func TestCreateUploadUsesFallbackProjectAndUppercasesType(t *testing.T) {
	fakeSDK := &fakeDeviceFarmAPI{createUploadOutput: &awsdevicefarm.CreateUploadOutput{Upload: &types.Upload{Arn: aws.String("arn:upload:1"), Name: aws.String("apk"), Type: types.UploadTypeAndroidApp, Status: types.UploadStatusInitialized, Url: aws.String("https://uploads.example/1"), ContentType: aws.String("application/octet-stream")}}} // Preconfigure one fake SDK upload response so request shaping and response mapping can both be asserted.
	client := &Client{sdk: fakeSDK, cfg: config.DeviceFarmConfig{ProjectARN: "arn:project:upload"}}                                                                                                                                                                                                                                                   // Construct one repository client around the fake SDK so fallback behavior can be asserted deterministically.

	result, err := client.CreateUpload(context.Background(), CreateUploadRequest{Name: "apk", Type: "android_app", ContentType: "application/octet-stream"}) // Create one upload without an explicit project ARN so fallback resolution and type normalization are exercised directly.
	if err != nil {                                                                                                                                          // Fail the test when the repository client cannot create the fake upload successfully.
		t.Fatalf("expected create upload to succeed, got error: %v", err) // Surface the unexpected error so request-shaping regressions are obvious.
	}
	if aws.ToString(fakeSDK.createUploadInput.ProjectArn) != "arn:project:upload" { // Fail the test when the repository client does not use the configured project fallback.
		t.Fatalf("expected fallback project ARN, got %q", aws.ToString(fakeSDK.createUploadInput.ProjectArn)) // Surface the unexpected project ARN so fallback regressions are obvious.
	}
	if fakeSDK.createUploadInput.Type != types.UploadTypeAndroidApp { // Fail the test when the repository client does not uppercase and normalize the upload type before calling AWS.
		t.Fatalf("expected upload type ANDROID_APP, got %q", fakeSDK.createUploadInput.Type) // Surface the unexpected type so request-shaping regressions are obvious.
	}
	if result["uploadArn"] != "arn:upload:1" || result["projectArn"] != "arn:project:upload" { // Fail the test when the repository client does not normalize the upload response payload as documented.
		t.Fatalf("expected normalized upload payload, got %#v", result) // Surface the unexpected payload so response-mapping regressions are obvious.
	}
}

// TestFirstDevicePoolARNRejectsEmptyResults verifies that the repository client surfaces the documented no-pool error when AWS returns no usable pools.
func TestFirstDevicePoolARNRejectsEmptyResults(t *testing.T) {
	client := &Client{sdk: &fakeDeviceFarmAPI{listDevicePoolsOutput: &awsdevicefarm.ListDevicePoolsOutput{DevicePools: []types.DevicePool{{Arn: aws.String("")}}}}} // Construct one repository client whose fake SDK returns no usable pool ARN values.
	if _, err := client.firstDevicePoolARN(context.Background(), "arn:project:no-pools"); err == nil {                                                              // Attempt to resolve the first pool so the no-usable-pool path is exercised directly.
		t.Fatal("expected missing pool lookup to fail") // Surface the missing failure because callers depend on a stable error when no pools are available.
	} else if !errors.IsCode(err, errors.CodeSchedNoWorker) { // Fail the test when the returned error does not preserve the stable no-worker code.
		t.Fatalf("expected no-worker error, got %v", err) // Surface the unexpected error so pool-resolution regressions are obvious.
	}
}

// TestCreateRemoteAccessSessionWaitsForRemoteDriverEndpoint verifies that the repository client waits for one WebDriver endpoint and normalizes the returned payload.
func TestCreateRemoteAccessSessionWaitsForRemoteDriverEndpoint(t *testing.T) {
	fakeSDK := &fakeDeviceFarmAPI{ // Preconfigure one fake SDK that immediately returns one ready remote-access session with a WebDriver endpoint so repository request shaping and response mapping can be asserted together.
		createRemoteOutput: &awsdevicefarm.CreateRemoteAccessSessionOutput{
			RemoteAccessSession: &types.RemoteAccessSession{
				Arn:    aws.String("arn:session:1"),
				Status: types.ExecutionStatusPending,
			},
		},
		getRemoteOutput: &awsdevicefarm.GetRemoteAccessSessionOutput{
			RemoteAccessSession: &types.RemoteAccessSession{
				Arn:    aws.String("arn:session:1"),
				Device: &types.Device{Arn: aws.String("arn:device:1")},
				Name:   aws.String("remote-name"),
				Status: types.ExecutionStatusRunning,
				Endpoints: &types.RemoteAccessEndpoints{
					InteractiveEndpoint:  aws.String("https://interactive.example/session"),
					RemoteDriverEndpoint: aws.String("https://driver.example/wd/hub"),
				},
			},
		},
	}
	client := &Client{sdk: fakeSDK, cfg: config.DeviceFarmConfig{ProjectARN: "arn:project:remote"}} // Construct one repository client around the fake SDK so remote-access fallback behavior can be asserted deterministically.

	result, err := client.CreateRemoteAccessSession(context.Background(), CreateRemoteAccessSessionRequest{DeviceARN: "arn:device:1", Name: "remote-name"}) // Create one remote-access session without an explicit project ARN so the configured fallback and polling path are both exercised directly.
	if err != nil {                                                                                                                                         // Fail the test when the repository client cannot create the fake remote-access session successfully.
		t.Fatalf("expected remote access session creation to succeed, got error: %v", err) // Surface the unexpected error so remote-access regressions are obvious.
	}
	if aws.ToString(fakeSDK.createRemoteInput.ProjectArn) != "arn:project:remote" { // Fail the test when the repository client does not use the configured project fallback for remote-access creation.
		t.Fatalf("expected fallback project ARN, got %q", aws.ToString(fakeSDK.createRemoteInput.ProjectArn)) // Surface the unexpected project ARN so fallback regressions are obvious.
	}
	if aws.ToString(fakeSDK.getRemoteInput.Arn) != "arn:session:1" { // Fail the test when the repository client does not poll the created remote-access session ARN.
		t.Fatalf("expected remote-access poll to use created session ARN, got %q", aws.ToString(fakeSDK.getRemoteInput.Arn)) // Surface the unexpected poll ARN so readiness regressions are obvious.
	}
	if result["remoteDriverEndpoint"] != "https://driver.example/wd/hub" || result["interactiveEndpoint"] != "https://interactive.example/session" { // Fail the test when the repository client does not flatten the AWS endpoint payload into the documented response shape.
		t.Fatalf("expected normalized remote endpoints, got %#v", result) // Surface the unexpected payload so response-mapping regressions are obvious.
	}
}

// TestStopRemoteAccessSessionValidatesARN verifies that StopRemoteAccessSession rejects one empty ARN before any AWS call is attempted.
func TestStopRemoteAccessSessionValidatesARN(t *testing.T) {
	client := &Client{}                                                              // Construct one client without an SDK because this test exercises only the local ARN-validation path.
	if err := client.StopRemoteAccessSession(context.Background(), ""); err == nil { // Attempt to stop one remote-access session with an empty ARN so the local validation path fails before SDK usage.
		t.Fatal("expected empty remote access session ARN to fail") // Surface the missing validation failure because callers depend on fast local feedback for malformed cleanup requests.
	} else if !errors.IsCode(err, errors.CodePlanInvalid) { // Fail the test when the returned error does not preserve the stable invalid-plan code.
		t.Fatalf("expected plan invalid error, got %v", err) // Surface the unexpected error so validation regressions are obvious.
	}
}
