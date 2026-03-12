package devicefarm

import (
	"context"
	"strings"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsdevicefarm "github.com/aws/aws-sdk-go-v2/service/devicefarm"
	"github.com/aws/aws-sdk-go-v2/service/devicefarm/types"
)

// Client wraps AWS Device Farm APIs used by run_api mode.
type Client struct {
	sdk *awsdevicefarm.Client
	cfg config.DeviceFarmConfig
}

// ScheduleRunRequest defines the input for scheduling a Device Farm run.
type ScheduleRunRequest struct {
	RunName        string
	ProjectARN     string
	AppARN         string
	DevicePoolARN  string
	TestType       string
	TestPackageARN string
	TestSpecARN    string
}

// CreateUploadRequest defines the input for creating a Device Farm upload slot.
type CreateUploadRequest struct {
	ProjectARN  string
	Name        string
	Type        string
	ContentType string
}

// NewClient creates a Device Farm client from AWS and Device Farm config.
func NewClient(ctx context.Context, awsCfg config.AWSConfig, dfCfg config.DeviceFarmConfig) (*Client, error) {
	// Load config with explicit region/profile so developers can choose credentials source.
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(awsCfg.Region),
	}
	if strings.TrimSpace(awsCfg.Profile) != "" {
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(awsCfg.Profile))
	}

	loaded, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, errors.Wrap(errors.CodeConfigInvalid, "failed to load aws config for device farm", err)
	}

	return &Client{
		sdk: awsdevicefarm.NewFromConfig(loaded),
		cfg: dfCfg,
	}, nil
}

// ScheduleRun creates a Device Farm run using uploaded app/test package ARNs.
func (c *Client) ScheduleRun(ctx context.Context, req ScheduleRunRequest) (map[string]interface{}, error) {
	// Resolve project ARN from request first, then fallback to configured project.
	projectARN := strings.TrimSpace(req.ProjectARN)
	if projectARN == "" {
		projectARN = strings.TrimSpace(c.cfg.ProjectARN)
	}
	if projectARN == "" {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm project arn is required")
	}
	if strings.TrimSpace(req.AppARN) == "" {
		return nil, errors.New(errors.CodePlanInvalid, "appArn is required")
	}
	if strings.TrimSpace(req.TestPackageARN) == "" {
		return nil, errors.New(errors.CodePlanInvalid, "testPackageArn is required")
	}

	// Resolve device pool from input or pick the first available pool in project.
	devicePoolARN := strings.TrimSpace(req.DevicePoolARN)
	if devicePoolARN == "" {
		pool, err := c.firstDevicePoolARN(ctx, projectARN)
		if err != nil {
			return nil, err
		}
		devicePoolARN = pool
	}

	// Use APPIUM_NODE as default test type for Appium-driven test package runs.
	testType := types.TestTypeAppiumNode
	if strings.TrimSpace(req.TestType) != "" {
		testType = types.TestType(strings.ToUpper(strings.TrimSpace(req.TestType)))
	}

	// Build test payload using package ARN + optional test spec.
	test := &types.ScheduleRunTest{
		Type:           testType,
		TestPackageArn: aws.String(req.TestPackageARN),
	}
	if strings.TrimSpace(req.TestSpecARN) != "" {
		test.TestSpecArn = aws.String(req.TestSpecARN)
	}

	// Use caller-provided name or generate a deterministic timestamped fallback.
	runName := strings.TrimSpace(req.RunName)
	if runName == "" {
		runName = "mcp-run-" + time.Now().UTC().Format("20060102-150405")
	}

	// Schedule the Device Farm run.
	out, err := c.sdk.ScheduleRun(ctx, &awsdevicefarm.ScheduleRunInput{
		ProjectArn:    aws.String(projectARN),
		AppArn:        aws.String(req.AppARN),
		DevicePoolArn: aws.String(devicePoolARN),
		Name:          aws.String(runName),
		Test:          test,
	})
	if err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to schedule device farm run", err)
	}
	if out.Run == nil {
		return nil, errors.New(errors.CodeInternal, "device farm returned empty run payload")
	}

	// Return normalized run fields for MCP/JSON-RPC responses.
	return map[string]interface{}{
		"runArn":        aws.ToString(out.Run.Arn),
		"runName":       aws.ToString(out.Run.Name),
		"status":        string(out.Run.Status),
		"result":        string(out.Run.Result),
		"projectArn":    projectARN,
		"devicePoolArn": devicePoolARN,
	}, nil
}

// GetRun fetches the latest status for a Device Farm run.
func (c *Client) GetRun(ctx context.Context, runARN string) (map[string]interface{}, error) {
	if strings.TrimSpace(runARN) == "" {
		return nil, errors.New(errors.CodePlanInvalid, "runArn is required")
	}

	// Query run status and summary details from AWS.
	out, err := c.sdk.GetRun(ctx, &awsdevicefarm.GetRunInput{Arn: aws.String(runARN)})
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to get device farm run", err)
	}
	if out.Run == nil {
		return nil, errors.New(errors.CodeTraceNotFound, "device farm run not found")
	}
	if out.Run.Counters == nil {
		out.Run.Counters = &types.Counters{}
	}

	// Expose fields commonly needed by API consumers.
	return map[string]interface{}{
		"runArn":        aws.ToString(out.Run.Arn),
		"runName":       aws.ToString(out.Run.Name),
		"status":        string(out.Run.Status),
		"result":        string(out.Run.Result),
		"message":       aws.ToString(out.Run.Message),
		"created":       out.Run.Created,
		"started":       out.Run.Started,
		"stopped":       out.Run.Stopped,
		"totalJobs":     out.Run.TotalJobs,
		"completedJobs": out.Run.CompletedJobs,
		"counters": map[string]int32{
			"passed":  aws.ToInt32(out.Run.Counters.Passed),
			"failed":  aws.ToInt32(out.Run.Counters.Failed),
			"warned":  aws.ToInt32(out.Run.Counters.Warned),
			"skipped": aws.ToInt32(out.Run.Counters.Skipped),
			"stopped": aws.ToInt32(out.Run.Counters.Stopped),
			"errored": aws.ToInt32(out.Run.Counters.Errored),
			"total":   aws.ToInt32(out.Run.Counters.Total),
		},
	}, nil
}

// CreateUpload creates a Device Farm upload and returns the pre-signed upload URL.
func (c *Client) CreateUpload(ctx context.Context, req CreateUploadRequest) (map[string]interface{}, error) {
	projectARN := strings.TrimSpace(req.ProjectARN)
	if projectARN == "" {
		projectARN = strings.TrimSpace(c.cfg.ProjectARN)
	}
	if projectARN == "" {
		return nil, errors.New(errors.CodeConfigMissing, "devicefarm project arn is required")
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, errors.New(errors.CodePlanInvalid, "upload name is required")
	}

	uploadType := strings.TrimSpace(req.Type)
	if uploadType == "" {
		return nil, errors.New(errors.CodePlanInvalid, "upload type is required")
	}

	input := &awsdevicefarm.CreateUploadInput{
		ProjectArn: aws.String(projectARN),
		Name:       aws.String(name),
		Type:       types.UploadType(strings.ToUpper(uploadType)),
	}
	if strings.TrimSpace(req.ContentType) != "" {
		input.ContentType = aws.String(strings.TrimSpace(req.ContentType))
	}

	out, err := c.sdk.CreateUpload(ctx, input)
	if err != nil {
		return nil, errors.Wrap(errors.CodeInternal, "failed to create device farm upload", err)
	}
	if out.Upload == nil {
		return nil, errors.New(errors.CodeInternal, "device farm returned empty upload payload")
	}

	return map[string]interface{}{
		"uploadArn":   aws.ToString(out.Upload.Arn),
		"name":        aws.ToString(out.Upload.Name),
		"type":        string(out.Upload.Type),
		"status":      string(out.Upload.Status),
		"message":     aws.ToString(out.Upload.Message),
		"uploadUrl":   aws.ToString(out.Upload.Url),
		"projectArn":  projectARN,
		"contentType": aws.ToString(out.Upload.ContentType),
	}, nil
}

// GetUpload fetches Device Farm upload processing status.
func (c *Client) GetUpload(ctx context.Context, uploadARN string) (map[string]interface{}, error) {
	if strings.TrimSpace(uploadARN) == "" {
		return nil, errors.New(errors.CodePlanInvalid, "uploadArn is required")
	}

	out, err := c.sdk.GetUpload(ctx, &awsdevicefarm.GetUploadInput{
		Arn: aws.String(strings.TrimSpace(uploadARN)),
	})
	if err != nil {
		return nil, errors.Wrap(errors.CodeStoreRead, "failed to get device farm upload", err)
	}
	if out.Upload == nil {
		return nil, errors.New(errors.CodeTraceNotFound, "device farm upload not found")
	}

	return map[string]interface{}{
		"uploadArn":   aws.ToString(out.Upload.Arn),
		"name":        aws.ToString(out.Upload.Name),
		"type":        string(out.Upload.Type),
		"status":      string(out.Upload.Status),
		"message":     aws.ToString(out.Upload.Message),
		"metadata":    aws.ToString(out.Upload.Metadata),
		"uploadUrl":   aws.ToString(out.Upload.Url),
		"contentType": aws.ToString(out.Upload.ContentType),
		"created":     out.Upload.Created,
	}, nil
}

// firstDevicePoolARN resolves the first available device pool in a project.
func (c *Client) firstDevicePoolARN(ctx context.Context, projectARN string) (string, error) {
	// Query available pools and return the first valid ARN for quickstart usage.
	out, err := c.sdk.ListDevicePools(ctx, &awsdevicefarm.ListDevicePoolsInput{
		Arn: aws.String(projectARN),
	})
	if err != nil {
		return "", errors.Wrap(errors.CodeStoreRead, "failed to list device farm pools", err)
	}
	for _, pool := range out.DevicePools {
		if arn := strings.TrimSpace(aws.ToString(pool.Arn)); arn != "" {
			return arn, nil
		}
	}
	return "", errors.New(errors.CodeSchedNoWorker, "no device pool available in device farm project")
}
