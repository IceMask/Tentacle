// adb_shell_test.go verifies the restrictive adb shell policy and argument construction used by the orchestrator service.
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"
)

// TestHelperProcess executes the fake adb subprocess used by AdbShell tests that replace subprocess creation with the current test binary.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" { // Exit immediately during normal test execution because the helper path should run only inside the fake subprocess.
		return // Stop here so normal tests do not execute the helper-process branch accidentally.
	}

	separatorIndex := -1              // Track the position of the explicit -- separator so the helper can recover the fake adb argv accurately.
	for index, arg := range os.Args { // Scan the current process argv so the helper can locate the forwarded fake adb command line.
		if arg == "--" { // Detect the separator inserted by the fake command factory below.
			separatorIndex = index // Record the separator position so the remaining args can be treated as the fake adb argv.
			break                  // Stop scanning once the separator has been found because later args belong to the fake adb invocation.
		}
	}
	if separatorIndex == -1 || separatorIndex+1 >= len(os.Args) { // Fail hard when the helper-process contract is broken because the test itself would be malformed.
		os.Exit(2) // Exit non-zero so the parent test sees one explicit helper-process contract failure.
	}

	_, _ = os.Stdout.WriteString(strings.Join(os.Args[separatorIndex+1:], " ")) // Echo the fake adb argv back to the parent test so argument construction can be asserted directly.
	os.Exit(0)                                                                  // Exit successfully so the parent test observes one successful fake adb subprocess invocation.
}

// TestValidateADBCommand verifies that the adb policy accepts reviewed read-only commands and rejects unsafe command families, subcommands, and arguments.
func TestValidateADBCommand(t *testing.T) {
	testCases := []struct {
		name      string
		command   []string
		wantError bool
		errorCode errors.ErrorCode
	}{
		{name: "allow getprop single key", command: []string{"getprop", "ro.build.version.sdk"}},
		{name: "allow dumpsys service", command: []string{"dumpsys", "activity", "activities"}},
		{name: "allow pm list packages", command: []string{"pm", "list", "packages", "-3"}},
		{name: "allow settings get", command: []string{"settings", "get", "global", "adb_enabled"}},
		{name: "allow logcat dump", command: []string{"logcat", "-d", "-t", "50", "ActivityManager:I", "*:S"}},
		{name: "allow wm size", command: []string{"wm", "size"}},
		{name: "allow ime list", command: []string{"ime", "list", "-a"}},
		{name: "reject empty command", command: []string{}, wantError: true, errorCode: errors.CodePlanInvalid},
		{name: "reject pm install", command: []string{"pm", "install", "-r", "http://malicious.apk"}, wantError: true, errorCode: errors.CodePermissionDenied},
		{name: "reject settings put", command: []string{"settings", "put", "global", "adb_enabled", "1"}, wantError: true, errorCode: errors.CodePermissionDenied},
		{name: "reject am start", command: []string{"am", "start", "-a", "android.intent.action.VIEW"}, wantError: true, errorCode: errors.CodePermissionDenied},
		{name: "reject monkey", command: []string{"monkey", "-p", "demo.app", "1"}, wantError: true, errorCode: errors.CodePermissionDenied},
		{name: "reject logcat stream", command: []string{"logcat"}, wantError: true, errorCode: errors.CodePermissionDenied},
		{name: "reject shell metacharacters", command: []string{"getprop", "ro.build.version.sdk;rm"}, wantError: true, errorCode: errors.CodePermissionDenied},
	}

	for _, testCase := range testCases { // Execute each policy scenario as one table-driven subtest so accepted and rejected cases share the same validator assertions cleanly.
		t.Run(testCase.name, func(t *testing.T) {
			normalized, err := validateADBCommand(testCase.command) // Validate the current command through the production adb policy helper.
			if testCase.wantError {                                 // Assert the rejection branch when the current policy scenario is expected to fail.
				if err == nil { // Fail when the policy unexpectedly accepts one unsafe command.
					t.Fatal("expected adb command validation error") // Surface the missing rejection because the command policy is the behavior under test.
				}
				if !errors.IsCode(err, testCase.errorCode) { // Assert the stable error code so callers can distinguish malformed and forbidden commands.
					t.Fatalf("expected error code %s, got %v", testCase.errorCode, err) // Surface the actual error so policy regressions are easy to diagnose.
				}
				return // Stop the current subtest once the expected rejection has been observed.
			}

			if err != nil { // Fail when the policy unexpectedly rejects one reviewed read-only command.
				t.Fatalf("expected adb command to pass validation, got error: %v", err) // Surface the rejection so policy regressions are easy to diagnose.
			}
			if len(normalized) != len(testCase.command) { // Assert that accepted commands keep the same token count after normalization.
				t.Fatalf("expected %d normalized tokens, got %d", len(testCase.command), len(normalized)) // Surface the actual normalized token count so normalization regressions are easy to diagnose.
			}
		})
	}
}

// TestAdbShellBuildsValidatedADBArguments verifies that AdbShell executes only normalized validated tokens and preserves the optional device serial selection.
func TestAdbShellBuildsValidatedADBArguments(t *testing.T) {
	originalRunner := adbCommandContext                                                    // Preserve the production subprocess factory so the test can restore it after replacing it.
	adbCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd { // Replace subprocess creation with one helper-process factory that echoes the requested argv.
		helperArgs := []string{"-test.run=TestHelperProcess", "--", name} // Seed the helper-process argv with the sentinel test selector and fake executable name.
		helperArgs = append(helperArgs, args...)                          // Append the fake adb argv so the helper process can echo it back to the parent test.
		cmd := exec.CommandContext(ctx, os.Args[0], helperArgs...)        // Spawn the current test binary as the fake adb subprocess so no real adb dependency is required.
		cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")        // Mark the subprocess as the helper path so TestHelperProcess executes its echo branch.
		return cmd                                                        // Return the helper-process command so AdbShell can invoke it through CombinedOutput normally.
	}
	defer func() {
		adbCommandContext = originalRunner // Restore the production subprocess factory after the test completes so later tests keep the real behavior.
	}()

	service := &Service{cfg: config.OrchestratorConfig{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}       // Construct one minimal service with one discard logger because AdbShell logs before invoking any subprocess.
	result, err := service.AdbShell(context.Background(), "emulator-5554", []string{"getprop", "ro.build.version.sdk"}) // Execute the production AdbShell path so normalization, policy validation, and argv assembly all run for real.
	if err != nil {                                                                                                     // Fail immediately when the reviewed read-only command unexpectedly fails policy validation or subprocess execution.
		t.Fatalf("expected AdbShell to succeed, got error: %v", err) // Surface the actual error so argument-construction regressions are easy to diagnose.
	}

	output, ok := result["output"].(string) // Extract the fake subprocess stdout so the exact constructed adb argv can be asserted directly.
	if !ok {                                // Reject non-string outputs because successful AdbShell responses must expose one textual command output payload.
		t.Fatalf("expected string output payload, got %#v", result["output"]) // Surface the actual payload so response-shape regressions are easy to diagnose.
	}
	expected := "adb -s emulator-5554 shell getprop ro.build.version.sdk" // Define the exact fake adb argv expected from the production AdbShell builder.
	if output != expected {                                               // Assert the exact constructed argv so policy-approved commands still execute the intended adb invocation.
		t.Fatalf("expected fake adb argv %q, got %q", expected, output) // Surface the actual argv so construction regressions are easy to diagnose.
	}
}

// TestAdbShellRejectsUnsafePMInstall verifies that AdbShell blocks one mutating package-manager command before any subprocess is started.
func TestAdbShellRejectsUnsafePMInstall(t *testing.T) {
	service := &Service{cfg: config.OrchestratorConfig{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}                  // Construct one minimal service with one discard logger because AdbShell logs before command-policy rejection.
	if _, err := service.AdbShell(context.Background(), "", []string{"pm", "install", "-r", "http://malicious.apk"}); err == nil { // Execute the production AdbShell path with one unsafe command so the policy rejection branch is exercised end to end.
		t.Fatal("expected unsafe pm install command to be rejected") // Surface the missing rejection because the command policy is the behavior under test.
	} else if !errors.IsCode(err, errors.CodePermissionDenied) { // Assert the stable error code so callers can classify policy rejections reliably.
		t.Fatalf("expected permission denied error code, got %v", err) // Surface the actual error so policy regressions are easy to diagnose.
	}
}
