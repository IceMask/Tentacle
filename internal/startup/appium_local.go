// appium_local.go manages one best-effort local Appium server process for loopback Appium URLs that should be auto-started by this repository.
package startup

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"mcp_for_appium/internal/errors"
)

const (
	localAppiumExecutableName  = "appium"               // localAppiumExecutableName keeps the default local Appium binary name in one place for all auto-start paths.
	localAppiumReadyPollEvery  = 200 * time.Millisecond // localAppiumReadyPollEvery bounds how often the supervisor polls /status while waiting for the local Appium process to become reachable.
	localAppiumShutdownTimeout = 5 * time.Second        // localAppiumShutdownTimeout bounds how long graceful supervisor shutdown waits before force-killing the spawned Appium process.
)

// LocalAppiumSupervisor owns one optional locally spawned Appium process for one configured loopback Appium base URL.
type LocalAppiumSupervisor struct {
	rawURL         string                            // rawURL stores the configured Appium base URL exactly as callers passed it so every reachability probe targets the same endpoint shape.
	mu             sync.Mutex                        // mu serializes process-start, process-stop, and process-state transitions inside the supervisor.
	cmd            *exec.Cmd                         // cmd holds the currently managed Appium process when the supervisor has successfully started one locally.
	waitDone       chan error                        // waitDone delivers the managed process exit result exactly once so readiness waits and shutdown can observe early process death.
	exited         bool                              // exited records whether the currently tracked managed process has already finished so duplicate starts can be avoided safely.
	waitErr        error                             // waitErr stores the most recent managed process exit result for diagnostics after the process exits.
	stdout         bytes.Buffer                      // stdout captures managed process stdout for richer startup diagnostics when the child process exits before becoming ready.
	stderr         bytes.Buffer                      // stderr captures managed process stderr for richer startup diagnostics when the child process exits before becoming ready.
	lookPath       func(string) (string, error)      // lookPath resolves the Appium executable path so tests can inject a helper binary without relying on a real Appium installation.
	commandFactory func(string, ...string) *exec.Cmd // commandFactory constructs the managed child process so tests can replace the real Appium executable with a deterministic helper.
}

// NewLocalAppiumSupervisor constructs one supervisor bound to one configured Appium base URL.
func NewLocalAppiumSupervisor(rawURL string) *LocalAppiumSupervisor {
	return &LocalAppiumSupervisor{ // Return one fully initialized supervisor so later EnsureReady and Stop calls never need nil factory checks.
		rawURL:         rawURL,        // Keep the configured Appium base URL so every readiness check and managed process start targets the same endpoint.
		lookPath:       exec.LookPath, // Use the standard executable lookup path resolution for the default production Appium binary.
		commandFactory: exec.Command,  // Use detached child-process construction so the spawned Appium service is not tied to one request context lifetime.
	} // Return the ready-to-use supervisor instance to the caller.
}

// EnsureReady verifies that the configured Appium base URL is reachable and auto-starts one local loopback Appium process when that URL is locally managed and currently unavailable.
func (s *LocalAppiumSupervisor) EnsureReady(ctx context.Context) error {
	if s == nil { // Reject nil supervisors because no reachability or auto-start work can be performed without one concrete supervisor instance.
		return errors.New(errors.CodeConfigInvalid, "local appium supervisor is nil") // Surface nil-supervisor misuse as a stable config error instead of panicking on the receiver.
	}
	firstErr := CheckAppiumReachability(ctx, s.rawURL) // Probe the configured Appium endpoint first so already-running servers avoid any process-management path entirely.
	if firstErr == nil {                               // Stop early when the configured Appium endpoint is already reachable.
		return nil // Return success immediately because no auto-start or retry work is needed when Appium is already healthy.
	}
	parsedURL, err := parseHTTPURL(s.rawURL, "worker.appium_url") // Parse the configured URL once so auto-start can inspect the host, scheme, port, and optional base path safely.
	if err != nil {                                               // Stop when the configured Appium URL itself is malformed because no managed process command can be derived safely.
		return err // Preserve the URL-validation failure because that is the actual root cause.
	}
	if !isManagedLocalAppiumURL(parsedURL) { // Skip local process management entirely for non-loopback or HTTPS URLs because this repository should not spawn remote Appium servers.
		return firstErr // Return the original reachability error so callers still see the real remote dependency failure.
	}
	if !IsLocalAppiumMissingServerError(firstErr) { // Skip local auto-start when the loopback endpoint is present but unhealthy because the requested behavior only covers missing local Appium servers.
		return firstErr // Return the original unhealthy-endpoint failure so callers see the real Appium dependency problem.
	}

	s.mu.Lock()                                            // Serialize local process startup so concurrent EnsureReady callers do not race to spawn duplicate Appium processes.
	recheckedErr := CheckAppiumReachability(ctx, s.rawURL) // Re-check under the lock so a peer caller that just finished starting Appium can short-circuit this caller cleanly.
	if recheckedErr == nil {                               // Stop under the lock when another concurrent caller already made Appium reachable.
		s.mu.Unlock() // Release the supervisor lock before returning because no local process state changes are required now.
		return nil    // Return success immediately because the endpoint became healthy while this caller waited for the lock.
	}
	if !IsLocalAppiumMissingServerError(recheckedErr) { // Skip local auto-start when the lock-held recheck proves the loopback endpoint is unhealthy rather than absent.
		s.mu.Unlock()       // Release the supervisor lock before returning because no child-process start should happen for non-missing-server failures.
		return recheckedErr // Return the latest unhealthy-endpoint failure so callers see the current dependency state precisely.
	}
	if !s.processRunningLocked() { // Start one new managed Appium process only when no currently tracked managed process is still alive.
		if err := s.startLocked(parsedURL); err != nil { // Attempt one managed local Appium start for the configured loopback URL.
			s.mu.Unlock() // Release the supervisor lock before returning the start failure to the caller.
			return err    // Preserve the precise auto-start failure so user-facing callers can surface it directly.
		}
	}
	waitDone := s.waitDone // Snapshot the managed process wait channel once so the readiness wait can observe early child-process exit without holding the supervisor lock.
	s.mu.Unlock()          // Release the supervisor lock before polling /status so other callers and shutdown can progress concurrently.

	if err := s.waitUntilReady(ctx, waitDone); err != nil { // Wait for the managed process to expose a healthy /status endpoint or fail fast on child-process exit or timeout.
		return err // Preserve the readiness-wait failure because callers need the concrete child-process or timeout error.
	}

	return nil // Return success once the configured Appium endpoint is reachable, whether it was already running or auto-started locally.
}

// Stop terminates the managed local Appium process when this supervisor started one and waits for that process to exit.
func (s *LocalAppiumSupervisor) Stop() error {
	if s == nil { // Treat nil supervisors as already stopped because there is no child process state to tear down.
		return nil // Return success because nil-supervisor shutdown is effectively a no-op.
	}

	s.mu.Lock()                 // Serialize managed process shutdown so no concurrent EnsureReady call can race a stop with a start on the same supervisor.
	cmd := s.cmd                // Snapshot the currently tracked managed process pointer once so shutdown can operate after releasing the supervisor lock.
	waitDone := s.waitDone      // Snapshot the currently tracked wait channel once so shutdown can wait for child-process exit after releasing the supervisor lock.
	if cmd == nil || s.exited { // Treat missing or already-exited managed processes as already stopped because there is nothing left to terminate.
		s.cmd = nil      // Clear any stale process pointer so later EnsureReady calls can start one fresh managed process if needed.
		s.waitDone = nil // Clear any stale wait channel so later starts can publish one new child-process lifecycle channel safely.
		s.exited = true  // Mark the tracked lifecycle as exited so later processRunningLocked checks stay consistent.
		s.waitErr = nil  // Clear any stale wait error because the supervisor no longer owns one active process lifecycle.
		s.mu.Unlock()    // Release the supervisor lock before returning because shutdown is already complete.
		return nil       // Return success because no live managed Appium process remains.
	}
	s.cmd = nil      // Clear the tracked process pointer before shutdown waits so later EnsureReady calls can safely start one new process after Stop finishes.
	s.waitDone = nil // Clear the tracked wait channel before shutdown waits so later starts do not reuse the old process lifecycle channel accidentally.
	s.exited = true  // Mark the tracked lifecycle as exiting so processRunningLocked immediately reports the process as unavailable to future callers.
	s.waitErr = nil  // Clear the tracked wait error because shutdown is about to observe one fresh child-process exit result.
	s.mu.Unlock()    // Release the supervisor lock before sending signals and waiting so other callers are not blocked behind the child-process exit path.

	_ = cmd.Process.Signal(os.Interrupt) // Ask the managed Appium process to stop gracefully first so it can flush logs and release sockets cleanly.

	select { // Wait briefly for graceful child-process exit before falling back to a hard kill.
	case <-waitDone: // Stop waiting once the managed Appium process has already exited after the graceful signal.
		return nil // Return success because the managed Appium process stopped gracefully.
	case <-time.After(localAppiumShutdownTimeout): // Fall back after the graceful shutdown budget expires so shutdown cannot hang forever on one child process.
		_ = cmd.Process.Kill() // Force-kill the managed Appium process so shutdown still completes deterministically when graceful exit does not happen.
		<-waitDone             // Wait for the forced child-process exit so no zombie process remains after shutdown returns.
		return nil             // Return success because the managed Appium process has now exited after the forced kill.
	}
}

// processRunningLocked reports whether the supervisor currently tracks one managed Appium process that has not yet exited.
func (s *LocalAppiumSupervisor) processRunningLocked() bool {
	return s.cmd != nil && !s.exited // Report one process as running only when the supervisor still owns a process pointer and has not observed its exit.
}

// startLocked starts one managed local Appium process for the configured loopback URL while the supervisor mutex is held.
func (s *LocalAppiumSupervisor) startLocked(parsedURL *url.URL) error {
	executablePath, err := s.lookPath(localAppiumExecutableName) // Resolve the Appium executable path once so startup fails fast when Appium is not installed locally.
	if err != nil {                                              // Stop immediately when no local Appium executable can be found in PATH.
		return errors.Wrap(errors.CodeHealthDown, "failed to find local appium executable", err) // Surface the missing binary as a dependency-health failure because the local automation dependency cannot be started.
	}
	commandArgs, err := localAppiumCommandArgs(parsedURL) // Build the Appium CLI arguments from the configured loopback URL once so the managed process binds the expected address, port, and base path.
	if err != nil {                                       // Stop immediately when the loopback URL cannot be translated into one concrete local Appium CLI command safely.
		return err // Preserve the precise command-construction failure for the caller.
	}
	s.stdout.Reset()                                        // Clear any previously captured managed-process stdout so fresh diagnostics belong only to the new child process attempt.
	s.stderr.Reset()                                        // Clear any previously captured managed-process stderr so fresh diagnostics belong only to the new child process attempt.
	cmd := s.commandFactory(executablePath, commandArgs...) // Construct one detached Appium child process that will outlive the current request context if startup succeeds.
	cmd.Stdout = &s.stdout                                  // Capture managed-process stdout so later early-exit failures can report the emitted output to the caller.
	cmd.Stderr = &s.stderr                                  // Capture managed-process stderr so later early-exit failures can report the emitted output to the caller.
	if err := cmd.Start(); err != nil {                     // Start the managed Appium child process immediately so the supervisor can begin polling /status for readiness.
		return errors.Wrap(errors.CodeHealthDown, "failed to start local appium service", err) // Surface the child-process spawn failure directly because no Appium endpoint can come up without it.
	}
	waitDone := make(chan error, 1)                    // Allocate one buffered wait-result channel so the wait goroutine can publish child-process exit without blocking.
	go func(startedCmd *exec.Cmd, done chan<- error) { // Wait for the managed Appium child process in the background so readiness polling and shutdown can observe early exit.
		waitErr := startedCmd.Wait() // Block until the child process exits so the supervisor can capture its final exit result exactly once.
		s.mu.Lock()                  // Serialize supervisor state updates with concurrent EnsureReady and Stop calls before publishing the exit result.
		if s.cmd == startedCmd {     // Update the tracked lifecycle fields only when the exited child process is still the supervisor's current managed process.
			s.exited = true     // Mark the currently tracked managed process as exited so future EnsureReady calls can spawn one replacement if needed.
			s.waitErr = waitErr // Preserve the exact child-process exit result for later diagnostics when readiness never succeeded.
		}
		s.mu.Unlock()   // Release the supervisor lock before publishing the exit result on the wait channel.
		done <- waitErr // Publish the child-process exit result so readiness waits and shutdown can react promptly.
		close(done)     // Close the wait channel so future receivers can observe completion without blocking.
	}(cmd, waitDone) // Bind the wait goroutine to the child process and wait channel created for this specific start attempt.
	s.cmd = cmd           // Track the newly started child process so later EnsureReady and Stop calls can manage the same process lifecycle.
	s.waitDone = waitDone // Track the child-process wait channel so readiness waits and shutdown can observe early process exit.
	s.exited = false      // Mark the newly started managed process as live until the wait goroutine observes one exit.
	s.waitErr = nil       // Clear any previous wait error so diagnostics now describe only the current managed process lifecycle.

	return nil // Return success once the child process has been started and its lifecycle is being tracked by the supervisor.
}

// waitUntilReady polls the configured Appium status endpoint until it becomes reachable or the managed process exits first.
func (s *LocalAppiumSupervisor) waitUntilReady(ctx context.Context, waitDone <-chan error) error {
	if err := CheckAppiumReachability(ctx, s.rawURL); err == nil { // Short-circuit the wait when the managed process became reachable before the polling loop began.
		return nil // Return success immediately because the Appium endpoint is already healthy.
	}

	ticker := time.NewTicker(localAppiumReadyPollEvery) // Poll the Appium status endpoint on one short interval so startup completes promptly once the child process is ready.
	defer ticker.Stop()                                 // Release the ticker resources immediately after readiness wait completes.

	for { // Continue polling until Appium becomes reachable, the child process exits, or the caller's context expires.
		select {
		case <-ctx.Done(): // Stop waiting when the caller's timeout or cancellation fires before the Appium endpoint becomes reachable.
			return errors.Wrap(errors.CodeHealthDown, "local appium service did not become ready", ctx.Err()) // Surface the caller timeout or cancellation as the reason readiness did not succeed.
		case waitErr := <-waitDone: // Stop waiting early when the managed child process exits before the Appium status endpoint becomes healthy.
			return s.childProcessExitError(waitErr) // Surface the child-process exit together with captured output because startup cannot succeed after an early process death.
		case <-ticker.C: // Re-check Appium reachability on every polling tick while the managed process is still booting.
			if err := CheckAppiumReachability(ctx, s.rawURL); err == nil { // Stop polling once the managed Appium endpoint responds successfully on /status.
				return nil // Return success because the Appium endpoint is now healthy.
			}
		}
	}
}

// childProcessExitError builds one stable readiness error that includes the managed Appium process exit result and any captured output.
func (s *LocalAppiumSupervisor) childProcessExitError(waitErr error) error {
	s.mu.Lock()                                                // Serialize access to the captured process output buffers so diagnostics read one consistent snapshot after process exit.
	stdoutText := trimCapturedProcessOutput(s.stdout.String()) // Snapshot and trim managed-process stdout so error messages stay concise while still preserving useful diagnostics.
	stderrText := trimCapturedProcessOutput(s.stderr.String()) // Snapshot and trim managed-process stderr so error messages stay concise while still preserving useful diagnostics.
	s.mu.Unlock()                                              // Release the supervisor lock after the captured process output snapshot has been taken.

	message := "local appium service exited before becoming ready" // Start from one stable base message so callers can classify early child-process death consistently.
	if stderrText != "" {                                          // Prefer surfaced stderr because Appium startup failures usually explain themselves there most clearly.
		message = fmt.Sprintf("%s (stderr: %s)", message, stderrText) // Append trimmed stderr so the user sees the actual startup failure reason directly.
	} else if stdoutText != "" { // Fall back to stdout only when stderr is empty but Appium still wrote potentially useful diagnostics there.
		message = fmt.Sprintf("%s (stdout: %s)", message, stdoutText) // Append trimmed stdout so the user still sees the most relevant emitted startup diagnostics.
	}
	if waitErr == nil { // Defend against nil child-process wait results even though os/exec normally reports one exit status when a process dies early.
		return errors.New(errors.CodeHealthDown, message) // Return one stable dependency-health error even without a concrete child-process wait result.
	}

	return errors.Wrap(errors.CodeHealthDown, message, waitErr) // Return the stable dependency-health error while preserving the child-process exit result for debugging.
}

// localAppiumCommandArgs derives one concrete Appium CLI command line from one locally managed loopback Appium URL.
func localAppiumCommandArgs(parsedURL *url.URL) ([]string, error) {
	host := strings.TrimSpace(parsedURL.Hostname()) // Extract and normalize the host portion so the managed Appium process binds the exact configured loopback interface.
	if host == "" {                                 // Reject empty hosts because no deterministic bind address can be derived without one.
		return nil, errors.New(errors.CodeConfigInvalid, "worker.appium_url must include a host for local auto-start") // Surface the missing host as a config problem because the CLI command cannot be built safely.
	}
	port := strings.TrimSpace(parsedURL.Port()) // Extract and normalize the port portion so the managed Appium process binds the exact configured loopback port.
	if port == "" {                             // Reject missing ports because the managed Appium process must know exactly which local port to bind.
		return nil, errors.New(errors.CodeConfigInvalid, "worker.appium_url must include a port for local auto-start") // Surface the missing port as a config problem because the CLI command cannot be built safely.
	}
	args := []string{"--address", host, "--port", port} // Start from the explicit bind host and port so the managed Appium process matches the configured base URL exactly.
	basePath := strings.TrimSpace(parsedURL.Path)       // Extract the optional base path so loopback URLs such as /wd/hub can map to matching Appium CLI flags.
	if basePath != "" && basePath != "/" {              // Append one base-path flag only when the configured Appium URL actually contains a non-root path component.
		args = append(args, "--base-path", basePath) // Preserve the configured non-root Appium base path so status and session calls stay path-compatible after auto-start.
	}

	return args, nil // Return the fully constructed Appium CLI argument slice for one locally managed loopback URL.
}

// isManagedLocalAppiumURL reports whether the configured Appium URL is one loopback HTTP endpoint that this repository is allowed to auto-start locally.
func isManagedLocalAppiumURL(parsedURL *url.URL) bool {
	if parsedURL == nil { // Reject nil parsed URLs because no local-management decision can be made without one concrete URL value.
		return false // Report non-manageable so callers keep the original reachability error instead of panicking.
	}
	if parsedURL.Scheme != "http" { // Restrict local auto-start to plaintext loopback HTTP URLs because the managed Appium CLI does not configure TLS material here.
		return false // Report non-manageable so HTTPS or other schemes remain externally managed dependencies.
	}
	host := strings.TrimSpace(parsedURL.Hostname()) // Normalize the host string before checking whether it identifies one loopback interface.
	if host == "localhost" {                        // Treat localhost as one locally managed loopback address because it resolves to the current machine.
		return true // Report manageable so the supervisor may spawn one local Appium child process for this URL.
	}
	ip := net.ParseIP(host) // Parse the host as one literal IP address so numeric loopback interfaces can be recognized too.
	if ip == nil {          // Reject non-IP, non-localhost hosts because they identify external dependencies that should not be auto-started locally.
		return false // Report non-manageable so the repository never spawns Appium for arbitrary hostnames.
	}

	return ip.IsLoopback() // Report manageable only for literal loopback IPs such as 127.0.0.1 and ::1.
}

// trimCapturedProcessOutput compacts one captured child-process output string into one short single-line diagnostic fragment.
func trimCapturedProcessOutput(output string) string {
	trimmed := strings.TrimSpace(output) // Drop surrounding whitespace so empty or newline-only output collapses cleanly before truncation.
	if trimmed == "" {                   // Short-circuit empty captured output because no diagnostic text should be appended in that case.
		return "" // Return the empty string so callers can skip the related stdout or stderr fragment entirely.
	}
	singleLine := strings.Join(strings.Fields(trimmed), " ") // Collapse all whitespace runs into single spaces so the final diagnostic fits cleanly in one returned error string.
	if len(singleLine) <= 200 {                              // Keep short output fragments intact because they already fit comfortably in one error message.
		return singleLine // Return the compacted single-line output without truncation.
	}

	return singleLine[:200] + "..." // Truncate very long output fragments so readiness errors stay readable while still surfacing the leading diagnostic detail.
}

// IsLocalAppiumMissingServerError reports whether one Appium reachability failure indicates that no local server is currently listening on the configured loopback port.
func IsLocalAppiumMissingServerError(err error) bool {
	if err == nil { // Treat nil errors as non-missing-server because there is no failure to classify in that case.
		return false // Report non-missing-server so EnsureReady never auto-starts after one successful probe.
	}
	normalizedMessage := strings.ToLower(err.Error())              // Normalize the failure text once so platform-specific connection-refused fragments can be matched case-insensitively.
	if strings.Contains(normalizedMessage, "connection refused") { // Treat explicit connection-refused errors as the canonical signal that no local Appium server is listening.
		return true // Report one missing-server condition so the supervisor may auto-start a local Appium process.
	}

	return false // Report all other failure shapes as non-missing-server so unhealthy loopback services are surfaced instead of replaced.
}
