// adb_shell.go defines the restrictive adb shell command policy used by the orchestrator service.
package orchestrator

import (
	"os/exec"
	"regexp"
	"strings"

	"mcp_for_appium/internal/errors"
)

// adbCommandContext allows tests to replace subprocess creation while production keeps using exec.CommandContext.
var adbCommandContext = exec.CommandContext

// adbSimpleTokenPattern matches conservative read-only adb shell tokens such as package names, namespaces, flags, and service names.
var adbSimpleTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._:@/-]+$`) // Compile the conservative token matcher once so every validation call reuses the same regex.

// adbFilterTokenPattern matches conservative logcat filter specs such as ActivityManager:I or *:W.
var adbFilterTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_*.:/-]+$`) // Compile the logcat filter matcher once so every validation call reuses the same regex.

// validateADBCommand normalizes one requested adb shell command and rejects unsafe command families, subcommands, or parameters.
func validateADBCommand(command []string) ([]string, error) {
	if len(command) == 0 { // Reject empty command slices before any normalization or policy checks begin.
		return nil, errors.New(errors.CodePlanInvalid, "adb command must not be empty") // Surface the missing command body through the stable invalid-plan contract.
	}

	normalized := make([]string, 0, len(command)) // Allocate the destination slice used to store trimmed command tokens for later subprocess execution.
	for _, token := range command {               // Normalize every supplied token so whitespace-only bypass attempts are rejected before policy validation.
		trimmed := strings.TrimSpace(token) // Trim surrounding whitespace so policy checks run against the actual intended token value.
		if trimmed == "" {                  // Reject empty tokens because they make the command ambiguous and can hide malformed requests.
			return nil, errors.New(errors.CodePlanInvalid, "adb command tokens must not be empty") // Surface malformed tokenization through the stable invalid-plan contract.
		}
		normalized = append(normalized, trimmed) // Preserve the normalized token for later policy validation and subprocess execution.
	}

	switch normalized[0] { // Dispatch to the command-family-specific validator so each family can enforce its own safe subcommand contract.
	case "getprop":
		return normalized, validateADBGetprop(normalized) // Validate one read-only getprop request and return the normalized token list when it passes.
	case "dumpsys":
		return normalized, validateADBDumpsys(normalized) // Validate one read-only dumpsys request and return the normalized token list when it passes.
	case "pm":
		return normalized, validateADBPM(normalized) // Validate one restricted package-manager request and return the normalized token list when it passes.
	case "settings":
		return normalized, validateADBSettings(normalized) // Validate one read-only settings request and return the normalized token list when it passes.
	case "logcat":
		return normalized, validateADBLogcat(normalized) // Validate one bounded logcat dump request and return the normalized token list when it passes.
	case "wm":
		return normalized, validateADBWM(normalized) // Validate one read-only window-manager request and return the normalized token list when it passes.
	case "ime":
		return normalized, validateADBIME(normalized) // Validate one read-only input-method request and return the normalized token list when it passes.
	default:
		return nil, errors.New(errors.CodePermissionDenied, "adb command is not in allowed whitelist") // Reject every non-whitelisted command family through the stable permission-denied contract.
	}
}

// validateADBGetprop validates one read-only getprop request.
func validateADBGetprop(command []string) error {
	if len(command) > 2 { // Reject extra tokens because getprop should accept at most one property key in this restricted policy.
		return errors.New(errors.CodePermissionDenied, "getprop accepts at most one property key") // Surface the rejected parameter shape through the stable permission-denied contract.
	}
	if len(command) == 2 && !adbSimpleTokenPattern.MatchString(command[1]) { // Reject unsafe property keys containing shell metacharacters or whitespace.
		return errors.New(errors.CodePermissionDenied, "getprop property key is not allowed") // Surface the unsafe property token through the stable permission-denied contract.
	}
	return nil // Accept the read-only getprop request because it satisfies the conservative token policy.
}

// validateADBDumpsys validates one bounded read-only dumpsys request.
func validateADBDumpsys(command []string) error {
	if len(command) > 4 { // Reject unusually long dumpsys commands because they expand the review surface without clear first-version value.
		return errors.New(errors.CodePermissionDenied, "dumpsys accepts at most three arguments") // Surface the rejected parameter shape through the stable permission-denied contract.
	}
	for _, token := range command[1:] { // Validate every optional dumpsys token against the conservative safe-token matcher.
		if !adbSimpleTokenPattern.MatchString(token) { // Reject unsafe dumpsys tokens containing shell metacharacters or whitespace.
			return errors.New(errors.CodePermissionDenied, "dumpsys argument is not allowed") // Surface the unsafe token through the stable permission-denied contract.
		}
	}
	return nil // Accept the read-only dumpsys request because every optional token satisfied the conservative matcher.
}

// validateADBPM validates one heavily restricted read-only package-manager request.
func validateADBPM(command []string) error {
	if len(command) < 2 { // Reject incomplete pm invocations because every accepted request must name one explicit subcommand.
		return errors.New(errors.CodePermissionDenied, "pm subcommand is required") // Surface the missing subcommand through the stable permission-denied contract.
	}

	switch command[1] { // Apply one read-only policy per supported package-manager subcommand.
	case "list":
		if len(command) < 3 || command[2] != "packages" { // Restrict list to package enumeration only because other pm list targets broaden the surface unnecessarily.
			return errors.New(errors.CodePermissionDenied, "pm list only supports packages") // Surface the restricted list target through the stable permission-denied contract.
		}
		allowedFlags := map[string]bool{"-3": true, "-s": true, "-d": true, "-e": true, "-u": true} // Allow a small set of read-only package-list flags that do not mutate device state.
		for _, token := range command[3:] {                                                         // Validate the remaining package-list flags or optional package filter.
			if strings.HasPrefix(token, "-") { // Treat dash-prefixed tokens as flags that must be in the explicit allowlist.
				if !allowedFlags[token] { // Reject unsupported flags because they have not been reviewed for safety.
					return errors.New(errors.CodePermissionDenied, "pm list flag is not allowed") // Surface the rejected flag through the stable permission-denied contract.
				}
				continue // Continue once the current flag has been accepted by the explicit allowlist.
			}
			if !adbSimpleTokenPattern.MatchString(token) { // Reject unsafe package filters containing shell metacharacters or whitespace.
				return errors.New(errors.CodePermissionDenied, "pm list package filter is not allowed") // Surface the unsafe package filter through the stable permission-denied contract.
			}
		}
		return nil // Accept the restricted package-list request because every token satisfied the explicit read-only policy.
	case "path", "dump":
		if len(command) != 3 || !adbSimpleTokenPattern.MatchString(command[2]) { // Restrict path and dump to exactly one safe package name argument.
			return errors.New(errors.CodePermissionDenied, "pm subcommand requires one safe package name") // Surface the rejected parameter shape through the stable permission-denied contract.
		}
		return nil // Accept the read-only pm path or pm dump request because it satisfies the exact package-name contract.
	default:
		return errors.New(errors.CodePermissionDenied, "pm subcommand is not allowed") // Reject every unreviewed pm subcommand such as install, uninstall, clear, or grant.
	}
}

// validateADBSettings validates one read-only settings request.
func validateADBSettings(command []string) error {
	if len(command) < 3 { // Reject incomplete settings invocations because every accepted request must name one read-only subcommand and namespace.
		return errors.New(errors.CodePermissionDenied, "settings command requires a read-only subcommand and namespace") // Surface the missing subcommand or namespace through the stable permission-denied contract.
	}

	namespace := command[2]                                                      // Read the requested namespace so only system, secure, and global remain accessible.
	if namespace != "system" && namespace != "secure" && namespace != "global" { // Reject unsupported namespaces because they are outside the reviewed settings surface.
		return errors.New(errors.CodePermissionDenied, "settings namespace is not allowed") // Surface the rejected namespace through the stable permission-denied contract.
	}

	switch command[1] { // Apply one read-only policy per supported settings subcommand.
	case "get":
		if len(command) != 4 || !adbSimpleTokenPattern.MatchString(command[3]) { // Restrict settings get to exactly one safe key inside one allowed namespace.
			return errors.New(errors.CodePermissionDenied, "settings get requires one safe key") // Surface the rejected parameter shape through the stable permission-denied contract.
		}
		return nil // Accept the read-only settings get request because it satisfies the exact namespace and key contract.
	case "list":
		if len(command) != 3 { // Restrict settings list to exactly one namespace without extra tokens.
			return errors.New(errors.CodePermissionDenied, "settings list accepts only one namespace") // Surface the rejected parameter shape through the stable permission-denied contract.
		}
		return nil // Accept the read-only settings list request because it satisfies the exact namespace contract.
	default:
		return errors.New(errors.CodePermissionDenied, "settings subcommand is not allowed") // Reject mutating subcommands such as put, delete, or reset.
	}
}

// validateADBLogcat validates one bounded logcat dump request.
func validateADBLogcat(command []string) error {
	if len(command) == 1 { // Reject bare logcat because it would start one unbounded stream rather than one finite dump.
		return errors.New(errors.CodePermissionDenied, "logcat requires explicit dump flags") // Surface the missing dump flag through the stable permission-denied contract.
	}

	sawDumpFlag := false                // Track whether the request includes -d so the command remains one bounded dump instead of one live stream.
	for i := 1; i < len(command); i++ { // Walk the remaining logcat tokens while handling flags that consume one following value.
		token := command[i] // Read the current token so it can be classified as one flag, one flag value, or one filter spec.
		switch token {      // Apply one explicit policy per supported flag.
		case "-d":
			sawDumpFlag = true // Record that the request is one bounded dump, which is required for the command to remain safe and finite.
		case "-t", "-v", "-b":
			if i+1 >= len(command) { // Reject missing flag values because the logcat request would be malformed and ambiguous.
				return errors.New(errors.CodePermissionDenied, "logcat flag requires a value") // Surface the malformed flag usage through the stable permission-denied contract.
			}
			i++                                                 // Consume the following token because it belongs to the current flag value.
			if !adbFilterTokenPattern.MatchString(command[i]) { // Reject unsafe logcat flag values containing shell metacharacters or whitespace.
				return errors.New(errors.CodePermissionDenied, "logcat flag value is not allowed") // Surface the unsafe flag value through the stable permission-denied contract.
			}
		default:
			if !adbFilterTokenPattern.MatchString(token) { // Reject unsafe filter specs containing shell metacharacters or whitespace.
				return errors.New(errors.CodePermissionDenied, "logcat filter is not allowed") // Surface the unsafe filter token through the stable permission-denied contract.
			}
		}
	}
	if !sawDumpFlag { // Reject streaming logcat requests because they can hang indefinitely and exceed the intended debug-only surface.
		return errors.New(errors.CodePermissionDenied, "logcat requires -d") // Surface the missing bounded-dump flag through the stable permission-denied contract.
	}
	return nil // Accept the bounded logcat dump request because every token satisfied the conservative reviewed policy.
}

// validateADBWM validates one read-only window-manager request.
func validateADBWM(command []string) error {
	if len(command) != 2 { // Reject extra parameters because the read-only wm policy supports only fixed inspection subcommands.
		return errors.New(errors.CodePermissionDenied, "wm accepts exactly one read-only subcommand") // Surface the rejected parameter shape through the stable permission-denied contract.
	}
	if command[1] != "size" && command[1] != "density" { // Reject mutating wm subcommands such as dismiss-keyguard, scaling, or rotation changes.
		return errors.New(errors.CodePermissionDenied, "wm subcommand is not allowed") // Surface the rejected subcommand through the stable permission-denied contract.
	}
	return nil // Accept the read-only wm request because it satisfies the exact subcommand contract.
}

// validateADBIME validates one read-only input-method request.
func validateADBIME(command []string) error {
	if len(command) < 2 || command[1] != "list" { // Restrict ime usage to list only because enable, disable, set, and reset mutate device state.
		return errors.New(errors.CodePermissionDenied, "ime only supports list") // Surface the rejected subcommand through the stable permission-denied contract.
	}
	if len(command) > 3 { // Reject extra parameters because the reviewed ime surface supports at most one optional list flag.
		return errors.New(errors.CodePermissionDenied, "ime list accepts at most one flag") // Surface the rejected parameter shape through the stable permission-denied contract.
	}
	if len(command) == 3 && command[2] != "-a" && command[2] != "-s" { // Restrict the optional flag to the two standard read-only list variants.
		return errors.New(errors.CodePermissionDenied, "ime list flag is not allowed") // Surface the rejected flag through the stable permission-denied contract.
	}
	return nil // Accept the read-only ime list request because it satisfies the exact reviewed contract.
}
