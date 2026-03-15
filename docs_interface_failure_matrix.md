# Interface Failure Matrix

## Unified Error Payload

All `tools/call` failures now return structured `error.data` fields:

- `interface`: fixed as `tools/call`
- `tool`: tool name
- `rawError`: original downstream/native error string
- `internalCode`: parsed internal code like `E.STORE.CONN` when available
- `hint`: actionable suggestion based on known failure patterns

Legacy JSON-RPC methods now return structured `error.data` fields:

- `internalCode`
- `rawError`

REST endpoints now return structured `error` fields:

- `message`
- `internalCode`
- `rawError`
- `hint`

## MCP Protocol Methods

### `initialize`
- Invalid JSON params
- Missing/invalid fields in initialize payload

### `tools/list`
- Tool registry initialization panic/misconfiguration (startup-time)

### `tools/call`
- Invalid tool name
- Schema validation failure
- Downstream execution failures (Appium, DB, Redis, AWS, network, auth)

### `resources/list`
- Static method, normally no runtime failure

### `resources/read`
- Invalid URI format
- Unknown resource type
- Trace/session/artifact not found
- DAO/storage read failures

## Tool Interfaces (MCP)

### Session & Plan

#### `startSession`
- `worker.appium_url` empty
- Appium unavailable / session create failure
- Invalid capabilities for driver/platform
- DB write failure creating session
- Redis mapping write failure (non-fatal warning in server logs)

#### `executePlan`
- Session not found / session ended
- Plan validation/parse failure
- Trace creation/write failure
- Dispatch enqueue failure / no worker available

#### `cancelPlan`
- Trace not found
- In-memory cancellation handle missing
- Worker cancel propagation failure

#### `endSession`
- Session not found
- Appium delete session failure
- DB session end write failure
- Redis mapping delete failure (warning path)

#### `getTrace`
- Trace not found
- Event read failure

#### `getSemanticSnapshot`
- Session not found
- Appium tree/source fetch failure
- Snapshot revision mismatch/cache issues

#### `takeScreenshot`
- Session not found
- Appium screenshot failure
- Artifact storage/persist failure

#### `healthCheck`
- Degraded/down states when DB/Redis checks fail

### Element Operations

#### `findElement`
- Invalid locator strategy/selector
- Appium element lookup timeout/not found
- Session not found

#### `clickElement`
- Invalid element id
- Element stale/not interactable
- Session not found

#### `sendKeysToElement`
- Invalid element id / text
- Input action failure in Appium
- Session not found

#### `clearElement`
- Invalid element id
- Clear action failure in Appium
- Session not found

#### `getElementText`
- Invalid element id
- Element read failure in Appium
- Session not found

#### `getElementAttribute`
- Invalid element id/attribute
- Attribute read failure in Appium
- Session not found

#### `isElementDisplayed`
- Invalid element id
- Visibility check failure in Appium
- Session not found

### Gesture Operations

#### `tap`
- Invalid coordinates
- Gesture dispatch failure
- Session not found

#### `swipe`
- Invalid coordinates/duration
- Gesture dispatch failure
- Session not found

#### `longPress`
- Invalid element id/duration
- Gesture dispatch failure
- Session not found

#### `pressBack`
- Unsupported on current platform/driver
- Session not found

#### `hideKeyboard`
- Keyboard not present / unsupported action
- Session not found

### Device Farm

#### `createDeviceFarmUpload`
- `devicefarm.mode` not `run_api`
- AWS config/client init missing
- Project ARN missing
- Upload type/name invalid
- AWS API failure / network or DNS failure

#### `getDeviceFarmUpload`
- Upload ARN missing/invalid
- AWS API failure / network or DNS failure
- Upload not found

#### `getDeviceFarmRuntimeContext`
- `devicefarm.mode` not `run_api`
- Project ARN missing (request + config empty)
- Redis read/cache unavailable

#### `scheduleDeviceFarmRun`
- `devicefarm.mode` not `run_api`
- Device Farm client not initialized
- Missing effective `appArn`/`testPackageArn` (request+cache)
- Device pool missing and auto-resolve failed
- AWS API validation failure
- AWS network/DNS failure

#### `getDeviceFarmRun`
- Run ARN missing/invalid
- Run not found
- AWS API/network/DNS failure

### Local ADB

#### `adbShell`
- Empty command
- Command not in whitelist
- `adb` not installed/in PATH
- Device serial offline/not found
- Command execution failure/non-zero exit

## Legacy JSON-RPC Methods

Legacy direct methods (`startSession`, `executePlan`, `endSession`, `getTrace`, etc.) share the same downstream failure classes as their MCP tool equivalents and now return structured JSON-RPC `error.data` with `internalCode` and `rawError`.

## REST Endpoints

REST interfaces in `internal/gateway/rest/router.go` now return a JSON body with:

- `error.message`
- `error.internalCode`
- `error.rawError`
- `error.hint`

This applies to session/plan/trace/artifact/capabilities and idempotency error paths.
