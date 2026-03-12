# Validation Checklist

## Dispatcher and queue safety

- No-worker path returns retriable error.
- Message is not ACKed on transient scheduling failures.
- Retry exhaustion path is bounded and explicit.

## Execution mode

- `execution_mode` exists in config and defaults to `monolith`.
- Gateway fails fast if unsupported distributed path is selected.
- Standalone orchestrator forces/uses distributed mode.

## Cancel propagation

- Cancel sets queue cancellation marker.
- Distributed mode attempts worker `CancelPlan` RPC.
- In-flight assignment key is cleaned after cancel response.

## Session recovery

- Start session persists Appium session mapping in Redis.
- End session removes mapping.
- On in-memory miss, orchestrator rebuilds Appium client from mapping.

## Test gates

- Compile gate for all packages passes.
- Core package tests pass.
- Any skipped/blocked tests are documented with reason.
