# Tentacle Third-Party Integration Guide

This document describes the supported public integration paths for Tentacle.

## Supported Integration Paths

| Interface | Typical Use Case | Transport |
| --- | --- | --- |
| MCP `stdio` | AI tools, desktop clients, local automation shells | stdio |
| HTTP JSON-RPC | backend services, API clients, CI systems | HTTP |
| WebSocket events | browser dashboards and live trace viewers | HTTP + WebSocket |

The repository also contains internal gRPC services for orchestrator-to-worker communication. Those services are runtime internals and are not part of the public third-party contract.

## 1. MCP `stdio`

Use `gateway --stdio` when your application wants to speak MCP directly.

### Python Example

```python
import json
import subprocess


class MCPClient:
    def __init__(self, gateway_path, config_path):
        self.process = subprocess.Popen(
            [gateway_path, "--stdio", "--config", config_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.request_id = 0

    def call(self, method, params):
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_id,
        }
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        return json.loads(self.process.stdout.readline())

    def close(self):
        self.process.terminate()
        self.process.wait()


client = MCPClient("/absolute/path/to/gateway", "/absolute/path/to/config.yaml")

try:
    print(client.call("initialize", {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "example-client", "version": "1.0.0"},
    }))
    print(client.call("tools/list", {}))
finally:
    client.close()
```

## 2. HTTP JSON-RPC

Start the gateway:

```bash
./gateway --config config.yaml
```

### List the Tool Catalog

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "params": {},
    "id": 1
  }'
```

### Create a Session

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "startSession",
      "arguments": {
        "projectId": "sample-project",
        "w3cCapsJson": {
          "platformName": "Android",
          "appium:automationName": "UiAutomator2",
          "appium:deviceName": "Android Emulator"
        }
      }
    },
    "id": 2
  }'
```

### Run a Plan

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "executePlan",
      "arguments": {
        "sessionId": "sess_123",
        "plan": {
          "steps": [
            {"type": "tap", "params": {"x": 540, "y": 1600}},
            {"type": "swipe", "params": {"startX": 540, "startY": 1600, "endX": 540, "endY": 700, "durationMs": 400}},
            {"type": "screenshot"}
          ]
        }
      }
    },
    "id": 3
  }'
```

## 3. Browser Event Streaming

Browser clients should mint a short-lived WebSocket token first:

```bash
curl -X POST http://localhost:8080/api/ws/traces/trace_123/subscription-token \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json"
```

Then connect to the event stream:

```javascript
const token = "<subscription-token>";
const traceId = "trace_123";
const ws = new WebSocket(
  `ws://localhost:8080/ws/plan-events?subscriptionToken=${encodeURIComponent(token)}&traceId=${encodeURIComponent(traceId)}`
);

ws.onmessage = (event) => {
  console.log("trace event", JSON.parse(event.data));
};
```

## 4. AWS Device Farm

### Scheduled Runs

Use the Device Farm `run_api` tools when you want the gateway to create uploads, schedule test runs, and poll run state.

### Live Remote Sessions

Use `devicefarm.mode=remote_access` when `startSession` should open a live Device Farm remote-access Appium session. The supported `w3cCapsJson` extension keys are:

- `devicefarm:deviceArn`
- `devicefarm:appArn`
- `devicefarm:projectArn`
- `devicefarm:sessionName`

## Authentication

If authentication is enabled, the public gateway can enforce:

- personal access tokens
- OIDC bearer tokens
- HMAC request signing

Use the configuration examples in [config.example.yaml](./config.example.yaml) and [config.production.example.yaml](./config.production.example.yaml) as the starting point for your deployment.
