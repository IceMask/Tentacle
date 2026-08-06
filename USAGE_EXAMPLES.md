# Tentacle Usage Examples

This document shows common ways to use Tentacle through MCP `stdio`, MCP Streamable HTTP, and the HTTP JSON-RPC compatibility endpoint.

## Claude Desktop Configuration

```json
{
  "mcpServers": {
    "tentacle": {
      "command": "/absolute/path/to/gateway",
      "args": ["--stdio", "--config", "/absolute/path/to/config.yaml"]
    }
  }
}
```

## Start the Gateway

### MCP `stdio`

```bash
./gateway --stdio --config config.yaml
```

### HTTP JSON-RPC

```bash
./gateway --config config.yaml
```

## Discover MCP 2026-07-28 Over HTTP

```bash
curl -X POST http://localhost:8080/mcp \
  -H "Accept: application/json, text/event-stream" \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2026-07-28" \
  -H "Mcp-Method: server/discover" \
  -d '{
    "jsonrpc": "2.0",
    "method": "server/discover",
    "params": {
      "_meta": {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "example-client", "version": "1.0.0"}
      }
    },
    "id": 1
  }'
```

Modern requests repeat `_meta` and mirror the JSON-RPC method in `Mcp-Method`. Calls to `tools/call` and `resources/read` also include the selected tool name or URI in `Mcp-Name`.

## List Tools Through JSON-RPC Compatibility

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

## Start a Local Appium Session

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

## Tap and Swipe

### Tap

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "tap",
      "arguments": {
        "sessionId": "sess_123",
        "x": 540,
        "y": 1600
      }
    },
    "id": 3
  }'
```

### Swipe

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "swipe",
      "arguments": {
        "sessionId": "sess_123",
        "startX": 540,
        "startY": 1600,
        "endX": 540,
        "endY": 700,
        "durationMs": 400
      }
    },
    "id": 4
  }'
```

## Run a Plan

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
            {"type": "click", "selector": "~login-button"},
            {"type": "sendKeys", "selector": "~username", "params": {"text": "testuser"}},
            {"type": "swipe", "params": {"startX": 540, "startY": 1600, "endX": 540, "endY": 700, "durationMs": 400}},
            {"type": "screenshot"}
          ]
        }
      }
    },
    "id": 5
  }'
```

## Read Trace State

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "getTrace",
      "arguments": {
        "traceId": "trace_123"
      }
    },
    "id": 6
  }'
```

## Take a Screenshot

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "takeScreenshot",
      "arguments": {
        "sessionId": "sess_123",
        "traceId": "trace_123",
        "includeThumb": true
      }
    },
    "id": 7
  }'
```

## Start a Live AWS Device Farm Session

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "startSession",
      "arguments": {
        "projectId": "devicefarm-project",
        "w3cCapsJson": {
          "platformName": "Android",
          "appium:automationName": "UiAutomator2",
          "devicefarm:deviceArn": "arn:aws:devicefarm:us-west-2:123456789012:device:EXAMPLE",
          "devicefarm:projectArn": "arn:aws:devicefarm:us-west-2:123456789012:project:EXAMPLE"
        }
      }
    },
    "id": 8
  }'
```

## Schedule an AWS Device Farm Run

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "scheduleDeviceFarmRun",
      "arguments": {
        "projectId": "devicefarm-project",
        "projectArn": "arn:aws:devicefarm:us-west-2:123456789012:project:EXAMPLE",
        "name": "smoke-suite"
      }
    },
    "id": 9
  }'
```

## Health Check

```bash
curl -X POST http://localhost:8080/jsonrpc \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "healthCheck",
      "arguments": {}
    },
    "id": 10
  }'
```
