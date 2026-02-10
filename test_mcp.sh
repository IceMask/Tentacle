#!/bin/bash
# Test MCP protocol via stdio

echo "Testing MCP tools/list..."
echo ""

# Send initialize request first
echo '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}},"id":1}' | ./bin/gateway --stdio --config config.yaml 2>/dev/null | head -1 | jq '.'

echo ""
echo "===================="
echo ""

# Send tools/list request
echo '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":2}' | ./bin/gateway --stdio --config config.yaml 2>/dev/null | head -1 | jq '.result.tools[] | {name: .name, description: .description} | if .description | length > 100 then .description = (.description[:100] + "...") else . end'
