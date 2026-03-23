package rpc

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// init executes this operation.
func init() {
	encoding.RegisterCodec(jsonCodec{})
}

// jsonCodec is a gRPC codec that uses JSON for marshaling.
// Registered under the name "json" so that clients can select it via
// grpc.CallContentSubtype("json") and servers will pick it up automatically.
type jsonCodec struct{}

// Marshal executes this operation.
func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal executes this operation.
func (jsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// Name executes this operation.
func (jsonCodec) Name() string {
	return "json"
}
