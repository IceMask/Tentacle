package jsonrpc

import (
	"embed"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"mcp_for_appium/internal/errors"

	"github.com/xeipuuv/gojsonschema"
)

//go:embed schemas/*.json
var schemasFS embed.FS

type Validator struct {
	once    sync.Once
	schemas map[string]*gojsonschema.Schema
	loadErr error
}

// NewValidator executes this operation.
func NewValidator() *Validator {
	return &Validator{
		schemas: make(map[string]*gojsonschema.Schema),
	}
}

// Validate executes this operation.
func (v *Validator) Validate(method string, params json.RawMessage) error {
	if v == nil {
		return nil
	}

	v.once.Do(v.loadSchemas)
	if v.loadErr != nil {
		return errors.Wrap(errors.CodeSchemaInvalid, "failed to load jsonrpc schemas", v.loadErr)
	}

	schema, ok := v.schemas[method]
	if !ok {
		return nil
	}

	if len(params) == 0 {
		params = []byte("{}")
	}
	docLoader := gojsonschema.NewBytesLoader(params)
	res, err := schema.Validate(docLoader)
	if err != nil {
		return errors.Wrap(errors.CodeSchemaInvalid, "jsonrpc schema validation failed", err)
	}
	if !res.Valid() {
		return errors.New(errors.CodeSchemaInvalid, fmt.Sprintf("jsonrpc schema errors: %s", res.Errors()))
	}
	return nil
}

// loadSchemas executes this operation.
func (v *Validator) loadSchemas() {
	entries, err := schemasFS.ReadDir("schemas")
	if err != nil {
		v.loadErr = err
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		method := entry.Name()[:len(entry.Name())-len(filepath.Ext(entry.Name()))]
		raw, err := schemasFS.ReadFile("schemas/" + entry.Name())
		if err != nil {
			v.loadErr = err
			return
		}

		schemaLoader := gojsonschema.NewBytesLoader(raw)
		schema, err := gojsonschema.NewSchema(schemaLoader)
		if err != nil {
			v.loadErr = err
			return
		}

		v.schemas[method] = schema
	}
}
