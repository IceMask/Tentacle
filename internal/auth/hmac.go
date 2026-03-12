package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"mcp_for_appium/internal/errors"
	"mcp_for_appium/internal/util"
)

const (
	HMACWindow = 5 * time.Minute
)

type HMACValidator struct {
	secretStore func(keyID string) (string, error)
	nonceStore  util.IdempotencyStore
}

// NewHMACValidator executes this operation.
func NewHMACValidator(secretStore func(string) (string, error), nonceStore util.IdempotencyStore) *HMACValidator {
	return &HMACValidator{
		secretStore: secretStore,
		nonceStore:  nonceStore,
	}
}

// Verify verifies the HMAC signature.
// sig: hex encoded signature
// method, path, body: request details
// ts: timestamp string (unix epoch)
// nonce: unique string
// keyID: identifier for the secret key
func (v *HMACValidator) Verify(ctx context.Context, sig, method, path, body string, ts string, nonce string, keyID string) (*Subject, error) {
	// 1. Check Timestamp
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, errors.New(errors.CodeUnauthenticated, "invalid timestamp")
	}
	reqTime := time.Unix(t, 0)
	if time.Since(reqTime).Abs() > HMACWindow {
		return nil, errors.New(errors.CodeUnauthenticated, "timestamp out of window")
	}

	// 2. Check Nonce
	// We use the nonce store to ensure uniqueness within a reasonable window (e.g. 24h)
	// If nonceStore is nil, we skip (but should warn)
	if v.nonceStore != nil {
		acquired, _, err := v.nonceStore.CheckAndSet(ctx, "nonce:"+nonce, "1", 24*time.Hour)
		if err != nil {
			return nil, errors.Wrap(errors.CodeInternal, "failed to check nonce", err)
		}
		if !acquired {
			return nil, errors.New(errors.CodeNonceDuplicate, "nonce reused")
		}
	}

	// 3. Get Secret
	secret, err := v.secretStore(keyID)
	if err != nil {
		return nil, errors.Wrap(errors.CodeUnauthenticated, "key not found", err)
	}

	// 4. Compute HMAC
	// Format: method + path + ts + nonce + bodyHash
	bodyHash := sha256.Sum256([]byte(body))
	msg := fmt.Sprintf("%s%s%s%s%x", method, path, ts, nonce, bodyHash)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sig), []byte(expectedSig)) {
		return nil, errors.New(errors.CodeUnauthenticated, "invalid signature")
	}

	return &Subject{
		ID:   keyID,
		Type: "hmac",
	}, nil
}
