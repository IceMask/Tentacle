// image_resize_test.go verifies screenshot thumbnail resizing behavior used by orchestrator artifact generation.
package orchestrator

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

// encodeTestPNG builds one deterministic solid-color PNG image with the requested dimensions for thumbnail-resize tests.
func encodeTestPNG(t *testing.T, width int, height int) []byte {
	t.Helper() // Mark this helper so failures point at the calling test rather than the shared image-encoding helper.

	img := image.NewNRGBA(image.Rect(0, 0, width, height)) // Allocate one source image with the requested bounds so resize math can be asserted precisely.
	fill := color.NRGBA{R: 16, G: 32, B: 64, A: 255}       // Define one stable non-transparent fill color so the PNG bytes remain deterministic enough for test use.
	for y := 0; y < height; y++ {                          // Fill every image row so the encoded PNG represents one complete solid-color image.
		for x := 0; x < width; x++ { // Fill every image column so the encoded PNG represents one complete solid-color image.
			img.Set(x, y, fill) // Write the stable fill color into the current source pixel.
		}
	}

	var output bytes.Buffer                          // Allocate the output buffer that will receive the encoded PNG bytes for the generated source image.
	if err := png.Encode(&output, img); err != nil { // Encode the generated source image as PNG so the resize helper receives realistic screenshot bytes.
		t.Fatalf("expected test png encode to succeed, got error: %v", err) // Surface the encode failure so the resize tests do not proceed with invalid fixtures.
	}

	return output.Bytes() // Return the encoded PNG bytes so the resize helper can be exercised with one realistic image payload.
}

// decodePNGConfig reads one PNG payload and returns its dimensions for compact thumbnail assertions.
func decodePNGConfig(t *testing.T, data []byte) (int, int) {
	t.Helper() // Mark this helper so failures point at the calling test rather than the shared image-decoding helper.

	cfg, err := png.DecodeConfig(bytes.NewReader(data)) // Decode only the image config so tests can assert dimensions without decoding full pixel data unnecessarily.
	if err != nil {                                     // Fail immediately when the thumbnail bytes are not one valid PNG payload.
		t.Fatalf("expected png decode config to succeed, got error: %v", err) // Surface the decode failure so resize regressions are easy to diagnose.
	}

	return cfg.Width, cfg.Height // Return the decoded dimensions so callers can assert the thumbnail size precisely.
}

// TestBuildScreenshotThumbnailResizesLongEdge verifies that large screenshots are downscaled so their longest edge is capped at 640 pixels while preserving aspect ratio.
func TestBuildScreenshotThumbnailResizesLongEdge(t *testing.T) {
	original := encodeTestPNG(t, 800, 400)                    // Build one wide screenshot fixture whose longest edge exceeds the thumbnail cap.
	thumbnail, err := buildScreenshotThumbnail(original, 640) // Resize the screenshot through the production thumbnail helper.
	if err != nil {                                           // Fail immediately when the resize helper unexpectedly rejects one valid PNG screenshot.
		t.Fatalf("expected thumbnail resize to succeed, got error: %v", err) // Surface the resize failure so thumbnail regressions are easy to diagnose.
	}

	width, height := decodePNGConfig(t, thumbnail) // Decode the thumbnail dimensions so the resize result can be asserted precisely.
	if width != 640 || height != 320 {             // Assert the expected aspect-ratio-preserving downscale from 800x400 into 640x320.
		t.Fatalf("expected thumbnail dimensions 640x320, got %dx%d", width, height) // Surface the actual dimensions so resize regressions are easy to diagnose.
	}
}

// TestBuildScreenshotThumbnailKeepsSmallImages verifies that screenshots already smaller than the thumbnail limit are not upscaled.
func TestBuildScreenshotThumbnailKeepsSmallImages(t *testing.T) {
	original := encodeTestPNG(t, 100, 50)                     // Build one small screenshot fixture whose longest edge is already below the thumbnail cap.
	thumbnail, err := buildScreenshotThumbnail(original, 640) // Resize the screenshot through the production thumbnail helper.
	if err != nil {                                           // Fail immediately when the resize helper unexpectedly rejects one valid PNG screenshot.
		t.Fatalf("expected thumbnail resize to succeed, got error: %v", err) // Surface the resize failure so thumbnail regressions are easy to diagnose.
	}

	width, height := decodePNGConfig(t, thumbnail) // Decode the thumbnail dimensions so the no-upscale behavior can be asserted precisely.
	if width != 100 || height != 50 {              // Assert that small images remain at their original size rather than being enlarged to the thumbnail limit.
		t.Fatalf("expected thumbnail dimensions 100x50, got %dx%d", width, height) // Surface the actual dimensions so resize regressions are easy to diagnose.
	}
}
