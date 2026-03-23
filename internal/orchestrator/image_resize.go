// image_resize.go provides small image-resize helpers used by screenshot artifact generation.
package orchestrator

import (
	"bytes"
	"image"
	"image/png"

	"mcp_for_appium/internal/errors"
)

// buildScreenshotThumbnail decodes one screenshot image and returns one PNG thumbnail whose longest edge is bounded by maxEdge without upscaling smaller images.
func buildScreenshotThumbnail(data []byte, maxEdge int) ([]byte, error) {
	if maxEdge <= 0 { // Reject invalid target sizes before attempting image decoding because the resize math depends on one positive maximum edge.
		return nil, errors.New(errors.CodePlanInvalid, "thumbnail max edge must be > 0") // Surface the invalid resize request through the stable invalid-plan contract.
	}

	src, _, err := image.Decode(bytes.NewReader(data)) // Decode the original screenshot bytes so the thumbnail dimensions can be derived from the real image bounds.
	if err != nil {                                    // Stop immediately when the screenshot bytes are not one decodable image payload.
		return nil, errors.Wrap(errors.CodeInternal, "failed to decode screenshot image", err) // Surface the decode failure through the stable internal error contract.
	}

	bounds := src.Bounds()         // Read the decoded image bounds so the resize ratio can be computed deterministically.
	width := bounds.Dx()           // Capture the source width because the resize helper needs the original horizontal pixel count.
	height := bounds.Dy()          // Capture the source height because the resize helper needs the original vertical pixel count.
	if width <= 0 || height <= 0 { // Reject degenerate images because the resize math and PNG encoder both require positive dimensions.
		return nil, errors.New(errors.CodeInternal, "screenshot image has invalid dimensions") // Surface the invalid source image through the stable internal error contract.
	}

	targetWidth := width                    // Start from the original width so small images remain unchanged rather than being upscaled.
	targetHeight := height                  // Start from the original height so small images remain unchanged rather than being upscaled.
	if width >= height && width > maxEdge { // Scale down landscape or square images by their width when the width exceeds the configured max edge.
		targetWidth = maxEdge                       // Clamp the long edge to the configured maximum width.
		targetHeight = max(1, height*maxEdge/width) // Preserve aspect ratio while ensuring the resized height never falls below one pixel.
	} else if height > width && height > maxEdge { // Scale down portrait images by their height when the height exceeds the configured max edge.
		targetHeight = maxEdge                     // Clamp the long edge to the configured maximum height.
		targetWidth = max(1, width*maxEdge/height) // Preserve aspect ratio while ensuring the resized width never falls below one pixel.
	}
	if targetWidth == width && targetHeight == height { // Return the original bytes unchanged when the image already fits inside the target box.
		return data, nil // Avoid needless re-encoding when the original screenshot is already small enough to serve as its own thumbnail.
	}

	thumbnail := resizeNearest(src, targetWidth, targetHeight) // Resize the decoded image into one smaller NRGBA image using one deterministic nearest-neighbor sampler.
	var output bytes.Buffer                                    // Allocate the encoded output buffer so the resized image can be serialized back into PNG bytes.
	if err := png.Encode(&output, thumbnail); err != nil {     // Encode the resized image as PNG so thumbnail artifacts keep the same content type as the full screenshot.
		return nil, errors.Wrap(errors.CodeInternal, "failed to encode screenshot thumbnail", err) // Surface the PNG-encode failure through the stable internal error contract.
	}

	return output.Bytes(), nil // Return the PNG thumbnail bytes so the caller can persist them as the thumbnail artifact payload.
}

// resizeNearest scales one decoded image into one new NRGBA image using one deterministic nearest-neighbor sampling strategy.
func resizeNearest(src image.Image, targetWidth int, targetHeight int) *image.NRGBA {
	srcBounds := src.Bounds()                                          // Read the source image bounds so the resize loop can map destination pixels back to source pixels.
	srcWidth := srcBounds.Dx()                                         // Capture the source width once so the pixel-mapping math can reuse it cheaply inside the nested loops.
	srcHeight := srcBounds.Dy()                                        // Capture the source height once so the pixel-mapping math can reuse it cheaply inside the nested loops.
	dst := image.NewNRGBA(image.Rect(0, 0, targetWidth, targetHeight)) // Allocate the destination image with the requested thumbnail dimensions.
	for y := 0; y < targetHeight; y++ {                                // Fill every destination row so the resize output covers the full thumbnail image.
		srcY := srcBounds.Min.Y + y*srcHeight/targetHeight // Map the current destination row back to the nearest source row while preserving the full vertical range.
		for x := 0; x < targetWidth; x++ {                 // Fill every destination column inside the current row so the output image is fully populated.
			srcX := srcBounds.Min.X + x*srcWidth/targetWidth // Map the current destination column back to the nearest source column while preserving the full horizontal range.
			dst.Set(x, y, src.At(srcX, srcY))                // Copy the nearest source pixel into the current destination pixel.
		}
	}
	return dst // Return the fully populated resized image so the caller can encode it into one thumbnail artifact payload.
}
