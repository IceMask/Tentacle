package appium

import (
	"net/http"
	"time"
)

type Client struct {
	httpClient *http.Client
	baseUrl    string
}

func NewClient(baseUrl string) *Client {
	return &Client{
		baseUrl: baseUrl,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Methods to interact with Appium will be added here
