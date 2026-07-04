package main

import (
	"net/http"
	"time"
)

type Config struct {
	RequestTimeout      time.Duration
	StartupTimeout      time.Duration
	PollingInterval     time.Duration
	Gateway             string
	Followers           []string
	Leader              string
	PeerPublicAddresses map[string]string
}

type Runner struct {
	cfg    Config
	client *http.Client
}

type StateResponse struct {
	Leader    string   `json:"leader"`
	Followers []string `json:"followers"`
}

type KVResponse struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

type HTTPError struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}
