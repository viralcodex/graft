package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// gateway request handlers
func (r *Runner) state(ctx context.Context, reqId string) (StateResponse, error) {
	var out StateResponse

	err := r.request(ctx, nil, http.MethodGet, r.cfg.Gateway+"/raft/state", &out, reqId)

	return out, err
}

func (r *Runner) get(ctx context.Context, key string, reqId string) (KVResponse, error) {
	var out KVResponse
	err := r.request(ctx, nil, http.MethodGet, r.cfg.Gateway+"/raft/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) put(ctx context.Context, key, value string, reqId string) (KVResponse, error) {
	var out KVResponse
	body, err := json.Marshal(map[string]string{"value": value})

	if err != nil {
		return out, err
	}

	err = r.request(ctx, body, http.MethodPut, r.cfg.Gateway+"/raft/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) delete(ctx context.Context, key, reqId string) (KVResponse, error) {
	var out KVResponse
	err := r.request(ctx, nil, http.MethodDelete, r.cfg.Gateway+"/raft/kv/"+key, &out, reqId)
	return out, err
}

// peer request handlers
func (r *Runner) getFromPeer(ctx context.Context, addr, key, reqId string) (KVResponse, error) {
	var out KVResponse
	publicAddr, ok := r.cfg.PeerPublicAddresses[addr]
	if !ok {
		return out, fmt.Errorf("no public address configured for peer %s", addr)
	}
	err := r.request(ctx, nil, http.MethodGet, publicAddr+"/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) deleteFromPeer(ctx context.Context, addr, key, reqId string) (KVResponse, error) {
	var out KVResponse
	publicAddr, ok := r.cfg.PeerPublicAddresses[addr]
	if !ok {
		return out, fmt.Errorf("no public address configured for peer %s", addr)
	}
	err := r.request(ctx, nil, http.MethodDelete, publicAddr+"/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) request(ctx context.Context, body []byte, method, url string, resObj any, reqId string) error {
	var reader io.Reader

	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reader)

	if err != nil {
		return err
	}

	if body != nil {
		req.Header.Set("content-type", "application/json")
		req.Header.Set("X-Request-Id", reqId)
	}

	res, err := r.client.Do(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		data, err := io.ReadAll(res.Body)

		if err != nil {
			return err
		}

		return &HTTPError{
			Method:     method,
			URL:        url,
			StatusCode: res.StatusCode,
			Body:       string(data),
		}
	}

	if resObj == nil {
		return nil
	}

	return json.NewDecoder(res.Body).Decode(resObj)
}