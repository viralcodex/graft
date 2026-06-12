package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/oklog/ulid/v2"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Gateway struct {
	mu         sync.RWMutex
	nodes      []string
	leaderAddr string // cached leader address
}

type StateResponse struct {
	Leader    string   `json:"leader"`
	Followers []string `json:"followers"`
}

type NodeStatus struct {
	Addr   string
	NodeId string
	Role   string
	Err    error
}

var client = &http.Client{Timeout: 500 * time.Millisecond}

func parseCluster(cluster string) []string {
	rawNodes := strings.Split(cluster, ",")
	nodes := make([]string, 0, len(rawNodes))

	for _, addr := range rawNodes {
		trimmedAddr := strings.TrimSpace(addr)
		if trimmedAddr == "" {
			continue
		}
		nodes = append(nodes, trimmedAddr)
	}

	return nodes
}

func getFollowers(nodes []NodeStatus) []string {
	followers := make([]string, 0, len(nodes))

	for _, node := range nodes {
		if node.Role == "follower" && node.Err == nil {
			followers = append(followers, node.Addr)
		}
	}

	return followers
}

func getNodesStatus(nodes []string) []NodeStatus {
	results := make(chan NodeStatus, len(nodes))

	for _, node := range nodes {
		go func(addr string) {
			results <- pollNode(addr)
		}(node)
	}

	var statuses []NodeStatus

	for range nodes {
		statuses = append(statuses, <-results)
	}

	return statuses
}

func pollNode(addr string) NodeStatus {
	status := NodeStatus{
		Addr: addr,
	}

	url := "http://" + addr + "/health"

	resp, err := client.Get(url)

	if err != nil {
		status.Err = err
		return status
	}

	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&status)

	if err != nil {
		status.Err = err
	}

	return status
}

func main() {
	port := flag.String("port", "7000", "Gateway port")
	cluster := flag.String("cluster", "raft-peer-0.raft-peers:8000,raft-peer-1.raft-peers:8000,raft-peer-2.raft-peers:8000", "Comma-separated cluster addresses")
	flag.Parse()

	gw := &Gateway{
		nodes: parseCluster(*cluster),
	}

	http.HandleFunc("GET /raft/state", func(w http.ResponseWriter, r *http.Request) { stateHandler(gw, w) })
	http.HandleFunc("GET /raft/get/{key}", func(w http.ResponseWriter, r *http.Request) { getHandler(gw, w, r) })
	http.HandleFunc("PUT /raft/update/{key}", func(w http.ResponseWriter, r *http.Request) { updateHandler(gw, w, r) })
	http.HandleFunc("DELETE /raft/delete/{key}", func(w http.ResponseWriter, r *http.Request) { deleteHandler(gw, w, r) })

	addr := ":" + *port
	fmt.Println("Gateway running on", *port)

	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Println("Error starting gateway:", err)
	}
}

func getReqId(r *http.Request) string {
	reqId := r.Header.Get("X-Request-ID")
	if reqId != "" {
		return reqId
	}
	reqId = ulid.Make().String()
	return reqId
}

// GET /raft/state — poll all nodes, return cluster state
func stateHandler(gw *Gateway, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")

	statuses := getNodesStatus(gw.nodes)

	for _, status := range statuses {
		if status.Role == "leader" {
			gw.mu.Lock()
			gw.leaderAddr = status.Addr
			gw.mu.Unlock()
		}
	}

	json.NewEncoder(w).Encode(StateResponse{
		Leader:    gw.leaderAddr,
		Followers: getFollowers(statuses),
	})
}

// GET /raft/get/{key} — forward to leader's GET /kv/{key}
func getHandler(gw *Gateway, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if leader == "" {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
		return
	}

	url := fmt.Sprintf("http://%s/kv/%s", leader, key)

	reqNode(http.MethodGet, url, w, r)
}

// PUT /raft/update/{key} — forward to leader's PUT /kv/{key}
func updateHandler(gw *Gateway, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if leader == "" {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
		return
	}

	url := fmt.Sprintf("http://%s/kv/%s", leader, key)

	reqNode(http.MethodPut, url, w, r)
}

// DELETE /raft/delete/{key} — forward to leader's DELETE /kv/{key}
func deleteHandler(gw *Gateway, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if leader == "" {
		http.Error(w, "no leader known", http.StatusServiceUnavailable)
		return
	}

	url := fmt.Sprintf("http://%s/kv/%s", leader, key)

	reqNode(http.MethodDelete, url, w, r)
}

func reqNode(method string, url string, w http.ResponseWriter, r *http.Request) {
	req, err := http.NewRequest(method, url, r.Body)

	if err != nil {
		http.Error(w, "bad request", http.StatusInternalServerError)
		return
	}

	reqId := getReqId(r)
	req.Header.Set("X-Request-ID", reqId)

	resp, err := client.Do(req)

	if err != nil {
		http.Error(w, "failed to reach leader", http.StatusBadGateway)
		return
	}

	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-ID", reqId)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
