package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type Health struct {
	Health string `json:"health"`
}

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
		slog.Error("Node returned an error response.", "Error", err.Error())
		status.Err = err
		return status
	}

	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&status)

	if err != nil {
		slog.Error("Node returned an error response.", "Error", err.Error())
		status.Err = err
	}

	return status
}
func initLogger() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})

	logger := slog.New(handler)

	slog.SetDefault(logger)
}

func main() {
	port := flag.String("port", "7000", "Gateway port")
	cluster := flag.String("cluster", "raft-peer-0.raft-peer:8000,raft-peer-1.raft-peer:8000,raft-peer-2.raft-peer:8000", "Comma-separated cluster addresses")
	flag.Parse()

	gw := &Gateway{
		nodes: parseCluster(*cluster),
	}

	initLogger()
	//fetch raft statefulset on startup
	fetchRaftState(gw)

	http.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { healthHandler(w) })
	http.HandleFunc("GET /raft/state", func(w http.ResponseWriter, r *http.Request) { stateHandler(gw, w) })
	http.HandleFunc("GET /raft/kv/{key}", func(w http.ResponseWriter, r *http.Request) { getHandler(gw, w, r) })
	http.HandleFunc("PUT /raft/kv/{key}", func(w http.ResponseWriter, r *http.Request) { updateHandler(gw, w, r) })
	http.HandleFunc("DELETE /raft/kv/{key}", func(w http.ResponseWriter, r *http.Request) { deleteHandler(gw, w, r) })
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

func fetchRaftState(gw *Gateway) []NodeStatus {
	statuses := getNodesStatus(gw.nodes)

	leaderAddr := ""

	//assign leader
	for _, status := range statuses {
		if status.Role == "leader" {
			leaderAddr = status.Addr
			break
		}
	}

	gw.mu.Lock()
	gw.leaderAddr = leaderAddr
	gw.mu.Unlock()

	return statuses
}

// GET /raft/state — poll all nodes, return cluster state
func stateHandler(gw *Gateway, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")

	statuses := fetchRaftState(gw)

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if err := json.NewEncoder(w).Encode(StateResponse{
		Leader:    leader,
		Followers: getFollowers(statuses),
	}); err != nil {
		http.Error(w, "Internal error occured", http.StatusInternalServerError)
		return
	}
}

// health endpoint
func healthHandler(w http.ResponseWriter) {
	w.Header().Set("Content-type", "application/json")

	if err := json.NewEncoder(w).Encode(Health{Health: "OK"}); err != nil {
		http.Error(w, "Internal error occured", http.StatusInternalServerError)
		return
	}
}

// GET /raft/kv/{key} — forward to leader's GET /kv/{key}
func getHandler(gw *Gateway, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if leader == "" {
		fetchRaftState(gw) //refetch it
		gw.mu.RLock()
		if gw.leaderAddr == "" { //still not found? return err
			http.Error(w, "no leader known", http.StatusServiceUnavailable)
			gw.mu.RUnlock()
			return
		}
		leader = gw.leaderAddr //update leader
		gw.mu.RUnlock()
	}

	url := fmt.Sprintf("http://%s/kv/%s", leader, key)

	reqNode(http.MethodGet, url, w, r)
}

// PUT /raft/kv/{key} — forward to leader's PUT /kv/{key}
func updateHandler(gw *Gateway, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if leader == "" {
		fetchRaftState(gw) //refetch it
		gw.mu.RLock()
		if gw.leaderAddr == "" { //still not found? return err
			http.Error(w, "no leader known", http.StatusServiceUnavailable)
			gw.mu.RUnlock()
			return
		}
		leader = gw.leaderAddr //update leader
		gw.mu.RUnlock()
	}

	url := fmt.Sprintf("http://%s/kv/%s", leader, key)

	reqNode(http.MethodPut, url, w, r)
}

// DELETE /raft/kv/{key} — forward to leader's DELETE /kv/{key}
func deleteHandler(gw *Gateway, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	gw.mu.RLock()
	leader := gw.leaderAddr
	gw.mu.RUnlock()

	if leader == "" {
		fetchRaftState(gw) //refetch it
		gw.mu.RLock()
		if gw.leaderAddr == "" { //still not found? return err
			http.Error(w, "no leader known", http.StatusServiceUnavailable)
			gw.mu.RUnlock()
			return
		}
		leader = gw.leaderAddr //update leader
		gw.mu.RUnlock()
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
