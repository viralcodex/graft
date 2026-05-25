package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string //current in-memory data
}

type PutRequest struct {
	Value string `json:"value"`
}

type GetAndPutResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DeleteResponse struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
}

type StatusResponse struct {
	NodeId string `json:"nodeId"`
	Port   string `json:"port"`
	Role   string `json:"role"`
}

type RootResponse struct {
	Message string `json:"message"`
}

type VoteRequest struct {
	Term        int    `json:"term"`
	CandidateId string `json:"candidateId"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type HeartbeatRequest struct {
	Term     int    `json:"term"`
	LeaderId string `json:"leaderId"`
}

type HeartbeatResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func initialise() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func setupConfig() (string, string, string, []string) {
	port := flag.String("port", "8000", "HTTP server port")
	nodeId := flag.String("nodeId", "node1", "nodeId for the server")
	cluster := flag.String("cluster", "localhost:8000,localhost:8001,localhost:8002", "Comma-separated cluster addresses")
	role := "follower"
	flag.Parse()

	selfAddr := "localhost:" + *port
	rawNodes := strings.Split(*cluster, ",")
	peers := make([]string, 0, len(rawNodes))

	for _, addr := range rawNodes {
		trimmedAddr := strings.TrimSpace(addr)
		if trimmedAddr == "" || trimmedAddr == selfAddr {
			continue
		}
		peers = append(peers, trimmedAddr)
	}

	return *port, *nodeId, role, peers
}

func startServer(port string) error {
	addr := ":" + port
	fmt.Println("Server running on", port)
	return http.ListenAndServe(addr, nil)
}

func main() {
	store := initialise()
	port, nodeId, role, peers := setupConfig()
	initRaftState(nodeId, role, peers)

	go runElectionTimer() //start the timer

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { rootHandler(w, r) })
	http.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { statusHandler(w, r, port, nodeId) })
	http.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) { getValueHandler(store, w, r) })
	http.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) { putValueHandler(store, w, r) })
	http.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) { deleteValueHandler(store, w, r) })
	http.HandleFunc("POST /vote", func(w http.ResponseWriter, r *http.Request) { requestVoteHandler(store, w, r) })
	http.HandleFunc("POST /heartbeat", func(w http.ResponseWriter, r *http.Request) { heartbeatHandler(w, r) })

	if err := startServer(port); err != nil {
		fmt.Println("Error starting server:", err)
	}
}

func rootHandler(w http.ResponseWriter, _ *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(RootResponse{
		Message: "This is a Raft Node running",
	})

	if err != nil {
		http.Error(w, "Node not responding", http.StatusInternalServerError)
		return
	}
}

func statusHandler(w http.ResponseWriter, _ *http.Request, port string, nodeId string) {
	w.Header().Set("Content-Type", "application/json")

	raftState.mu.Lock()
	role := raftState.role
	raftState.mu.Unlock()

	err := json.NewEncoder(w).Encode(StatusResponse{
		NodeId: nodeId,
		Port:   port,
		Role:   role,
	})

	if err != nil {
		http.Error(w, "Node not responding", http.StatusInternalServerError)
		return
	}
}

// gets a value from the concurrent map or returns null
func getValueHandler(store *KVStore, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	store.mu.RLock()
	value, ok := store.data[key]
	store.mu.RUnlock()

	if !ok {
		http.Error(w, "No record for this key", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(GetAndPutResponse{
		Key:   key,
		Value: value,
	})

	if err != nil {
		http.Error(w, "failed to get value", http.StatusInternalServerError)
		return
	}
}

func putValueHandler(store *KVStore, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	var req PutRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON Body", http.StatusBadRequest)
		return
	}

	store.mu.Lock()
	store.data[key] = req.Value
	store.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(GetAndPutResponse{
		Key:   key,
		Value: req.Value,
	})

	if err != nil {
		http.Error(w, "Failed to update store", http.StatusInternalServerError)
		return
	}
}

func deleteValueHandler(store *KVStore, w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	store.mu.Lock()
	delete(store.data, key)
	store.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(DeleteResponse{
		Key:     key,
		Deleted: true,
	})

	if err != nil {
		http.Error(w, "Failed to delete record", http.StatusInternalServerError)
		return
	}
}

func requestVoteHandler(_ *KVStore, w http.ResponseWriter, r *http.Request) {
	var req VoteRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON Body", http.StatusBadRequest)
		return
	}

	//now we call raft's grant vote method to either grant or deny the vote
	vote := grantVote(req)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(vote); err != nil {
		http.Error(w, "Failed to vote", http.StatusInternalServerError)
		return
	}
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatRequest

	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, "Invalid JSON Body", http.StatusBadRequest)
		return
	}

	res := receiveHeartBeat(req)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, "Failed to vote", http.StatusInternalServerError)
		return
	}
}
