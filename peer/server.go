package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
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
	Role string `json:"role"`
}

type RootResponse struct {
	Message string `json:"message"`
}

func initialise() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func setupConfig() (string, string, string) {
	port := flag.String("port", "8000", "HTTP server port")
	nodeId := flag.String("nodeId", "node1", "nodeId for the server")
	role := "follower"
	flag.Parse()
	return *port, *nodeId, role
}

func startServer(port string) error {
	addr := ":" + port
	fmt.Println("Server running on", port)
	return http.ListenAndServe(addr, nil)
}

func main() {
	store := initialise()
	port, nodeId, role := setupConfig()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { rootHandler(w, r) })
	http.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { statusHandler(w, r, port, nodeId, role) })
	http.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) { getValueHandler(store, w, r) })
	http.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) { putValueHandler(store, w, r) })
	http.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) { deleteValueHandler(store, w, r) })

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

func statusHandler(w http.ResponseWriter, _ *http.Request, port string, nodeId string, role string) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(StatusResponse{
		NodeId: nodeId,
		Port:   port,
		Role: role,
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
