package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

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
	Term         int    `json:"term"`
	CandidateId  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesRequest struct {
	Term              int        `json:"term"`
	LeaderId          string     `json:"leaderId"`
	PrevLogIndex      int        `json:"prevLogIndex"`
	PrevLogTerm       int        `json:"prevLogTerm"`
	Entries           []LogEntry `json:"entries"`
	LeaderCommitIndex int        `json:"leaderCommitIndex"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type Command struct {
	ReqId     string
	Operation string
	Key       string
	Value     string
}

func setupConfig() (string, string, string, []string, string) {
	port := flag.String("port", "8000", "HTTP server port")
	nodeId := flag.String("nodeId", "node1", "nodeId for the server")
	cluster := flag.String("cluster", "raft-peer-0.raft-peers:8000,raft-peer-1.raft-peers:8000,raft-peer-2.raft-peers:8000", "Comma-separated cluster addresses")
	stateDir := flag.String("stateDir", "state", "Directory for persisted raft state")
	role := "follower"
	flag.Parse()

	selfAddr := *nodeId + ".raft-peers:" + *port
	rawNodes := strings.Split(*cluster, ",")
	peers := make([]string, 0, len(rawNodes))

	for _, addr := range rawNodes {
		trimmedAddr := strings.TrimSpace(addr)
		if trimmedAddr == "" || trimmedAddr == selfAddr {
			continue
		}
		peers = append(peers, trimmedAddr)
	}

	return *port, *nodeId, role, peers, *stateDir
}

func startServer(port string, mux *http.ServeMux) (error) {
	server := &http.Server{
		Addr: ":" + port,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errorChannel := make(chan error, 1)

	go func() {
		errorChannel <- server.ListenAndServe()
	}()

	//handle graceful shutdown (1. for interruption signals/errors 2. for server errors)
	select {
	case err := <-errorChannel:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
		defer cancel()
		
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
		}
	}
	return nil
}

func initDBConfig() (error) {
	err := initDB()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	port, nodeId, role, peers, stateDir := setupConfig()
	
	if err := initDBConfig(); err != nil {
		panic(err)
	}
	
	defer dbPool.Close()
	
	initRaftState(nodeId, role, peers, stateDir)
	go runElectionTimer() //start the timer

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { rootHandler(w, r) })
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { statusHandler(w, r, port, nodeId) })
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) { getValueHandler(w, r) })
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) { putValueHandler(w, r) })
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) { deleteValueHandler(w, r) })
	mux.HandleFunc("POST /vote", func(w http.ResponseWriter, r *http.Request) { requestVoteHandler(w, r) })
	mux.HandleFunc("POST /appendEntries", func(w http.ResponseWriter, r *http.Request) { appendEntriesHandler(w, r) })
	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, r *http.Request) { receiveSnapshotHandler(w, r) })
	
	if err := startServer(port, mux); err != nil {
		panic(err)
	}
}

func rootHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(RootResponse{
		Message: "This is a Raft Node running",
	})

	if err != nil {
		http.Error(w, "Node not responding", http.StatusInternalServerError)
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
	}
}

// gets a value from the concurrent map or returns null
func getValueHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, ok, err := getValue(r.Context(), key)

	if err != nil {
		http.Error(w, "failed to get value", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "No record for this key", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(GetAndPutResponse{
		Key:   key,
		Value: value,
	})

	if err != nil {
		http.Error(w, "failed to get value", http.StatusInternalServerError)
	}
}

func putValueHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	reqId := r.Header.Get("X-Request-ID")

	var req PutRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON Body", http.StatusBadRequest)
		return
	}

	command := Command{
		ReqId:     reqId,
		Operation: "PUT",
		Key:       key,
		Value:     req.Value,
	}

	res := submitCommand(command)

	if !res {
		http.Error(w, "failed to commit write", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(GetAndPutResponse{
		Key:   key,
		Value: req.Value,
	})

	if err != nil {
		http.Error(w, "Failed to update store", http.StatusInternalServerError)
	}
}

func deleteValueHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	reqId := r.Header.Get("X-Request-ID")

	command := Command{
		ReqId:     reqId,
		Operation: "DELETE",
		Key:       key,
	}

	res := submitCommand(command)

	if !res {
		http.Error(w, "failed to commit delete", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(DeleteResponse{
		Key:     key,
		Deleted: true,
	})

	if err != nil {
		http.Error(w, "Failed to delete record", http.StatusInternalServerError)
	}
}

func requestVoteHandler(w http.ResponseWriter, r *http.Request) {
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
	}
}

func appendEntriesHandler(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest

	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, "Invalid JSON Body", http.StatusBadRequest)
		return
	}

	res := receiveAppendEntries(req)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, "Failed to vote", http.StatusInternalServerError)
	}
}

func receiveSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	var req InstallSnapshotRequest

	err := json.NewDecoder(r.Body).Decode(&req)

	if err != nil {
		http.Error(w, "Invalid Request", http.StatusBadRequest)
		return
	}

	res, err := receiveSnapshot(req)

	if err != nil {
		http.Error(w, "Failed to apply snapshot", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, "Failed to apply snapshot", http.StatusInternalServerError)
	}
}
