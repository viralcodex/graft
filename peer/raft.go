package main

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type RaftState struct {
	mu       sync.Mutex
	nodeId   string
	role     string
	term     int
	peers    []string
	votedFor string
	timer    *time.Timer
}

var raftState RaftState
var client = &http.Client{Timeout: 120 * time.Millisecond}

func randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func resetElectionTimerLocked() {
	raftState.timer.Reset(randomTimeout())
}

func stepDownLocked(newTerm int) {
	raftState.role = "follower"
	if newTerm > raftState.term {
		raftState.term = newTerm
		raftState.votedFor = ""
	}
	resetElectionTimerLocked()
}

func initRaftState(nodeId string, role string, peers []string) {
	raftState = RaftState{
		nodeId:   nodeId,
		role:     role,
		term:     0,
		peers:    peers,
		votedFor: "",
		timer:    time.NewTimer(randomTimeout()),
	}
}

func runElectionTimer() {
	for {
		<-raftState.timer.C //blocks until timer fires

		raftState.mu.Lock()
		if raftState.role == "leader" {
			raftState.mu.Unlock()
			continue
		}
		raftState.mu.Unlock()

		startElection()
	}
}

func startElection() {
	raftState.mu.Lock()
	raftState.term++
	raftState.role = "candidate"
	raftState.votedFor = raftState.nodeId
	resetElectionTimerLocked()

	term := raftState.term
	nodeId := raftState.nodeId
	peers := raftState.peers
	raftState.mu.Unlock()

	votes := 1
	clusterSize := len(peers) + 1

	responses := make(chan VoteResponse, len(raftState.peers))

	for _, peer := range peers {
		go func(addr string, currentTerm int, leaderID string) {
			resp, err := requestVote(addr, VoteRequest{
				Term:        currentTerm,
				CandidateId: leaderID,
			})

			if err != nil {
				responses <- VoteResponse{Term: term, VoteGranted: false}
				return
			}
			responses <- resp
		}(peer, term, nodeId)
	}

	for range peers {
		resp := <-responses
		if resp.Term > term {
			raftState.mu.Lock()
			if resp.Term > raftState.term {
				stepDownLocked(resp.Term)
			}
			raftState.mu.Unlock()
			return
		}

		if resp.Term == term && resp.VoteGranted {
			votes++
		}
	}

	raftState.mu.Lock()
	defer raftState.mu.Unlock()

	if votes > clusterSize/2 && raftState.role == "candidate" {
		raftState.role = "leader"
		go sendHeartBeats()
	}
}

func grantVote(req VoteRequest) VoteResponse {
	raftState.mu.Lock()
	defer raftState.mu.Unlock()

	//stale term - reject
	if raftState.term > req.Term {
		return VoteResponse{
			Term:        raftState.term,
			VoteGranted: false,
		}
	}

	//already voted for someone else - reject
	if req.Term == raftState.term && raftState.votedFor != "" && req.CandidateId != raftState.votedFor {
		return VoteResponse{
			Term:        raftState.term,
			VoteGranted: false,
		}
	}

	raftState.role = "follower"
	raftState.term = req.Term
	raftState.votedFor = req.CandidateId
	resetElectionTimerLocked()

	return VoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}
}

func sendHeartBeats() {
	for {
		raftState.mu.Lock()
		if raftState.role != "leader" {
			raftState.mu.Unlock()
			return
		}
		term := raftState.term
		nodeId := raftState.nodeId
		peers := raftState.peers
		raftState.mu.Unlock()

		for _, peer := range peers {
			go func(addr string, currentTerm int, leaderID string) {
				resp, err := sendHeartBeat(addr, HeartbeatRequest{
					Term:     currentTerm,
					LeaderId: leaderID,
				})
				if err != nil {
					return
				}
				if resp.Term > currentTerm {
					raftState.mu.Lock()
					if resp.Term > raftState.term { //double check to prevent stale update
						stepDownLocked(resp.Term)
					}
					raftState.mu.Unlock()
				}
			}(peer, term, nodeId)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func sendHeartBeat(peer string, req HeartbeatRequest) (HeartbeatResponse, error) {
	body, _ := json.Marshal(req) //converts struct to json object

	resp, err := client.Post("http://"+peer+"/heartbeat", "application/json", bytes.NewReader(body))

	if err != nil {
		return HeartbeatResponse{}, err
	}

	defer resp.Body.Close()

	var response HeartbeatResponse

	err = json.NewDecoder(resp.Body).Decode(&response)

	if err != nil {
		return HeartbeatResponse{}, err
	}

	return response, nil
}

func requestVote(peer string, req VoteRequest) (VoteResponse, error) {
	body, _ := json.Marshal(req) //converts struct to json object

	resp, err := client.Post("http://"+peer+"/vote", "application/json", bytes.NewReader(body))

	if err != nil {
		return VoteResponse{}, err
	}

	defer resp.Body.Close()

	var response VoteResponse

	err = json.NewDecoder(resp.Body).Decode(&response)

	if err != nil {
		return VoteResponse{}, err
	}

	return response, nil
}

func receiveHeartBeat(req HeartbeatRequest) HeartbeatResponse {
	raftState.mu.Lock()
	defer raftState.mu.Unlock()

	if req.Term < raftState.term {
		return HeartbeatResponse{
			Term:    raftState.term,
			Success: false,
		}
	}

	stepDownLocked(req.Term)

	return HeartbeatResponse{
		Term:    raftState.term,
		Success: true,
	}
}
