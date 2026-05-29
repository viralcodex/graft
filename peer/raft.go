package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RaftState struct {
	mu sync.Mutex

	nodeId   string
	role     string
	peers    []string
	stateDir string
	timer    *time.Timer

	indexState       IndexState
	leaderIndexState LeaderIndexState

	persistentState PersistentState
}

type PersistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

type LogEntry struct {
	Command Command
	Term    int
	Index   int
}

// volatile
type IndexState struct {
	CommitIndex int
	LastApplied int
}

type LeaderIndexState struct {
	NextIndex  map[string]int
	MatchIndex map[string]int
}

var raftState RaftState
var client = &http.Client{Timeout: 120 * time.Millisecond}

func initRaftState(nodeId string, role string, peers []string, stateDir string) {
	persistantState := loadPersistantState(nodeId, stateDir)

	indexState := IndexState{
		CommitIndex: 0,
		LastApplied: 0,
	}

	leaderIndexState := LeaderIndexState{
		NextIndex:  make(map[string]int),
		MatchIndex: make(map[string]int),
	}

	raftState = RaftState{
		nodeId:           nodeId,
		role:             role,
		peers:            peers,
		stateDir:         stateDir,
		timer:            time.NewTimer(randomTimeout()),
		persistentState:  persistantState,
		indexState:       indexState,
		leaderIndexState: leaderIndexState,
	}
}

func randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func resetElectionTimer() {
	raftState.timer.Reset(randomTimeout())
}

func stepDownLocked(newTerm int) {
	raftState.role = "follower"
	if newTerm > raftState.persistentState.CurrentTerm {
		raftState.persistentState.CurrentTerm = newTerm
		raftState.persistentState.VotedFor = ""
		persistLocked()
	}
	resetElectionTimer()
}

func persistLocked() {
	if err := savePersistentState(raftState.persistentState, raftState.nodeId, raftState.stateDir); err != nil {
		panic(err)
	}
}

/*
commit the entries from commitIndex -> lastIndex (uncommitted entries) if majority nodes have replicated the entries sent from leader
then apply those entries to the state machine (applyToStore)
*/
func advanceCommitIndexLocked() {
	lastIndex := len(raftState.persistentState.Log)

	for i := lastIndex; i > raftState.indexState.CommitIndex; i-- {
		if raftState.persistentState.Log[i-1].Term != raftState.persistentState.CurrentTerm {
			continue
		}

		replicationNumber := 1 //replicated myself (leader)

		for _, peer := range raftState.peers {
			if raftState.leaderIndexState.MatchIndex[peer] >= i {
				replicationNumber++
			}
		}

		//when majority, commit these entries to your log & apply to state machine
		if replicationNumber > (len(raftState.peers)+1)/2 {
			raftState.indexState.CommitIndex = i
			applyCommittedEntriesLocked()
			return
		}
	}
}

func applyCommittedEntriesLocked() {
	for raftState.indexState.LastApplied < raftState.indexState.CommitIndex {
		raftState.indexState.LastApplied++
		currCommand := raftState.persistentState.Log[raftState.indexState.LastApplied-1].Command
		applyToStore(currCommand)
	}
}


func loadPersistantState(nodeId string, stateDir string) PersistentState {
	path := filepath.Join(stateDir, nodeId+".json")
	bytes, err := os.ReadFile(path)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			state := PersistentState{
				CurrentTerm: 0,
				VotedFor:    "",
				Log:         []LogEntry{},
			}

			err := savePersistentState(state, nodeId, stateDir)

			if err != nil {
				panic(err)
			}

			return state
		}
		panic(err)
	}

	var persistentState PersistentState

	if err := json.Unmarshal(bytes, &persistentState); err != nil {
		panic(err)
	}

	if persistentState.Log == nil {
		persistentState.Log = []LogEntry{}
	}

	return persistentState
}

/**
saves the log file as a json
*/
func savePersistentState(state PersistentState, nodeId string, stateDir string) error {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	
	path := filepath.Join(stateDir, nodeId+".json")
	
	bytes, err := json.MarshalIndent(state, "", "	")
	
	if err != nil {
		return err
	}
	
	return os.WriteFile(path, bytes, 0o644)
}

/*
apply the request to the state machine (persistent storage)
*/
func applyToStore(command Command) AppliedResult {
	result := AppliedResult{
		Found: false,
	}

	if command.ReqId == "" {
		return result
	}

	Store.mu.Lock()
	defer Store.mu.Unlock()

	if result, ok := Store.appliedReqIDs[command.ReqId]; ok {
		return result
	}

	if command.Operation == "PUT" {
		Store.data[command.Key] = command.Value
		result.Found = true
		result.Value = command.Value
		Store.appliedReqIDs[command.ReqId] = result
	}

	if command.Operation == "DELETE" {
		_, ok := Store.data[command.Key]
		result.Found = ok
		Store.appliedReqIDs[command.ReqId] = result
		delete(Store.data, command.Key)
	}

	return result
}

/*
commits the command entry to the Log (making sure the idempotency is maintained)

if the reqID is already applied, return true early (already applied)

if not, we try to see if the entry is already present in the log.

if not, we append it to the log and then keep waiting for 2 seconds until the entry is applied to state machine & return if done or not
*/
func submitCommand(command Command) bool {
	raftState.mu.Lock()

	if raftState.role != "leader" {
		raftState.mu.Unlock()
		return false
	}

	Store.mu.Lock()
	_, ok := Store.appliedReqIDs[command.ReqId]
	Store.mu.Unlock()

	if ok {
		return true
	}

	existingIndex := 0
	existingTerm := raftState.persistentState.CurrentTerm

	for _, entry := range raftState.persistentState.Log {
		if entry.Command.ReqId == command.ReqId {
			existingIndex = entry.Index
			break
		}
	}

	if existingIndex == 0 {
		entry := LogEntry{
			Command: command,
			Term:    raftState.persistentState.CurrentTerm,
			Index:   len(raftState.persistentState.Log) + 1,
		}
		raftState.persistentState.Log = append(raftState.persistentState.Log, entry)
		persistLocked()
		existingIndex = entry.Index
		existingTerm = entry.Term
	}

	raftState.mu.Unlock()

	timeout := time.Now().Add(2 * time.Second)

	//keeps checking for 2 seconds until timeout that the entry is committed or the leadership is lost
	for time.Now().Before(timeout) {
		raftState.mu.Lock()
		isSaved := raftState.indexState.LastApplied >= existingIndex
		isLeader := raftState.role == "leader" && raftState.persistentState.CurrentTerm == existingTerm
		raftState.mu.Unlock()

		if isSaved {
			return true
		}

		if !isLeader {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

/**
main loop keeps running, if the timer expires & the node isn't the leader, start election
*/
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

/*
this function makes the node a candidate, and sends a vote request to every follower.

1. if it receives a term higher that its own, it steps down immediately

2. if you receive votes > (peers + 1)/2, become leader

3. if you receive less votes than majority or impossible to achieve, step down early
*/
func startElection() {
	raftState.mu.Lock()
	raftState.persistentState.CurrentTerm++
	raftState.persistentState.VotedFor = raftState.nodeId //vote for yourself
	raftState.role = "candidate"
	resetElectionTimer()

	persistLocked()

	term := raftState.persistentState.CurrentTerm
	nodeId := raftState.nodeId
	peers := raftState.peers

	// current log state for this node (send it to others to compare with)
	lastLogIndex := len(raftState.persistentState.Log)
	lastLogTerm := 0

	if lastLogIndex > 0 {
		lastLogTerm = raftState.persistentState.Log[lastLogIndex-1].Term
	}

	raftState.mu.Unlock()

	votes := 1
	clusterSize := len(peers) + 1
	remainingResponses := len(peers)

	responses := make(chan VoteResponse, len(raftState.peers))

	for _, peer := range peers {
		go func(addr string, currentTerm int, leaderID string, lastLogIndex int, lastLogTerm int) {
			resp, err := requestVote(addr, VoteRequest{
				Term:         currentTerm,
				CandidateId:  leaderID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				responses <- VoteResponse{Term: term, VoteGranted: false}
				return
			}
			responses <- resp
		}(peer, term, nodeId, lastLogIndex, lastLogTerm)
	}

	for range peers {
		resp := <-responses
		remainingResponses--
		if resp.Term > term {
			raftState.mu.Lock()
			if resp.Term > raftState.persistentState.CurrentTerm { //recheck to prevent any staleness issues
				stepDownLocked(resp.Term)
			}
			raftState.mu.Unlock()
			return
		}
		raftState.mu.Lock()
		isCandidate := raftState.role == "candidate" && raftState.persistentState.CurrentTerm == term
		raftState.mu.Unlock()

		if !isCandidate {
			return
		}

		if resp.Term == term && resp.VoteGranted {
			votes++
		}

		//early promotion
		if votes > clusterSize/2 {
			raftState.mu.Lock()
			if raftState.role == "candidate" && raftState.persistentState.CurrentTerm == term {
				raftState.role = "leader"
				raftState.mu.Unlock()
				go sendAppendEntries()
				return
			}
			raftState.mu.Unlock()
			return
		}

		//early stepdown
		if votes+remainingResponses <= clusterSize/2 {
			raftState.mu.Lock()
			if raftState.role == "candidate" && raftState.persistentState.CurrentTerm == term {
				stepDownLocked(term)
			}
			raftState.mu.Unlock()
			return
		}
	}

	//still candidate, stepdown
	raftState.mu.Lock()
	if raftState.role == "candidate" && raftState.persistentState.CurrentTerm == term {
		stepDownLocked(term)
	}
	raftState.mu.Unlock()
}

/*
A follower grants vote to a candidate if:

1. its term is less than candidate term

2. its hasn't voted for anyone else in the term it's in (raftState.currentTerm)

3. if the log state of the candidate is ahead than the follower (logterm and then logindex(tiebreaker))
*/
func grantVote(req VoteRequest) VoteResponse {
	raftState.mu.Lock()
	defer raftState.mu.Unlock()

	//stale term - reject
	if raftState.persistentState.CurrentTerm > req.Term {
		return VoteResponse{
			Term:        raftState.persistentState.CurrentTerm,
			VoteGranted: false,
		}
	}

	//already voted for someone else - reject
	if req.Term == raftState.persistentState.CurrentTerm && raftState.persistentState.VotedFor != "" && req.CandidateId != raftState.persistentState.VotedFor {
		return VoteResponse{
			Term:        raftState.persistentState.CurrentTerm,
			VoteGranted: false,
		}
	}

	//match the log state and if candidate's less updated than me - reject
	lastLogIndex := len(raftState.persistentState.Log)
	lastLogTerm := 0

	if lastLogIndex > 0 {
		lastLogTerm = raftState.persistentState.Log[lastLogIndex-1].Term
	}

	if lastLogTerm > req.LastLogTerm || (lastLogTerm == req.LastLogTerm && lastLogIndex > req.LastLogIndex) {
		return VoteResponse{
			Term:        raftState.persistentState.CurrentTerm,
			VoteGranted: false,
		}
	}

	raftState.role = "follower"
	raftState.persistentState.CurrentTerm = req.Term
	raftState.persistentState.VotedFor = req.CandidateId
	resetElectionTimer()

	persistLocked()

	return VoteResponse{
		Term:        req.Term,
		VoteGranted: true,
	}
}

/*
sends heartbeats (empty entries[]) at constant intervals of 50 ms to all followers

sends entries (logs) to append to each follower's append logs (on client requests)

if the follower rejects the req with false, it retries indefinitely until the follower accepts.
*/
func sendAppendEntries() {
	for {
		raftState.mu.Lock()
		if raftState.role != "leader" {
			raftState.mu.Unlock()
			return
		}
		term := raftState.persistentState.CurrentTerm
		nodeId := raftState.nodeId
		peers := raftState.peers
		raftState.mu.Unlock()

		//appendEntries per follower
		for _, peer := range peers {
			raftState.mu.Lock()

			leaderCommitIndex := raftState.indexState.CommitIndex

			nextIndex := raftState.leaderIndexState.NextIndex[peer]
			if nextIndex == 0 {
				nextIndex = len(raftState.persistentState.Log) + 1
				raftState.leaderIndexState.NextIndex[peer] = nextIndex
			}

			prevLogIndex := nextIndex - 1
			prevLogTerm := 0

			if prevLogIndex > 0 {
				prevLogTerm = raftState.persistentState.Log[prevLogIndex-1].Term
			}

			entries := append([]LogEntry(nil), raftState.persistentState.Log[nextIndex-1:]...)

			appendReq := AppendEntriesRequest{
				Term:              term,
				LeaderId:          nodeId,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       prevLogTerm,
				LeaderCommitIndex: leaderCommitIndex,
				Entries:           entries,
			}
			raftState.mu.Unlock()

			go func(addr string, req AppendEntriesRequest) {
				resp, err := sendAppendEntry(addr, req)
				if err != nil {
					return
				}
				raftState.mu.Lock()
				defer raftState.mu.Unlock()

				if raftState.role != "leader" || raftState.persistentState.CurrentTerm != req.Term {
					return
				}

				if resp.Term > raftState.persistentState.CurrentTerm {
					stepDownLocked(resp.Term)
					return
				}

				//on success from the follower, we increase the next and match index ahead by how many entries are replicated for this node & advance the commit index
				if resp.Success {
					replicatedIndex := req.PrevLogIndex + len(req.Entries)
					raftState.leaderIndexState.MatchIndex[addr] = replicatedIndex
					raftState.leaderIndexState.NextIndex[addr] = replicatedIndex + 1

					advanceCommitIndexLocked()
					return
				}

				//otherwise we backoff one index due to mismatch
				if raftState.leaderIndexState.NextIndex[addr] > 1 {
					raftState.leaderIndexState.NextIndex[addr]--
				}
			}(peer, appendReq)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func sendAppendEntry(peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	body, _ := json.Marshal(req) //converts struct to json object

	resp, err := client.Post("http://"+peer+"/appendEntries", "application/json", bytes.NewReader(body))

	if err != nil {
		return AppendEntriesResponse{}, err
	}

	defer resp.Body.Close()

	var response AppendEntriesResponse

	err = json.NewDecoder(resp.Body).Decode(&response)

	if err != nil {
		return AppendEntriesResponse{}, err
	}

	return response, nil
}

/*
the follower node receive the request (hearbeat/appendEntry)

it rejects the request if:

1. the term of the last log entry of the leader doesn't match that of the follower
2. the index of the last log entry of the leader is OOB

otherwise the follower removes all its logs ahead of the last log index (from the leader) [:lastIndex]

then it appends the entries (From the req) from that point to the follower's logs [lastIndex:]
*/
func receiveAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	raftState.mu.Lock()
	defer raftState.mu.Unlock()

	if req.Term < raftState.persistentState.CurrentTerm {
		return AppendEntriesResponse{
			Term:    raftState.persistentState.CurrentTerm,
			Success: false,
		}
	}

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > len(raftState.persistentState.Log) {
			return AppendEntriesResponse{
				Term:    raftState.persistentState.CurrentTerm,
				Success: false,
			}
		}

		logTerm := raftState.persistentState.Log[req.PrevLogIndex-1].Term
		if logTerm != req.PrevLogTerm {
			return AppendEntriesResponse{
				Term:    raftState.persistentState.CurrentTerm,
				Success: false,
			}
		}
	}

	//remove the suffix and add the entries to the log
	insertAt := req.PrevLogIndex // 1-based prev index, so next entry starts here in 0-based slice
	log := raftState.persistentState.Log

	i := 0
	for i < len(req.Entries) {
		localPos := insertAt + i // offset from the lastIndex in node's log
		if localPos >= len(log) {
			break
		}
		if log[localPos].Term != req.Entries[i].Term {
			// conflict: delete local suffix from first mismatch
			log = log[:localPos]
			break
		}
		i++
	}

	// append entries after the mismatch
	if i < len(req.Entries) {
		log = append(log, req.Entries[i:]...)
	}

	raftState.persistentState.Log = log

	persistLocked()

	//move the committed state ahead if leader has committed till ahead
	raftState.indexState.CommitIndex = min(req.LeaderCommitIndex, len(log))

	applyCommittedEntriesLocked()

	stepDownLocked(req.Term) //step down if you're behind in terms

	return AppendEntriesResponse{
		Term:    raftState.persistentState.CurrentTerm,
		Success: true,
	}
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
