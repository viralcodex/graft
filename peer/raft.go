package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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

	//extra metadata for snapshotting
	snapshotInFlight map[string]bool
	snapshotState    SnapshotState
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

// extra metadata to keep in memory raft state
type SnapshotState struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term int
}

// config
var logSizeThreshold int64
var unappliedLogEntries int

func initConfig() {
	value1, err := strconv.ParseInt(os.Getenv("DEV_LOG_SIZE"), 10, 64)

	if err != nil {
		fmt.Print(errors.New("Error parsing log size threshold"))
		value1 = 512*1024
	}

	logSizeThreshold = value1

	value2, err := strconv.ParseInt(os.Getenv("UNAPPLIED_LOG_ENTRIES"), 10, 64)

	if err != nil {
		fmt.Print(errors.New("Error parsing unapplied log entries threshold"))
		value2 = 2000
	}
	unappliedLogEntries = int(value2)
}

var raftState RaftState
var client = &http.Client{Timeout: 120 * time.Millisecond}

func initRaftState(nodeId string, role string, peers []string, stateDir string) {
	initConfig()
	persistantState := loadPersistantState(nodeId, stateDir)
	snapshotState := loadSnapshotFile(nodeId, stateDir)

	//init the store data from the snapshot
	Store.mu.Lock()
	Store.data = snapshotState.Data
	Store.appliedReqIDs = snapshotState.AppliedReqIDs
	Store.mu.Unlock()

	indexState := IndexState{
		CommitIndex: snapshotState.LastIncludedIndex,
		LastApplied: snapshotState.LastIncludedIndex,
	}

	leaderIndexState := LeaderIndexState{
		NextIndex:  make(map[string]int),
		MatchIndex: make(map[string]int),
	}

	snapshotMetadata := SnapshotState{
		LastIncludedIndex: snapshotState.LastIncludedIndex,
		LastIncludedTerm:  snapshotState.LastIncludedTerm,
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
		snapshotInFlight: make(map[string]bool),
		snapshotState:    snapshotMetadata,
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
	lastIndex := getLastLogIndexLocked()

	for i := lastIndex; i > raftState.indexState.CommitIndex; i-- {
		termAtI, ok := logTermAtLocked(i)
		if !ok || termAtI != raftState.persistentState.CurrentTerm {
			continue
		}

		replicationNumber := 1 //replicated myself (leader)

		for _, peer := range raftState.peers {
			if raftState.leaderIndexState.MatchIndex[peer] >= i {
				replicationNumber++
			}
		}

		//when majority, commit these entries to your log & apply to state machine
		//if the threshold are met as a leader, snapshot the log
		if replicationNumber > (len(raftState.peers)+1)/2 {
			raftState.indexState.CommitIndex = i
			applyCommittedEntriesLocked()

			//currently only snapshotting as a leader, followers depend on leaders to send installSnapshot RPC
			if raftState.role == "leader" {
				maySnapshotLocked()
			}
			return
		}
	}
}

func maySnapshotLocked() {
	snapshot := loadSnapshotFile(raftState.nodeId, raftState.stateDir)
	logPath := filepath.Join(raftState.stateDir, raftState.nodeId+".json")
	info, err := os.Stat(logPath)
	if err != nil {
		return
	}

	cooldownPassed := snapshot.LastSnapshotAt.IsZero() || time.Since(snapshot.LastSnapshotAt) >= time.Minute
	maxSizeReached := info.Size() > logSizeThreshold
	uncompactedEntriesReached := raftState.indexState.LastApplied-raftState.snapshotState.LastIncludedIndex > unappliedLogEntries

	if !cooldownPassed || (!maxSizeReached && !uncompactedEntriesReached) {
		return
	}

	lastIncludedIndex := raftState.indexState.LastApplied
	if lastIncludedIndex <= raftState.snapshotState.LastIncludedIndex {
		return
	}
	lastIncludedOffset := getLogOffsetLocked(lastIncludedIndex) //relative index (absolute - last snapshot index)

	lastLogEntry := raftState.persistentState.Log[lastIncludedOffset]

	Store.mu.Lock()
	//reset then copy to prevent stale data
	snapshot.Data = make(map[string]string)
	snapshot.AppliedReqIDs = make(map[string]AppliedResult)

	maps.Copy(snapshot.Data, Store.data)
	maps.Copy(snapshot.AppliedReqIDs, Store.appliedReqIDs)

	Store.mu.Unlock()

	snapshot.LastIncludedIndex = lastLogEntry.Index
	snapshot.LastIncludedTerm = lastLogEntry.Term
	snapshot.LastSnapshotAt = time.Now()

	err = saveSnapshotFile(raftState.nodeId, raftState.stateDir, snapshot)

	if err != nil {
		return
	}

	raftState.snapshotState = SnapshotState{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
	}

	//truncate the log
	raftState.persistentState.Log = append([]LogEntry(nil), raftState.persistentState.Log[lastIncludedOffset+1:]...)

	persistLocked() //save the updated log
}

// returns absolute log offset index based on last snapshot index [absolute index - last snapshotted log index]
func getLogOffsetLocked(index int) int {
	return index - raftState.snapshotState.LastIncludedIndex - 1
}

// returns index without log compaction (snapshotting) [snapshot index + current compacted log length]
func getLastLogIndexLocked() int {
	return raftState.snapshotState.LastIncludedIndex + len(raftState.persistentState.Log)
}

// returns the term at the log position after snapshotting (offset)
func logTermAtLocked(index int) (int, bool) {
	if index == raftState.snapshotState.LastIncludedIndex {
		return raftState.snapshotState.LastIncludedTerm, true
	}

	offset := getLogOffsetLocked(index)

	if offset >= len(raftState.persistentState.Log) || offset < 0 {
		return 0, false
	}

	return raftState.persistentState.Log[offset].Term, true
}

func applyCommittedEntriesLocked() {
	for raftState.indexState.LastApplied < raftState.indexState.CommitIndex {
		raftState.indexState.LastApplied++
		offset := getLogOffsetLocked(raftState.indexState.LastApplied)
		currCommand := raftState.persistentState.Log[offset].Command
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

/*
*
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
			Index:   getLastLogIndexLocked() + 1,
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

/*
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
	lastLogIndex := getLastLogIndexLocked()
	lastLogTerm := raftState.snapshotState.LastIncludedTerm //min term = term in snapshot file

	if len(raftState.persistentState.Log) > 0 {
		lastLogTerm = raftState.persistentState.Log[len(raftState.persistentState.Log)-1].Term
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
	lastLogIndex := getLastLogIndexLocked()
	lastLogTerm := raftState.snapshotState.LastIncludedTerm

	if len(raftState.persistentState.Log) > 0 {
		lastLogTerm = raftState.persistentState.Log[len(raftState.persistentState.Log)-1].Term
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
				nextIndex = getLastLogIndexLocked() + 1
				raftState.leaderIndexState.NextIndex[peer] = nextIndex
			}

			//when the follower log is behind, send the snapshot to update it (only if not sent yet)
			if nextIndex <= raftState.snapshotState.LastIncludedIndex {
				if raftState.snapshotInFlight[peer] {
					raftState.mu.Unlock()
					continue
				}
				raftState.snapshotInFlight[peer] = true
				raftState.mu.Unlock()
				go func(addr string) {
					defer func() {
						raftState.mu.Lock()
						raftState.snapshotInFlight[addr] = false
						raftState.mu.Unlock()
					}()
					_ = installSnapshot(addr)
				}(peer)
				continue
			}

			prevLogIndex := nextIndex - 1
			prevLogTerm := 0

			if prevLogIndex > 0 {
				prevLogTerm, _ = logTermAtLocked(prevLogIndex)
			}

			entries := append([]LogEntry(nil), raftState.persistentState.Log[getLogOffsetLocked(nextIndex):]...)

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

	if req.PrevLogIndex > getLastLogIndexLocked() {
		return AppendEntriesResponse{
			Term:    raftState.persistentState.CurrentTerm,
			Success: false,
		}
	}

	if req.PrevLogIndex > 0 {
		prevLogTerm, ok := logTermAtLocked(req.PrevLogIndex)
		if !ok || prevLogTerm != req.PrevLogTerm {
			return AppendEntriesResponse{
				Term:    raftState.persistentState.CurrentTerm,
				Success: false,
			}
		}
	}

	//remove the suffix and add the entries to the log
	insertAt := getLogOffsetLocked(req.PrevLogIndex) + 1 // 1-based prev index, so next entry starts here in 0-based slice
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
	raftState.indexState.CommitIndex = min(req.LeaderCommitIndex, getLastLogIndexLocked())

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

//extra stuff: snapshotting to reduce log size

func loadSnapshotFile(nodeId string, stateDir string) SnapshotFile {
	path := filepath.Join(stateDir, nodeId+".snapshot.json")

	bytes, err := os.ReadFile(path)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			snapshot := SnapshotFile{
				LastIncludedIndex: 0,
				LastIncludedTerm:  0,
				Data:              make(map[string]string),
				AppliedReqIDs:     make(map[string]AppliedResult),
			}

			return snapshot
		}
		panic(err)
	}

	var snapshot SnapshotFile

	if err := json.Unmarshal(bytes, &snapshot); err != nil {
		panic(err)
	}

	if snapshot.Data == nil {
		snapshot.Data = make(map[string]string)
	}

	if snapshot.AppliedReqIDs == nil {
		snapshot.AppliedReqIDs = make(map[string]AppliedResult)
	}

	return snapshot
}

func saveSnapshotFile(nodeId string, stateDir string, snapshot SnapshotFile) error {

	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}

	path := filepath.Join(stateDir, nodeId+".snapshot.json")

	bytes, err := json.MarshalIndent(snapshot, "", "	")

	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0o644)
}

/*
we send the snapshotfile chunks at a regular interval to the followers (who need it)
if they send back a term > currentTerm, we step down immediately
otherwise we move the nextIndex (& matchIndex) for that follower to lastIncludedIndex (+ 1)
*/
func installSnapshot(peer string) error {
	raftState.mu.Lock()
	term := raftState.persistentState.CurrentTerm
	leaderId := raftState.nodeId
	lastIncludedIndex := raftState.snapshotState.LastIncludedIndex
	lastIncludedTerm := raftState.snapshotState.LastIncludedTerm
	snapshotPath := filepath.Join(raftState.stateDir, raftState.nodeId+".snapshot.json")
	raftState.mu.Unlock()

	bytes, err := os.ReadFile(snapshotPath)

	if err != nil {
		return err
	}

	offset := 0
	chunkSize := 64 * 1024 //64KB

	for offset < len(bytes) {
		end := offset + chunkSize

		if end > len(bytes) { //clamp
			end = len(bytes)
		}

		chunk := bytes[offset:end]

		done := end == len(bytes)

		req := InstallSnapshotRequest{
			Term:              term,
			LeaderId:          leaderId,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Offset:            offset,
			Data:              chunk,
			Done:              done,
		}

		resp, err := sendSnapshot(peer, req)

		if err != nil {
			return err
		}

		if resp.Term > term {
			raftState.mu.Lock()
			if resp.Term > raftState.persistentState.CurrentTerm {
				stepDownLocked(resp.Term)
				raftState.mu.Unlock()
				return nil
			}
			raftState.mu.Unlock()
		}
		offset = end
	}

	raftState.mu.Lock()
	raftState.leaderIndexState.NextIndex[peer] = lastIncludedIndex + 1
	raftState.leaderIndexState.MatchIndex[peer] = lastIncludedIndex
	raftState.mu.Unlock()

	return nil
}

func sendSnapshot(addr string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {

	body, err := json.Marshal(req)

	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	url := "http://" + addr + "/snapshot"

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))

	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return InstallSnapshotResponse{}, errors.New("snapshot rpc failed")
	}

	var response InstallSnapshotResponse

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return InstallSnapshotResponse{}, err
	}

	return response, nil
}

func receiveSnapshot(req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	raftState.mu.Lock()
	stateDir := raftState.stateDir
	nodeId := raftState.nodeId
	if req.Term < raftState.persistentState.CurrentTerm {
		raftState.mu.Unlock()
		return InstallSnapshotResponse{
			Term: raftState.persistentState.CurrentTerm,
		}, nil
	}

	stepDownLocked(req.Term)
	term := raftState.persistentState.CurrentTerm
	resetElectionTimer()
	raftState.mu.Unlock()

	tempSnapshotPath := filepath.Join(stateDir, nodeId+".snapshot.tmp")

	if req.Offset == 0 {
		if err := os.MkdirAll(stateDir, 0o755); err != nil {
			return InstallSnapshotResponse{}, err
		}
	}

	flags := os.O_RDWR | os.O_CREATE

	if req.Offset == 0 {
		flags |= os.O_TRUNC
	}

	file, err := os.OpenFile(tempSnapshotPath, flags, 0o644)

	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	fileClosed := false
	defer func() {
		if !fileClosed {
			_ = file.Close()
		}
	}()

	// fileInfo, err := file.Stat()

	// if err != nil {
	// 	return InstallSnapshotResponse{Term: term}
	// }

	// //offset mismatch
	// if int(fileInfo.Size()) > req.Offset || int(fileInfo.Size()) < req.Offset {
	// 	return InstallSnapshotResponse{
	// 		Term: term,
	// 	}
	// }

	written, err := file.WriteAt(req.Data, int64(req.Offset))

	if err != nil || written != len(req.Data) {
		return InstallSnapshotResponse{}, err
	}

	//wait for more chunks before saving the file
	if req.Done == false {
		return InstallSnapshotResponse{
			Term: term,
		}, nil
	}

	//now the done is true, so we save the file
	finalSnapshotPath := filepath.Join(stateDir, nodeId+".snapshot.json")

	//sync then close for no unexpected issues later
	if err := file.Sync(); err != nil {
		return InstallSnapshotResponse{}, err
	}

	if err := file.Close(); err != nil {
		return InstallSnapshotResponse{}, err
	}
	fileClosed = true

	if err := os.Rename(tempSnapshotPath, finalSnapshotPath); err != nil {
		return InstallSnapshotResponse{}, err
	}

	snapshotFile := loadSnapshotFile(nodeId, stateDir)

	raftState.mu.Lock()
	//staleness leader check
	if req.Term < raftState.persistentState.CurrentTerm {
		raftState.mu.Unlock()
		return InstallSnapshotResponse{
			Term: raftState.persistentState.CurrentTerm,
		}, nil
	}

	log := raftState.persistentState.Log

	//find if the logs have the snapshot last included term and index in it
	i := 0
	matched := false
	for i = len(raftState.persistentState.Log) - 1; i >= 0; i-- {
		if log[i].Index == req.LastIncludedIndex && log[i].Term == req.LastIncludedTerm {
			matched = true
			break
		}
	}

	//discard the logs prefixing the index (if nothing found, delete the entire log[])
	if matched {
		raftState.persistentState.Log = append([]LogEntry(nil), log[i+1:]...)
	} else {
		raftState.persistentState.Log = []LogEntry{}
	}

	raftState.snapshotState.LastIncludedIndex = snapshotFile.LastIncludedIndex
	raftState.snapshotState.LastIncludedTerm = snapshotFile.LastIncludedTerm

	if raftState.indexState.CommitIndex < snapshotFile.LastIncludedIndex {
		raftState.indexState.CommitIndex = snapshotFile.LastIncludedIndex
	}

	if raftState.indexState.LastApplied < snapshotFile.LastIncludedIndex {
		raftState.indexState.LastApplied = snapshotFile.LastIncludedIndex
	}

	currentTerm := raftState.persistentState.CurrentTerm
	persistLocked()
	raftState.mu.Unlock()

	//update the state machine with the snapshot
	Store.mu.Lock()
	Store.data = snapshotFile.Data
	Store.appliedReqIDs = snapshotFile.AppliedReqIDs
	Store.mu.Unlock()

	return InstallSnapshotResponse{
		Term: currentTerm,
	}, nil
}
