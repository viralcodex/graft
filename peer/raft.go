package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const HEARTBEAT_INTERVAL = 100 * time.Millisecond

// config
var logSizeThreshold int64
var unappliedLogEntries int

func initRaftConfig() {
	logSizeThreshold = 512 * 1024
	if raw := os.Getenv("DEV_LOG_SIZE"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			fmt.Printf("invalid DEV_LOG_SIZE %q, using default %d\n", raw, logSizeThreshold)
		} else {
			logSizeThreshold = value
		}
	}

	unappliedLogEntries = 2000
	if raw := os.Getenv("UNAPPLIED_LOG_ENTRIES"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			fmt.Printf("invalid UNAPPLIED_LOG_ENTRIES %q, using default %d\n", raw, unappliedLogEntries)
		} else {
			unappliedLogEntries = int(value)
		}
	}
}

var raftState RaftState
var client = &http.Client{Timeout: 120 * time.Millisecond}

func initRaftState(nodeId string, role string, peers []string, stateDir string) {
	initRaftConfig()
	persistantState := loadPersistantState(nodeId, stateDir)
	snapshotFile := loadSnapshotFile(nodeId, stateDir)

	//reconciliate db and snapshot state then init indexState
	indexState := reconcileSnapshotState(snapshotFile, persistantState)

	leaderIndexState := LeaderIndexState{
		NextIndex:  make(map[string]int),
		MatchIndex: make(map[string]int),
	}

	snapshotMetadata := SnapshotState{
		LastIncludedIndex: snapshotFile.LastIncludedIndex,
		LastIncludedTerm:  snapshotFile.LastIncludedTerm,
		LastSnapshotAt:    snapshotFile.LastSnapshotAt,
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

	slog.Info("raft state initialized",
		"node", nodeId,
		"role", role,
		"term", persistantState.CurrentTerm,
		"peers", len(peers),
		"last_log_index", getLastLogIndexLocked(),
		"commit_index", indexState.CommitIndex,
		"last_applied", indexState.LastApplied,
		"snapshot_index", snapshotFile.LastIncludedIndex,
	)
}

func reconcileSnapshotState(snapshotFile SnapshotFile, persistentState PersistentState) IndexState {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	lastApplied, found, err := getRaftMetadata(ctx)

	if err != nil {
		panic(err)
	}

	if !found {
		lastApplied = 0
	}

	var reconciledLastAppliedIndex int

	if lastApplied == snapshotFile.LastIncludedIndex || lastApplied > snapshotFile.LastIncludedIndex {
		reconciledLastAppliedIndex = lastApplied
	}

	//bring DB state upto the snapshot when behind
	if lastApplied < snapshotFile.LastIncludedIndex {
		slog.Info("reconciling db from snapshot",
			"node", raftState.nodeId,
			"db_last_applied", lastApplied,
			"snapshot_index", snapshotFile.LastIncludedIndex,
		)
		if err = updateFromSnapshot(ctx, &snapshotFile); err != nil {
			slog.Error("failed to reconcile db from snapshot", "error", err.Error())
			panic(err)
		}
		reconciledLastAppliedIndex = snapshotFile.LastIncludedIndex
	}

	//if lastApplied is out of bounds from absolute log index, panic
	if lastApplied > snapshotFile.LastIncludedIndex+len(persistentState.Log) {
		slog.Error("invalid lastApplied value",
			"db_last_applied", lastApplied,
			"snapshot_index", snapshotFile.LastIncludedIndex,
			"log_len", len(persistentState.Log),
		)
		panic("Invalid lastApplied value")
	}

	return IndexState{
		CommitIndex: reconciledLastAppliedIndex,
		LastApplied: reconciledLastAppliedIndex,
	}
}

// keeping timeout between 500ms-800ms (raft paper has 150ms-300ms)
func randomTimeout() time.Duration {
	return time.Duration(500+rand.Intn(300)) * time.Millisecond
}

func resetElectionTimer() {
	raftState.timer.Reset(randomTimeout())
}

func stepDownLocked(newTerm int) {
	previousRole := raftState.role
	raftState.role = "follower"
	if newTerm > raftState.persistentState.CurrentTerm {
		raftState.persistentState.CurrentTerm = newTerm
		raftState.persistentState.VotedFor = ""
		persistLocked()
	}
	if previousRole != "follower" {
		slog.Info("stepping down",
			"node", raftState.nodeId,
			"new_role", raftState.role,
			"new_term", raftState.persistentState.CurrentTerm,
		)
	}
	resetElectionTimer()
}

func persistLocked() {
	if err := savePersistentState(raftState.persistentState, raftState.nodeId, raftState.stateDir); err != nil {
		panic(err)
	}
}

// applies committed commands to the state machine and advances LastApplied only after success
func applyCommittedCommands(commands []Command, appliedThrough int) {
	if len(commands) == 0 {
		return
	}

	//use a different mutex to prevent race condition on DB updates in db.go
	raftState.stateMachineMu.Lock()
	defer raftState.stateMachineMu.Unlock()

	startIndex := appliedThrough - len(commands) + 1
	for offset, command := range commands {
		applyToStore(command, startIndex+offset)
	}

	slog.Info("applied committed commands",
		"node", raftState.nodeId,
		"count", len(commands),
		"start_index", startIndex,
		"applied_through", appliedThrough,
	)

	raftState.mu.Lock()
	if appliedThrough > raftState.indexState.LastApplied {
		raftState.indexState.LastApplied = appliedThrough
	}
	raftState.mu.Unlock()
}

/*
collect the entries from commitIndex -> lastIndex (uncommitted entries) if majority nodes have replicated the entries sent from leader
and send it back for committing
*/
func advanceCommitIndexLocked() ([]Command, int, bool) {
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

		//when majority, return the commands to be applied
		//if the threshold are met as a leader, return the flag to snapshot the log
		if replicationNumber > (len(raftState.peers)+1)/2 {
			raftState.indexState.CommitIndex = i
			commands, appliedThrough := collectCommittedEntriesLocked()

			shouldSnapshot := raftState.role == "leader"

			return commands, appliedThrough, shouldSnapshot
		}
	}
	return nil, raftState.indexState.LastApplied, false
}
func buildSnapshotPlanLocked(logSize int64) (snapshotPlan, bool) {
	//thresholds
	cooldownPassed := raftState.snapshotState.LastSnapshotAt.IsZero() ||
		time.Since(raftState.snapshotState.LastSnapshotAt) >= time.Minute
	maxSizeReached := logSize > logSizeThreshold
	uncompactedEntriesReached := raftState.indexState.LastApplied-raftState.snapshotState.LastIncludedIndex > unappliedLogEntries

	if !cooldownPassed || (!maxSizeReached && !uncompactedEntriesReached) {
		return snapshotPlan{}, false
	}

	lastIncludedIndex := raftState.indexState.LastApplied
	if lastIncludedIndex <= raftState.snapshotState.LastIncludedIndex {
		return snapshotPlan{}, false
	}

	lastIncludedOffset := getLogOffsetLocked(lastIncludedIndex) //absolute index - last snapshotted index
	if lastIncludedOffset < 0 || lastIncludedOffset >= len(raftState.persistentState.Log) {
		return snapshotPlan{}, false
	}

	lastLogEntry := raftState.persistentState.Log[lastIncludedOffset]

	return snapshotPlan{
		nodeId:             raftState.nodeId,
		stateDir:           raftState.stateDir,
		lastIncludedIndex:  lastLogEntry.Index,
		lastIncludedTerm:   lastLogEntry.Term,
		lastIncludedOffset: lastIncludedOffset,
		baseSnapshotIndex:  raftState.snapshotState.LastIncludedIndex,
		baseSnapshotTerm:   raftState.snapshotState.LastIncludedTerm,
	}, true
}

func snapshotPlanStillValidLocked(plan snapshotPlan) bool {
	if raftState.snapshotState.LastIncludedIndex != plan.baseSnapshotIndex ||
		raftState.snapshotState.LastIncludedTerm != plan.baseSnapshotTerm ||
		plan.lastIncludedIndex <= raftState.snapshotState.LastIncludedIndex ||
		(plan.lastIncludedOffset < 0 || plan.lastIncludedOffset >= len(raftState.persistentState.Log)) {
		return false
	}

	entry := raftState.persistentState.Log[plan.lastIncludedOffset]
	return entry.Index == plan.lastIncludedIndex && entry.Term == plan.lastIncludedTerm
}

func maySnapshot() {
	logPath := filepath.Join(raftState.stateDir, raftState.nodeId+".json")
	info, err := os.Stat(logPath)
	if err != nil {
		return
	}

	raftState.stateMachineMu.Lock()
	defer raftState.stateMachineMu.Unlock()

	raftState.mu.Lock()
	plan, ok := buildSnapshotPlanLocked(info.Size())
	raftState.mu.Unlock()
	if !ok {
		return
	}

	snapshotFile := SnapshotFile{
		Data:          make(map[string]string),
		AppliedReqIDs: make(map[string]AppliedResult),
	}

	ctx, cancel := dbContext()
	defer cancel()

	if err := updateSnapshotFromDB(ctx, &snapshotFile); err != nil {
		panic(err)
	}

	snapshotFile.LastIncludedIndex = plan.lastIncludedIndex
	snapshotFile.LastIncludedTerm = plan.lastIncludedTerm
	snapshotFile.LastSnapshotAt = time.Now()

	tempSnapshotPath := filepath.Join(plan.stateDir, plan.nodeId+".snapshot.new")
	finalSnapshotPath := filepath.Join(plan.stateDir, plan.nodeId+".snapshot.json")

	if err := saveSnapshotFileAtPath(tempSnapshotPath, snapshotFile); err != nil {
		slog.Error("failed to write snapshot temp file",
			"node", raftState.nodeId,
			"path", tempSnapshotPath,
			"error", err.Error(),
		)
		return
	}

	raftState.mu.Lock()

	if !snapshotPlanStillValidLocked(plan) {
		raftState.mu.Unlock()
		_ = os.Remove(tempSnapshotPath)
		return
	}

	raftState.snapshotState = SnapshotState{
		LastIncludedIndex: snapshotFile.LastIncludedIndex,
		LastIncludedTerm:  snapshotFile.LastIncludedTerm,
		LastSnapshotAt:    snapshotFile.LastSnapshotAt,
	}

	raftState.persistentState.Log = append([]LogEntry(nil), raftState.persistentState.Log[plan.lastIncludedOffset+1:]...)

	persistLocked() //save the updated log
	raftState.mu.Unlock()

	//update the snapshot file now
	if err := os.Rename(tempSnapshotPath, finalSnapshotPath); err != nil {
		slog.Error("failed to publish snapshot",
			"node", raftState.nodeId,
			"path", finalSnapshotPath,
			"error", err.Error(),
		)
		panic(err)
	}

	slog.Info("snapshot published",
		"node", raftState.nodeId,
		"last_included_index", snapshotFile.LastIncludedIndex,
		"last_included_term", snapshotFile.LastIncludedTerm,
	)
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

func collectCommittedEntriesLocked() ([]Command, int) {
	var commands []Command
	nextApplied := raftState.indexState.LastApplied
	for nextApplied < raftState.indexState.CommitIndex {
		nextApplied++
		offset := getLogOffsetLocked(nextApplied)
		commands = append(commands, raftState.persistentState.Log[offset].Command)
	}
	return commands, nextApplied
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
func applyToStore(command Command, lastAppliedIndex int) AppliedResult {
	result := AppliedResult{
		Found: false,
	}

	if command.ReqId == "" {
		return result
	}

	ctx, cancel := dbContext()
	defer cancel()

	switch command.Operation {
	case "PUT":
		result, err := updateValue(ctx, command.Key, command.Value, command.ReqId, lastAppliedIndex)
		if err != nil {
			panic(err)
		}
		return result

	case "DELETE":
		result, err := deleteValue(ctx, command.Key, command.ReqId, lastAppliedIndex)
		if err != nil {
			panic(err)
		}
		return result

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
	//first dedupe check then proceed
	ctx, cancel := dbContext()
	defer cancel()

	_, found, err := appliedRequestExist(ctx, &command.ReqId)

	if err != nil {
		panic(err)
	}

	if found {
		slog.Info("command already applied",
			"node", raftState.nodeId,
			"req_id", command.ReqId,
			"operation", command.Operation,
			"key", command.Key,
		)
		return true
	}

	raftState.mu.Lock()

	if raftState.role != "leader" {
		slog.Info("No longer the leader", "node", raftState.nodeId, "term", raftState.persistentState.CurrentTerm)
		raftState.mu.Unlock()
		return false
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
		slog.Info("queued command",
			"node", raftState.nodeId,
			"req_id", command.ReqId,
			"operation", command.Operation,
			"key", command.Key,
			"index", entry.Index,
			"term", entry.Term,
		)
	}

	raftState.mu.Unlock()

	timeout := time.Now().Add(2 * time.Second)

	//keeps checking for 2s until timeout that the entry is committed or the leadership is lost
	for time.Now().Before(timeout) {
		raftState.mu.Lock()
		isSaved := raftState.indexState.LastApplied >= existingIndex
		isLeader := raftState.role == "leader" && raftState.persistentState.CurrentTerm == existingTerm
		raftState.mu.Unlock()

		if isSaved {
			slog.Info("command committed",
				"node", raftState.nodeId,
				"req_id", command.ReqId,
				"index", existingIndex,
			)
			return true
		}

		if !isLeader {
			slog.Info("leadership lost before command commit",
				"node", raftState.nodeId,
				"req_id", command.ReqId,
				"index", existingIndex,
			)
			return false
		}

		time.Sleep(10 * time.Millisecond)
	}

	slog.Error("command commit timed out",
		"node", raftState.nodeId,
		"req_id", command.ReqId,
		"index", existingIndex,
	)
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

	slog.Info("starting election",
		"node", nodeId,
		"term", term,
		"last_log_index", lastLogIndex,
		"last_log_term", lastLogTerm,
	)

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
				slog.Info("election aborted by higher term",
					"node", raftState.nodeId,
					"term", term,
					"higher_term", resp.Term,
				)
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
				slog.Info("became leader",
					"node", raftState.nodeId,
					"term", term,
					"votes", votes,
				)
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
				slog.Info("election lost",
					"node", raftState.nodeId,
					"term", term,
					"votes", votes,
					"remaining", remainingResponses,
				)
				stepDownLocked(term)
			}
			raftState.mu.Unlock()
			return
		}
	}

	//still candidate, stepdown
	raftState.mu.Lock()
	if raftState.role == "candidate" && raftState.persistentState.CurrentTerm == term {
		slog.Info("election timed out without majority",
			"node", raftState.nodeId,
			"term", term,
		)
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

	slog.Info("vote granted",
		"node", raftState.nodeId,
		"term", req.Term,
		"candidate", req.CandidateId,
	)

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
					slog.Info("sending snapshot to follower",
						"node", raftState.nodeId,
						"peer", addr,
						"snapshot_index", raftState.snapshotState.LastIncludedIndex,
					)
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
					slog.Error("append entries failed",
						"node", raftState.nodeId,
						"peer", addr,
						"term", req.Term,
						"error", err.Error(),
					)
					return
				}

				raftState.mu.Lock()

				if raftState.role != "leader" || raftState.persistentState.CurrentTerm != req.Term {
					raftState.mu.Unlock()
					return
				}

				if resp.Term > raftState.persistentState.CurrentTerm {
					stepDownLocked(resp.Term)
					raftState.mu.Unlock()
					return
				}

				//on success from the follower, we increase the next and match index ahead by how many entries are replicated for this node & advance the commit index
				//and apply the commands to the DB (and snapshot)
				if resp.Success {
					replicatedIndex := req.PrevLogIndex + len(req.Entries)
					raftState.leaderIndexState.MatchIndex[addr] = replicatedIndex
					raftState.leaderIndexState.NextIndex[addr] = replicatedIndex + 1

					commands, appliedThrough, shouldSnapshot := advanceCommitIndexLocked()
					raftState.mu.Unlock()

					applyCommittedCommands(commands, appliedThrough)

					if shouldSnapshot {
						maySnapshot()
					}
					return
				}

				//otherwise we backoff one index due to mismatch
				if raftState.leaderIndexState.NextIndex[addr] > 1 {
					slog.Info("append entries rejected, backing off next index",
						"node", raftState.nodeId,
						"peer", addr,
						"current_next_index", raftState.leaderIndexState.NextIndex[addr],
					)
					raftState.leaderIndexState.NextIndex[addr]--
				}
				raftState.mu.Unlock()
			}(peer, appendReq)
		}
		time.Sleep(HEARTBEAT_INTERVAL) //keeping it 100ms for now
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

	if req.Term < raftState.persistentState.CurrentTerm {
		slog.Info("rejecting stale append entries",
			"node", raftState.nodeId,
			"request_term", req.Term,
			"current_term", raftState.persistentState.CurrentTerm,
		)
		currentTerm := raftState.persistentState.CurrentTerm
		raftState.mu.Unlock()
		return AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	}

	stepDownLocked(req.Term)
	if req.PrevLogIndex > getLastLogIndexLocked() {
		currentTerm := raftState.persistentState.CurrentTerm
		raftState.mu.Unlock()
		return AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	}

	if req.PrevLogIndex > 0 {
		prevLogTerm, ok := logTermAtLocked(req.PrevLogIndex)
		if !ok || prevLogTerm != req.PrevLogTerm {
			currentTerm := raftState.persistentState.CurrentTerm
			raftState.mu.Unlock()
			return AppendEntriesResponse{
				Term:    currentTerm,
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

	commands, appliedThrough := collectCommittedEntriesLocked()
	currentTerm := raftState.persistentState.CurrentTerm

	raftState.mu.Unlock()

	//apply commands to DB
	applyCommittedCommands(commands, appliedThrough)

	slog.Info("append entries applied",
		"node", raftState.nodeId,
		"leader", req.LeaderId,
		"commit_index", raftState.indexState.CommitIndex,
		"last_applied", appliedThrough,
		"entries", len(req.Entries),
	)

	return AppendEntriesResponse{
		Term:    currentTerm,
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
	return loadSnapshotFileAtPath(path)
}

func loadSnapshotFileAtPath(path string) SnapshotFile {
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
	path := filepath.Join(stateDir, nodeId+".snapshot.json")
	return saveSnapshotFileAtPath(path, snapshot)
}

func saveSnapshotFileAtPath(path string, snapshot SnapshotFile) error {
	stateDir := filepath.Dir(path)

	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}

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

	slog.Info("starting snapshot install",
		"node", leaderId,
		"peer", peer,
		"last_included_index", lastIncludedIndex,
		"last_included_term", lastIncludedTerm,
	)

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

	slog.Info("snapshot install completed",
		"node", leaderId,
		"peer", peer,
		"last_included_index", lastIncludedIndex,
	)

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

	//now the done is true, so we can publish the snapshot after the matching raft state is durable
	finalSnapshotPath := filepath.Join(stateDir, nodeId+".snapshot.json")

	//sync then close for no unexpected issues later
	if err := file.Sync(); err != nil {
		return InstallSnapshotResponse{}, err
	}
	if err := file.Close(); err != nil {
		return InstallSnapshotResponse{}, err
	}

	fileClosed = true

	snapshotFile := loadSnapshotFileAtPath(tempSnapshotPath)
	raftState.stateMachineMu.Lock()
	defer raftState.stateMachineMu.Unlock()

	ctx, cancel := dbContext()
	defer cancel()

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

	//update DB then update the lastIncludedIndex
	if err := updateFromSnapshot(ctx, &snapshotFile); err != nil {
		panic(err)
	}

	if raftState.indexState.LastApplied < snapshotFile.LastIncludedIndex {
		raftState.indexState.LastApplied = snapshotFile.LastIncludedIndex
	}

	currentTerm := raftState.persistentState.CurrentTerm
	persistLocked()
	raftState.mu.Unlock()

	if err := os.Rename(tempSnapshotPath, finalSnapshotPath); err != nil {
		return InstallSnapshotResponse{}, err
	}

	slog.Info("snapshot received and published",
		"node", nodeId,
		"leader", req.LeaderId,
		"last_included_index", snapshotFile.LastIncludedIndex,
		"last_included_term", snapshotFile.LastIncludedTerm,
	)

	return InstallSnapshotResponse{
		Term: currentTerm,
	}, nil
}
