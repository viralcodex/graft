package main

import (
	"sync"
	"time"
)

//server.go
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

/************/

//raft.go


type RaftState struct {
	mu             sync.Mutex
	stateMachineMu sync.Mutex //this locks when we are doing DB ops

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
	LastSnapshotAt    time.Time
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

// temp struct needed
type snapshotPlan struct {
	nodeId             string
	stateDir           string
	lastIncludedIndex  int
	lastIncludedTerm   int
	lastIncludedOffset int
	baseSnapshotIndex  int
	baseSnapshotTerm   int
}

