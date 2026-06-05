package main

import (
	"sync"
	"time"
)

type KVStore struct {
	mu            sync.RWMutex
	data          map[string]string        //current in-memory data
	appliedReqIDs map[string]AppliedResult //stores already committed operations to the node's state machines
}

type AppliedResult struct {
	Found bool   `json:"found,omitempty"`
	Value string `json:"value,omitempty"`
}

type SnapshotFile struct {
	LastIncludedIndex int                      `json:"lastIncludedIndex"`
	LastIncludedTerm  int                      `json:"lastIncludedTerm"`
	LastSnapshotAt    time.Time                `json:"lastSnapshotAt"`
	Data              map[string]string        `json:"data"`
	AppliedReqIDs     map[string]AppliedResult `json:"appliedReqIds"`
}

func initialise() *KVStore {
	return &KVStore{
		data:          make(map[string]string),
		appliedReqIDs: make(map[string]AppliedResult),
	}
}

func initialiseSnapshot() *SnapshotFile {
	return &SnapshotFile{
		Data:          make(map[string]string),
		AppliedReqIDs: make(map[string]AppliedResult),
	}
}

var Store = initialise()
var SnapShotFile = initialiseSnapshot()
