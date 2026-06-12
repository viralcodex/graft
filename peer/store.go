package main

import (
	"time"
)


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

func initialiseSnapshot() *SnapshotFile {
	return &SnapshotFile{
		Data:          make(map[string]string),
		AppliedReqIDs: make(map[string]AppliedResult),
	}
}

var SnapShotFile = initialiseSnapshot()
