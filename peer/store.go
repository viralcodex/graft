package main

import (
	"sync"
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

func initialise() *KVStore {
	return &KVStore{
		data: make(map[string]string),
		appliedReqIDs: make(map[string]AppliedResult),
	}
}

var Store = initialise()
