package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReceiveSnapshotRejectsStaleTerm(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "leader")

	raftState.mu.Lock()
	raftState.persistentState.CurrentTerm = 5
	persistLocked()
	raftState.mu.Unlock()

	resp, err := receiveSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderId:          "node2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              []byte("ignored"),
		Done:              true,
	})
	if err != nil {
		t.Fatalf("receiveSnapshot returned error: %v", err)
	}
	if resp.Term != 5 {
		t.Fatalf("expected stale snapshot response term 5, got %d", resp.Term)
	}

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.role != "leader" {
		t.Fatalf("expected stale snapshot to leave role unchanged, got %q", raftState.role)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no snapshot file to be created, stat err=%v", err)
	}
}

func TestReceiveSnapshotPublishesOnlyAfterDone(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "follower")

	raftState.mu.Lock()
	raftState.persistentState.CurrentTerm = 2
	raftState.persistentState.Log = []LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 2, Index: 3},
	}
	raftState.indexState.CommitIndex = 0
	raftState.indexState.LastApplied = 0
	persistLocked()
	raftState.mu.Unlock()

	seedSnapshotState(t, SnapshotFile{
		Data:          map[string]string{"before": "value"},
		AppliedReqIDs: map[string]AppliedResult{"before": {Value: "value"}},
	})

	payload := mustSnapshotPayload(t, SnapshotFile{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              map[string]string{"after": "value"},
		AppliedReqIDs:     map[string]AppliedResult{"after": {Value: "value"}},
	})
	middle := len(payload) / 2

	resp, err := receiveSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderId:          "node2",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              payload[:middle],
		Done:              false,
	})
	if err != nil {
		t.Fatalf("receiveSnapshot first chunk returned error: %v", err)
	}
	if resp.Term != 2 {
		t.Fatalf("expected first chunk response term 2, got %d", resp.Term)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.tmp")); err != nil {
		t.Fatalf("expected temp snapshot file after partial chunk: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no final snapshot file before done chunk, stat err=%v", err)
	}

	assertDBValue(t, "before", "value", true)
	assertDBValue(t, "after", "", false)
	assertAppliedRequestState(t, "before", AppliedResult{Value: "value"}, true)
	assertAppliedRequestState(t, "after", AppliedResult{}, false)

	raftState.mu.Lock()
	if raftState.snapshotState.LastIncludedIndex != 0 {
		raftState.mu.Unlock()
		t.Fatalf("expected snapshot state to remain unchanged before done chunk, got %d", raftState.snapshotState.LastIncludedIndex)
	}
	raftState.mu.Unlock()

	resp, err = receiveSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderId:          "node2",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Offset:            middle,
		Data:              payload[middle:],
		Done:              true,
	})
	if err != nil {
		t.Fatalf("receiveSnapshot final chunk returned error: %v", err)
	}
	if resp.Term != 2 {
		t.Fatalf("expected final chunk response term 2, got %d", resp.Term)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.tmp")); !os.IsNotExist(err) {
		t.Fatalf("expected temp snapshot file to be removed after final chunk, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.json")); err != nil {
		t.Fatalf("expected final snapshot file after done chunk: %v", err)
	}

	assertDBValue(t, "after", "value", true)
	assertDBValue(t, "before", "", false)
	assertAppliedRequestState(t, "after", AppliedResult{Value: "value"}, true)
	assertAppliedRequestState(t, "before", AppliedResult{}, false)

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.snapshotState.LastIncludedIndex != 2 {
		t.Fatalf("expected snapshot index 2 after install, got %d", raftState.snapshotState.LastIncludedIndex)
	}
	if raftState.indexState.CommitIndex != 2 {
		t.Fatalf("expected commit index to advance to 2, got %d", raftState.indexState.CommitIndex)
	}
	if raftState.indexState.LastApplied != 2 {
		t.Fatalf("expected last applied to advance to 2, got %d", raftState.indexState.LastApplied)
	}
	if len(raftState.persistentState.Log) != 1 || raftState.persistentState.Log[0].Index != 3 {
		t.Fatalf("expected matching log prefix to be compacted, got %+v", raftState.persistentState.Log)
	}
}

func TestMaySnapshotLockedRewritesSnapshotWithoutStaleKeys(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "leader")

	oldLogSizeThreshold := logSizeThreshold
	oldUnappliedLogEntries := unappliedLogEntries
	logSizeThreshold = 0
	unappliedLogEntries = 0
	t.Cleanup(func() {
		logSizeThreshold = oldLogSizeThreshold
		unappliedLogEntries = oldUnappliedLogEntries
	})

	err := saveSnapshotFile("node1", stateDir, SnapshotFile{
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		LastSnapshotAt:    time.Time{},
		Data:              map[string]string{"stale": "old"},
		AppliedReqIDs:     map[string]AppliedResult{"stale": {Value: "old"}},
	})
	if err != nil {
		t.Fatalf("save initial snapshot: %v", err)
	}

	seedSnapshotState(t, SnapshotFile{
		Data:          map[string]string{"fresh": "value"},
		AppliedReqIDs: map[string]AppliedResult{"fresh": {Value: "value"}},
	})

	raftState.mu.Lock()
	raftState.persistentState.Log = []LogEntry{{
		Term:  3,
		Index: 1,
		Command: Command{
			ReqId:     "fresh",
			Operation: "PUT",
			Key:       "fresh",
			Value:     "value",
		},
	}}
	raftState.indexState.CommitIndex = 1
	raftState.indexState.LastApplied = 1
	persistLocked()
	raftState.mu.Unlock()

	maySnapshot()

	snapshot := loadSnapshotFile("node1", stateDir)
	if got := snapshot.Data["fresh"]; got != "value" {
		t.Fatalf("expected rewritten snapshot to contain fresh key, got %q", got)
	}
	if _, ok := snapshot.Data["stale"]; ok {
		t.Fatal("expected rewritten snapshot to drop stale keys")
	}
	if _, ok := snapshot.AppliedReqIDs["stale"]; ok {
		t.Fatal("expected rewritten snapshot to drop stale request ids")
	}

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.snapshotState.LastIncludedIndex != 1 {
		t.Fatalf("expected snapshot metadata to advance to index 1, got %d", raftState.snapshotState.LastIncludedIndex)
	}
	if len(raftState.persistentState.Log) != 0 {
		t.Fatalf("expected log to be truncated after local snapshot, got %+v", raftState.persistentState.Log)
	}
}

func TestInitRaftStateRepairsDBWhenMetadataBehindSnapshot(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "follower")

	snapshot := SnapshotFile{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              map[string]string{"snapshot-key": "snapshot-value"},
		AppliedReqIDs:     map[string]AppliedResult{"snapshot-req": {Found: true, Value: "snapshot-value"}},
	}
	if err := saveSnapshotFile("node1", stateDir, snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := savePersistentState(PersistentState{
		CurrentTerm: 2,
		VotedFor:    "",
		Log:         []LogEntry{},
	}, "node1", stateDir); err != nil {
		t.Fatalf("save persistent state: %v", err)
	}

	seedSnapshotState(t, SnapshotFile{
		Data:          map[string]string{"stale-key": "stale-value"},
		AppliedReqIDs: map[string]AppliedResult{"stale-req": {Found: true, Value: "stale-value"}},
	})
	setRaftMetadata(t, 0)

	initRaftState("node1", "follower", nil, stateDir)
	if raftState.timer != nil {
		raftState.timer.Stop()
	}

	assertDBValue(t, "snapshot-key", "snapshot-value", true)
	assertDBValue(t, "stale-key", "", false)
	assertAppliedRequestState(t, "snapshot-req", AppliedResult{Found: true, Value: "snapshot-value"}, true)
	assertAppliedRequestState(t, "stale-req", AppliedResult{}, false)
	assertRaftMetadata(t, 2)

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.indexState.CommitIndex != 2 {
		t.Fatalf("expected commit index 2 after repair, got %d", raftState.indexState.CommitIndex)
	}
	if raftState.indexState.LastApplied != 2 {
		t.Fatalf("expected last applied 2 after repair, got %d", raftState.indexState.LastApplied)
	}
	if raftState.snapshotState.LastIncludedIndex != 2 {
		t.Fatalf("expected snapshot index 2 after repair, got %d", raftState.snapshotState.LastIncludedIndex)
	}
}

func TestInitRaftStateUsesDBMetadataAheadOfSnapshot(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "follower")

	snapshot := SnapshotFile{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              map[string]string{"snapshot-key": "snapshot-value"},
		AppliedReqIDs:     map[string]AppliedResult{"snapshot-req": {Found: true, Value: "snapshot-value"}},
	}
	if err := saveSnapshotFile("node1", stateDir, snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := savePersistentState(PersistentState{
		CurrentTerm: 3,
		VotedFor:    "",
		Log: []LogEntry{
			{
				Term:  2,
				Index: 3,
				Command: Command{
					ReqId:     "req-3",
					Operation: "PUT",
					Key:       "post-snapshot-key",
					Value:     "post-snapshot-value",
				},
			},
			{
				Term:  3,
				Index: 4,
				Command: Command{
					ReqId:     "req-4",
					Operation: "PUT",
					Key:       "ahead-key",
					Value:     "ahead-value",
				},
			},
		},
	}, "node1", stateDir); err != nil {
		t.Fatalf("save persistent state: %v", err)
	}

	seedSnapshotState(t, snapshot)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := updateValue(ctx, "ahead-key", "ahead-value", "req-4", 4); err != nil {
		t.Fatalf("advance db metadata ahead of snapshot: %v", err)
	}

	initRaftState("node1", "follower", nil, stateDir)
	if raftState.timer != nil {
		raftState.timer.Stop()
	}

	assertDBValue(t, "snapshot-key", "snapshot-value", true)
	assertDBValue(t, "ahead-key", "ahead-value", true)
	assertRaftMetadata(t, 4)

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.indexState.CommitIndex != 4 {
		t.Fatalf("expected commit index 4 from db metadata, got %d", raftState.indexState.CommitIndex)
	}
	if raftState.indexState.LastApplied != 4 {
		t.Fatalf("expected last applied 4 from db metadata, got %d", raftState.indexState.LastApplied)
	}
	if raftState.snapshotState.LastIncludedIndex != 2 {
		t.Fatalf("expected snapshot index to remain 2, got %d", raftState.snapshotState.LastIncludedIndex)
	}
}

func TestInitRaftStatePanicsWhenMetadataExceedsRetainedLog(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "follower")

	if err := saveSnapshotFile("node1", stateDir, SnapshotFile{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              map[string]string{"snapshot-key": "snapshot-value"},
		AppliedReqIDs:     map[string]AppliedResult{"snapshot-req": {Found: true, Value: "snapshot-value"}},
	}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := savePersistentState(PersistentState{
		CurrentTerm: 3,
		VotedFor:    "",
		Log: []LogEntry{{
			Term:  2,
			Index: 3,
			Command: Command{
				ReqId:     "req-3",
				Operation: "PUT",
				Key:       "post-snapshot-key",
				Value:     "post-snapshot-value",
			},
		}},
	}, "node1", stateDir); err != nil {
		t.Fatalf("save persistent state: %v", err)
	}
	setRaftMetadata(t, 4)

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected initRaftState to panic when metadata exceeds retained log")
		}
	}()

	initRaftState("node1", "follower", nil, stateDir)
}

func setupSnapshotTestState(t *testing.T, role string) string {
	t.Helper()

	backend := startSnapshotTestPostgres(t)

	oldDatabaseURL, hadDatabaseURL := os.LookupEnv("DATABASE_URL")
	if err := os.Setenv("DATABASE_URL", backend.url); err != nil {
		t.Fatalf("set DATABASE_URL: %v", err)
	}
	t.Cleanup(func() {
		if hadDatabaseURL {
			_ = os.Setenv("DATABASE_URL", oldDatabaseURL)
			return
		}
		_ = os.Unsetenv("DATABASE_URL")
	})

	if dbPool != nil {
		dbPool.Close()
		dbPool = nil
	}

	if err := initDB(); err != nil {
		t.Fatalf("init test db: %v", err)
	}

	stateDir := t.TempDir()
	initRaftState("node1", role, nil, stateDir)
	if raftState.timer != nil {
		raftState.timer.Stop()
	}

	t.Cleanup(func() {
		if raftState.timer != nil {
			raftState.timer.Stop()
		}
		if dbPool != nil {
			dbPool.Close()
			dbPool = nil
		}
	})

	return stateDir
}

func mustSnapshotPayload(t *testing.T, snapshot SnapshotFile) []byte {
	t.Helper()

	bytes, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot payload: %v", err)
	}

	return bytes
}

type snapshotTestPostgres struct {
	name string
	port int
	url  string
}

func startSnapshotTestPostgres(t *testing.T) snapshotTestPostgres {
	t.Helper()

	ensureSnapshotTestDocker(t)
	port := reserveSnapshotTestPort(t)
	container := snapshotTestPostgres{
		name: fmt.Sprintf("raft-peer-snapshot-%d", time.Now().UnixNano()),
		port: port,
		url:  fmt.Sprintf("postgres://raft:raft@127.0.0.1:%d/raft?sslmode=disable", port),
	}

	cmd := exec.Command(
		"docker", "run", "-d",
		"--name", container.name,
		"-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"--health-cmd=pg_isready -U raft -d raft",
		"--health-interval=1s",
		"--health-timeout=2s",
		"--health-retries=30",
		"-e", "POSTGRES_DB=raft",
		"-e", "POSTGRES_USER=raft",
		"-e", "POSTGRES_PASSWORD=raft",
		"postgres:16-alpine",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("start snapshot test postgres: %v\n%s", err, output)
	}

	waitForSnapshotTestPostgres(t, container.name)

	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", container.name).Run()
	})

	return container
}

func ensureSnapshotTestDocker(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is required for snapshot tests")
	}

	cmd := exec.Command("docker", "version", "--format", "{{.Server.Version}}")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("docker server is unavailable: %v\n%s", err, output)
	}
}

func waitForSnapshotTestPostgres(t *testing.T, name string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "inspect", "-f", "{{.State.Health.Status}}", name)
		output, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(output)) == "healthy" {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
	t.Fatalf("snapshot test postgres did not become healthy:\n%s", logs)
}

func reserveSnapshotTestPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve snapshot test port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func seedSnapshotState(t *testing.T, snapshot SnapshotFile) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := updateFromSnapshot(ctx, &snapshot); err != nil {
		t.Fatalf("seed snapshot state: %v", err)
	}
}

func assertDBValue(t *testing.T, key string, wantValue string, wantFound bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	value, found, err := getValue(ctx, key)
	if err != nil {
		t.Fatalf("get db value for %q: %v", key, err)
	}
	if found != wantFound {
		t.Fatalf("expected found=%t for key %q, got %t", wantFound, key, found)
	}
	if found && value != wantValue {
		t.Fatalf("expected value %q for key %q, got %q", wantValue, key, value)
	}
}

func assertAppliedRequestState(t *testing.T, reqID string, want AppliedResult, wantFound bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, found, err := appliedRequestExist(ctx, &reqID)
	if err != nil {
		t.Fatalf("get applied request for %q: %v", reqID, err)
	}
	if found != wantFound {
		t.Fatalf("expected applied request found=%t for %q, got %t", wantFound, reqID, found)
	}
	if found && result != want {
		t.Fatalf("expected applied request %+v for %q, got %+v", want, reqID, result)
	}
}

func assertRaftMetadata(t *testing.T, want int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	lastApplied, found, err := getRaftMetadata(ctx)
	if err != nil {
		t.Fatalf("get raft metadata: %v", err)
	}
	if !found {
		t.Fatal("expected raft metadata row to exist")
	}
	if lastApplied != want {
		t.Fatalf("expected raft metadata last_applied=%d, got %d", want, lastApplied)
	}
}

func setRaftMetadata(t *testing.T, lastApplied int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := dbPool.Exec(ctx, `TRUNCATE TABLE raft_metadata`); err != nil {
		t.Fatalf("truncate raft metadata: %v", err)
	}
	if _, err := dbPool.Exec(ctx, `INSERT INTO raft_metadata (id, last_applied) VALUES (TRUE, $1)`, lastApplied); err != nil {
		t.Fatalf("insert raft metadata: %v", err)
	}
}
