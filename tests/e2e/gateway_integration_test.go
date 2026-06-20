package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

type stateResponse struct {
	Leader    string   `json:"leader"`
	Followers []string `json:"followers"`
}

type kvResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type persistedState struct {
	CurrentTerm int              `json:"currentTerm"`
	VotedFor    string           `json:"votedFor"`
	Log         []persistedEntry `json:"log"`
}

type persistedEntry struct {
	Term  int `json:"term"`
	Index int `json:"index"`
}

type persistedSnapshot struct {
	LastIncludedIndex int               `json:"lastIncludedIndex"`
	LastIncludedTerm  int               `json:"lastIncludedTerm"`
	Data              map[string]string `json:"data"`
	AppliedReqIDs     map[string]any    `json:"appliedReqIds"`
}

type clusterOptions struct {
	peerEnv map[string]string
}

type postgresBackend struct {
	name string
	port int
}

type clusterHandle struct {
	processes     []*clusterProcess
	postgres      []*postgresBackend
	peerEnvByNode map[string]map[string]string
}

type clusterProcess struct {
	name   string
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func TestGatewayClientFlow(t *testing.T) {
	runGatewayClientFlow(t, 3, "test-key", "test-value")
}

func TestGatewayClientFlowWithSplitVotePressure(t *testing.T) {
	runGatewayClientFlow(t, 4, "split-vote-key", "split-vote-value")
}

func TestGatewayClientFlowWithLeaderFailover(t *testing.T) {
	runGatewayLeaderFailoverFlow(t, 3, "failover-key", "failover-value")
}

func TestLeaderCreatesSnapshotAndCompactsLog(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	defer logTestResult(t)

	repoRoot := repoRoot(t)
	binaries := buildBinaries(t, repoRoot)
	stateDir := filepath.Join(t.TempDir(), "raft-state")
	ports := reservePorts(t, 4)
	gatewayPort := ports[0]
	peerPorts := ports[1:]
	cluster := clusterAddrs(peerPorts)
	clusterRun := startClusterWithOptions(t, repoRoot, binaries, gatewayPort, peerPorts, cluster, stateDir, clusterOptions{
		peerEnv: map[string]string{
			"DEV_LOG_SIZE":          strconv.FormatInt(1<<30, 10),
			"UNAPPLIED_LOG_ENTRIES": "1",
		},
	})
	defer stopCluster(t, clusterRun)

	client := &http.Client{Timeout: time.Second}
	baseURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	state := waitForLeader(t, client, baseURL, len(cluster)-1, 10*time.Second)

	for idx := 0; idx < 3; idx++ {
		key := fmt.Sprintf("snapshot-key-%d", idx)
		value := fmt.Sprintf("snapshot-value-%d", idx)
		putResp := putValue(t, client, baseURL, key, value)
		if putResp.Key != key || putResp.Value != value {
			t.Fatalf("unexpected PUT response: %+v", putResp)
		}
		for _, addr := range cluster {
			waitForPeerValue(t, client, addr, key, value, 5*time.Second)
		}
	}

	leaderNodeID := nodeIDForAddr(t, cluster, state.Leader)
	leaderStateDir := filepath.Join(stateDir, leaderNodeID)

	waitForSnapshotState(t, leaderStateDir, leaderNodeID, func(snapshot persistedSnapshot, persisted persistedState) bool {
		return snapshot.LastIncludedIndex >= 2 && len(persisted.Log) <= 1
	}, 10*time.Second)

	snapshot := readSnapshotFile(t, leaderStateDir, leaderNodeID)
	persisted := readPersistentStateFile(t, leaderStateDir, leaderNodeID)

	if snapshot.LastIncludedIndex < 2 {
		t.Fatalf("expected leader snapshot to include at least 2 entries, got %d", snapshot.LastIncludedIndex)
	}
	if got := snapshot.Data["snapshot-key-1"]; got != "snapshot-value-1" {
		t.Fatalf("expected snapshot to include compacted value, got %q", got)
	}
	if len(persisted.Log) > 1 {
		t.Fatalf("expected leader log to be compacted, got %d entries", len(persisted.Log))
	}
	if len(persisted.Log) == 1 && persisted.Log[0].Index <= snapshot.LastIncludedIndex {
		t.Fatalf("expected remaining log entry to be after snapshot index %d, got %+v", snapshot.LastIncludedIndex, persisted.Log[0])
	}

	logEvent(t, "PASS snapshot-created", "leader=%s snapshotIndex=%d remainingLog=%d", state.Leader, snapshot.LastIncludedIndex, len(persisted.Log))
}

func TestFollowerCatchesUpFromSnapshotAfterRestart(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	defer logTestResult(t)

	repoRoot := repoRoot(t)
	binaries := buildBinaries(t, repoRoot)
	stateDir := filepath.Join(t.TempDir(), "raft-state")
	ports := reservePorts(t, 4)
	gatewayPort := ports[0]
	peerPorts := ports[1:]
	cluster := clusterAddrs(peerPorts)
	peerEnv := map[string]string{
		"DEV_LOG_SIZE":          strconv.FormatInt(1<<30, 10),
		"UNAPPLIED_LOG_ENTRIES": "1",
	}
	clusterRun := startClusterWithOptions(t, repoRoot, binaries, gatewayPort, peerPorts, cluster, stateDir, clusterOptions{peerEnv: peerEnv})
	defer stopCluster(t, clusterRun)

	client := &http.Client{Timeout: time.Second}
	baseURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	state := waitForLeader(t, client, baseURL, len(cluster)-1, 10*time.Second)
	if len(state.Followers) == 0 {
		t.Fatal("expected at least one follower")
	}

	laggingFollowerAddr := state.Followers[0]
	laggingFollowerNodeID := nodeIDForAddr(t, cluster, laggingFollowerAddr)
	laggingFollowerProc := peerProcessForAddr(t, clusterRun.processes, cluster, laggingFollowerAddr)
	stopProcess(laggingFollowerProc)

	keys := []string{"offline-key-0", "offline-key-1", "offline-key-2"}
	for idx, key := range keys {
		value := fmt.Sprintf("offline-value-%d", idx)
		putResp := putValue(t, client, baseURL, key, value)
		if putResp.Key != key || putResp.Value != value {
			t.Fatalf("unexpected PUT response: %+v", putResp)
		}
	}

	leaderState := getState(t, client, baseURL)
	leaderNodeID := nodeIDForAddr(t, cluster, leaderState.Leader)
	leaderStateDir := filepath.Join(stateDir, leaderNodeID)
	waitForSnapshotState(t, leaderStateDir, leaderNodeID, func(snapshot persistedSnapshot, persisted persistedState) bool {
		return snapshot.LastIncludedIndex >= 2 && len(persisted.Log) <= 1
	}, 10*time.Second)

	restartedFollower := startProcess(t, repoRoot, laggingFollowerNodeID, binaries["peer"], clusterRun.peerEnvByNode[laggingFollowerNodeID],
		"-port", strconv.Itoa(portForAddr(t, laggingFollowerAddr)),
		"-nodeId", laggingFollowerNodeID,
		"-cluster", peerClusterFlag(cluster, nodeIndexForID(t, laggingFollowerNodeID)),
		"-stateDir", filepath.Join(stateDir, laggingFollowerNodeID),
	)
	replaceProcessForAddr(t, clusterRun.processes, cluster, laggingFollowerAddr, restartedFollower)
	waitForEndpoint(t, fmt.Sprintf("http://%s/health", laggingFollowerAddr), 5*time.Second)

	for idx, key := range keys {
		waitForPeerValue(t, client, laggingFollowerAddr, key, fmt.Sprintf("offline-value-%d", idx), 10*time.Second)
	}

	laggingSnapshot := readSnapshotFile(t, filepath.Join(stateDir, laggingFollowerNodeID), laggingFollowerNodeID)
	if laggingSnapshot.LastIncludedIndex < 2 {
		t.Fatalf("expected restarted follower snapshot to be installed, got index %d", laggingSnapshot.LastIncludedIndex)
	}
	if got := laggingSnapshot.Data[keys[1]]; got != "offline-value-1" {
		t.Fatalf("expected follower snapshot to contain compacted value, got %q", got)
	}

	logEvent(t, "PASS snapshot-restart", "follower=%s snapshotIndex=%d", laggingFollowerAddr, laggingSnapshot.LastIncludedIndex)
}

func runGatewayClientFlow(t *testing.T, peerCount int, key string, value string) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	defer logTestResult(t)

	repoRoot := repoRoot(t)
	binaries := buildBinaries(t, repoRoot)
	stateDir := filepath.Join(t.TempDir(), "raft-state")
	ports := reservePorts(t, peerCount+1)
	gatewayPort := ports[0]
	peerPorts := ports[1:]
	cluster := clusterAddrs(peerPorts)

	clusterRun := startCluster(t, repoRoot, binaries, gatewayPort, peerPorts, cluster, stateDir)
	defer stopCluster(t, clusterRun)

	client := &http.Client{Timeout: time.Second}
	baseURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	state := waitForLeader(t, client, baseURL, len(cluster)-1, 10*time.Second)
	if state.Leader == "" {
		t.Fatal("gateway state did not report a leader")
	}

	knownNodes := make(map[string]struct{}, len(cluster))
	for _, addr := range cluster {
		knownNodes[addr] = struct{}{}
	}
	if _, ok := knownNodes[state.Leader]; !ok {
		t.Fatalf("state leader %q not in cluster %v", state.Leader, cluster)
	}
	assertFollowers(t, state, knownNodes, len(cluster)-1)

	putResp := putValue(t, client, baseURL, key, value)
	if putResp.Key != key || putResp.Value != value {
		t.Fatalf("unexpected PUT response: %+v", putResp)
	}

	getResp := getValue(t, client, baseURL, key)
	if getResp.Key != key || getResp.Value != value {
		t.Fatalf("unexpected GET response: %+v", getResp)
	}

	stateAfterWrite := getState(t, client, baseURL)
	if stateAfterWrite.Leader == "" {
		t.Fatal("gateway state lost leader after write")
	}
	if _, ok := knownNodes[stateAfterWrite.Leader]; !ok {
		t.Fatalf("state leader after write %q not in cluster %v", stateAfterWrite.Leader, cluster)
	}
	assertFollowers(t, stateAfterWrite, knownNodes, len(cluster)-1)

	logEvent(t, "PASS cluster-state", "%s", mustJSON(t, stateAfterWrite))
	logEvent(t, "PASS stored-data", "%s", mustJSON(t, getResp))
}

func runGatewayLeaderFailoverFlow(t *testing.T, peerCount int, key string, value string) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	defer logTestResult(t)

	repoRoot := repoRoot(t)
	binaries := buildBinaries(t, repoRoot)
	stateDir := filepath.Join(t.TempDir(), "raft-state")
	ports := reservePorts(t, peerCount+1)
	gatewayPort := ports[0]
	peerPorts := ports[1:]
	cluster := clusterAddrs(peerPorts)

	clusterRun := startCluster(t, repoRoot, binaries, gatewayPort, peerPorts, cluster, stateDir)
	defer stopCluster(t, clusterRun)

	client := &http.Client{Timeout: time.Second}
	baseURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	state := waitForLeader(t, client, baseURL, len(cluster)-1, 10*time.Second)
	knownNodes := make(map[string]struct{}, len(cluster))
	for _, addr := range cluster {
		knownNodes[addr] = struct{}{}
	}
	assertFollowers(t, state, knownNodes, len(cluster)-1)

	putResp := putValue(t, client, baseURL, key, value)
	if putResp.Key != key || putResp.Value != value {
		t.Fatalf("unexpected PUT response: %+v", putResp)
	}

	for _, follower := range state.Followers {
		waitForPeerValue(t, client, follower, key, value, 5*time.Second)
	}

	leaderProc := peerProcessForAddr(t, clusterRun.processes, cluster, state.Leader)
	stopProcess(leaderProc)

	remainingNodes := make(map[string]struct{}, len(cluster)-1)
	for _, addr := range cluster {
		if addr == state.Leader {
			continue
		}
		remainingNodes[addr] = struct{}{}
	}

	stateAfterFailover := waitForDifferentLeader(t, client, baseURL, state.Leader, len(remainingNodes)-1, 10*time.Second)
	if _, ok := remainingNodes[stateAfterFailover.Leader]; !ok {
		t.Fatalf("new leader %q not in surviving cluster", stateAfterFailover.Leader)
	}
	assertFollowers(t, stateAfterFailover, remainingNodes, len(remainingNodes)-1)

	getResp := getValue(t, client, baseURL, key)
	if getResp.Key != key || getResp.Value != value {
		t.Fatalf("unexpected GET response after failover: %+v", getResp)
	}

	logEvent(t, "PASS failover-state", "%s", mustJSON(t, stateAfterFailover))
	logEvent(t, "PASS failover-data", "%s", mustJSON(t, getResp))
}

func assertFollowers(t *testing.T, state stateResponse, knownNodes map[string]struct{}, expectedCount int) {
	t.Helper()

	if len(state.Followers) != expectedCount {
		t.Fatalf("expected %d followers in state response, got %d", expectedCount, len(state.Followers))
	}

	seen := make(map[string]struct{}, len(state.Followers))
	for _, follower := range state.Followers {
		if follower == state.Leader {
			t.Fatalf("leader %q was also reported as a follower", follower)
		}
		if _, ok := knownNodes[follower]; !ok {
			t.Fatalf("follower %q not in cluster", follower)
		}
		if _, ok := seen[follower]; ok {
			t.Fatalf("duplicate follower %q in state response", follower)
		}
		seen[follower] = struct{}{}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, filePath, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve test file path")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(filePath)))
}

func buildBinaries(t *testing.T, repoRoot string) map[string]string {
	t.Helper()

	distDir := filepath.Join(repoRoot, "dist")
	if err := os.MkdirAll(distDir, 0o755); err != nil {
		t.Fatalf("create dist dir: %v", err)
	}

	binaries := map[string]string{
		"peer":    filepath.Join(distDir, "peer"),
		"gateway": filepath.Join(distDir, "gateway"),
	}

	buildTarget(t, repoRoot, "./peer", binaries["peer"])
	buildTarget(t, repoRoot, "./gateway", binaries["gateway"])

	return binaries
}

func buildTarget(t *testing.T, repoRoot string, pkg string, output string) {
	t.Helper()

	cmd := exec.Command("go", "build", "-o", output, pkg)
	cmd.Dir = repoRoot
	outputText, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, outputText)
	}
}

func reservePorts(t *testing.T, count int) []int {
	t.Helper()

	ports := make([]int, 0, count)
	listeners := make([]net.Listener, 0, count)

	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		listeners = append(listeners, listener)
		addr := listener.Addr().(*net.TCPAddr)
		ports = append(ports, addr.Port)
	}

	for _, listener := range listeners {
		listener.Close()
	}

	return ports
}

func clusterAddrs(ports []int) []string {
	addrs := make([]string, 0, len(ports))
	for _, port := range ports {
		addrs = append(addrs, fmt.Sprintf("localhost:%d", port))
	}
	return addrs
}

func startCluster(t *testing.T, repoRoot string, binaries map[string]string, gatewayPort int, peerPorts []int, cluster []string, stateDir string) *clusterHandle {
	t.Helper()
	return startClusterWithOptions(t, repoRoot, binaries, gatewayPort, peerPorts, cluster, stateDir, clusterOptions{})
}

func startClusterWithOptions(t *testing.T, repoRoot string, binaries map[string]string, gatewayPort int, peerPorts []int, cluster []string, stateDir string, options clusterOptions) *clusterHandle {
	t.Helper()

	handle := &clusterHandle{
		processes:     make([]*clusterProcess, 0, len(peerPorts)+1),
		peerEnvByNode: make(map[string]map[string]string, len(peerPorts)),
	}
	t.Cleanup(func() {
		stopCluster(t, handle)
	})

	handle.postgres = startPostgresBackends(t, len(peerPorts))
	clusterFlag := strings.Join(cluster, ",")

	for idx, port := range peerPorts {
		nodeID := fmt.Sprintf("node%d", idx+1)
		nodeStateDir := filepath.Join(stateDir, fmt.Sprintf("node%d", idx+1))
		peerEnv := mergeEnv(options.peerEnv, map[string]string{
			"DATABASE_URL": databaseURLForPort(handle.postgres[idx].port),
		})
		handle.peerEnvByNode[nodeID] = peerEnv

		proc := startProcess(t, repoRoot, fmt.Sprintf("peer-%d", idx+1), binaries["peer"], peerEnv,
			"-port", strconv.Itoa(port),
			"-nodeId", nodeID,
			"-cluster", peerClusterFlag(cluster, idx),
			"-stateDir", nodeStateDir,
		)
		handle.processes = append(handle.processes, proc)
	}

	handle.processes = append(handle.processes, startProcess(t, repoRoot, "gateway", binaries["gateway"], nil,
		"-port", strconv.Itoa(gatewayPort),
		"-cluster", clusterFlag,
	))

	for _, port := range peerPorts {
		waitForEndpoint(t, fmt.Sprintf("http://localhost:%d/health", port), 5*time.Second)
	}
	waitForEndpoint(t, fmt.Sprintf("http://localhost:%d/raft/state", gatewayPort), 5*time.Second)

	return handle
}

func startPostgresBackends(t *testing.T, count int) []*postgresBackend {
	t.Helper()
	ensureDockerAvailable(t)

	ports := reservePorts(t, count)
	prefix := dockerNamePrefix(t)
	backends := make([]*postgresBackend, 0, count)

	for idx, port := range ports {
		backend := &postgresBackend{
			name: fmt.Sprintf("%s-pg-%d", prefix, idx+1),
			port: port,
		}

		cmd := exec.Command(
			"docker", "run", "-d",
			"--name", backend.name,
			"-p", fmt.Sprintf("%d:5432", backend.port),
			"--health-cmd=pg_isready -U raft -d raft",
			"--health-interval=2s",
			"--health-timeout=2s",
			"--health-retries=30",
			"-e", "POSTGRES_DB=raft",
			"-e", "POSTGRES_USER=raft",
			"-e", "POSTGRES_PASSWORD=raft",
			"postgres:16-alpine",
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("start postgres backend %s: %v\n%s", backend.name, err, output)
		}

		waitForPostgresHealthy(t, backend)
		backends = append(backends, backend)
	}

	return backends
}

func ensureDockerAvailable(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is required for e2e tests")
	}

	cmd := exec.Command("docker", "version", "--format", "{{.Server.Version}}")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("docker server is unavailable: %v\n%s", err, output)
	}
}

func waitForPostgresHealthy(t *testing.T, backend *postgresBackend) {
	t.Helper()

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("docker", "inspect", "-f", "{{.State.Health.Status}}", backend.name)
		output, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(output)) == "healthy" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	logsCmd := exec.Command("docker", "logs", backend.name)
	logs, _ := logsCmd.CombinedOutput()
	t.Fatalf("postgres backend %s did not become healthy\n%s", backend.name, logs)
}

func databaseURLForPort(port int) string {
	return fmt.Sprintf("postgres://raft:raft@127.0.0.1:%d/raft?sslmode=disable", port)
}

func peerClusterFlag(cluster []string, selfIndex int) string {
	entries := make([]string, 0, len(cluster))
	for idx, addr := range cluster {
		if idx == selfIndex {
			entries = append(entries, fmt.Sprintf("node%d.raft-peer:%d", idx+1, portForAddrText(addr)))
			continue
		}
		entries = append(entries, addr)
	}
	return strings.Join(entries, ",")
}

func portForAddrText(addr string) int {
	_, portText, ok := strings.Cut(addr, ":")
	if !ok {
		return 0
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return 0
	}
	return port
}

func dockerNamePrefix(t *testing.T) string {
	t.Helper()

	name := strings.ToLower(t.Name())
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '-'
	}, name)

	return fmt.Sprintf("raft-e2e-%s-%d", name, time.Now().UnixNano())
}

func mergeEnv(base map[string]string, extra map[string]string) map[string]string {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}

	merged := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}

	return merged
}

func startProcess(t *testing.T, repoRoot string, name string, binary string, env map[string]string, args ...string) *clusterProcess {
	t.Helper()

	cmd := exec.Command(binary, args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), formatEnv(env)...)

	proc := &clusterProcess{name: name, cmd: cmd}
	cmd.Stdout = &proc.stdout
	cmd.Stderr = &proc.stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}

	return proc
}

func formatEnv(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	formatted := make([]string, 0, len(env))
	for _, key := range keys {
		formatted = append(formatted, key+"="+env[key])
	}

	return formatted
}

func stopProcess(proc *clusterProcess) {
	if proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
		return
	}

	_ = proc.cmd.Process.Kill()
	_ = proc.cmd.Wait()
	proc.cmd = nil
}

func peerProcessForAddr(t *testing.T, processes []*clusterProcess, cluster []string, addr string) *clusterProcess {
	t.Helper()

	for idx, peerAddr := range cluster {
		if peerAddr == addr {
			return processes[idx]
		}
	}

	t.Fatalf("no peer process found for addr %q", addr)
	return nil
}

func replaceProcessForAddr(t *testing.T, processes []*clusterProcess, cluster []string, addr string, proc *clusterProcess) {
	t.Helper()

	for idx, peerAddr := range cluster {
		if peerAddr == addr {
			processes[idx] = proc
			return
		}
	}

	t.Fatalf("no peer process slot found for addr %q", addr)
}

func nodeIDForAddr(t *testing.T, cluster []string, addr string) string {
	t.Helper()

	for idx, peerAddr := range cluster {
		if peerAddr == addr {
			return fmt.Sprintf("node%d", idx+1)
		}
	}

	t.Fatalf("no node id found for addr %q", addr)
	return ""
}

func nodeIndexForID(t *testing.T, nodeID string) int {
	t.Helper()

	if !strings.HasPrefix(nodeID, "node") {
		t.Fatalf("unexpected node id %q", nodeID)
	}

	index, err := strconv.Atoi(strings.TrimPrefix(nodeID, "node"))
	if err != nil || index <= 0 {
		t.Fatalf("parse node index from %q: %v", nodeID, err)
	}

	return index - 1
}

func portForAddr(t *testing.T, addr string) int {
	t.Helper()

	_, portText, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("addr %q missing port", addr)
	}

	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse port for %q: %v", addr, err)
	}

	return port
}

func stopCluster(t *testing.T, handle *clusterHandle) {
	t.Helper()
	if handle == nil {
		return
	}

	for _, proc := range handle.processes {
		if proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
			continue
		}
		_ = proc.cmd.Process.Kill()
		_ = proc.cmd.Wait()
		if t.Failed() {
			t.Logf("%s stdout:\n%s", proc.name, proc.stdout.String())
			t.Logf("%s stderr:\n%s", proc.name, proc.stderr.String())
		}
	}

	for _, backend := range handle.postgres {
		if backend == nil || backend.name == "" {
			continue
		}
		if t.Failed() {
			logsCmd := exec.Command("docker", "logs", backend.name)
			if logs, err := logsCmd.CombinedOutput(); err == nil && len(logs) > 0 {
				t.Logf("%s logs:\n%s", backend.name, logs)
			}
		}
		_ = exec.Command("docker", "rm", "-f", backend.name).Run()
	}
}

func logTestResult(t *testing.T) {
	t.Helper()
	if t.Failed() {
		logEvent(t, "FAIL test-result", "gateway integration test failed")
		return
	}
	logEvent(t, "PASS test-result", "gateway integration test passed")
}

func waitForEndpoint(t *testing.T, url string, timeout time.Duration) {
	t.Helper()

	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("endpoint did not come up: %s", url)
}

func waitForLeader(t *testing.T, client *http.Client, baseURL string, expectedFollowers int, timeout time.Duration) stateResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		status, body, err := rawRequest(client, http.MethodGet, baseURL+"/raft/state", nil)
		if err == nil && status == http.StatusOK {
			var state stateResponse
			if err := json.Unmarshal(body, &state); err == nil && state.Leader != "" && len(state.Followers) == expectedFollowers {
				logHTTPResponse(t, "PASS GET /raft/state", status, body)
				return state
			}
			lastErr = fmt.Errorf("gateway state not stable yet: %s", formatBody(body))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err == nil {
			lastErr = fmt.Errorf("unexpected state status %d: %s", status, string(body))
		} else {
			lastErr = err
		}
		if err == nil {
			logHTTPResponse(t, "FAIL GET /raft/state", status, body)
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("leader election did not stabilize within %s: %v", timeout, lastErr)
	return stateResponse{}
}

func waitForDifferentLeader(t *testing.T, client *http.Client, baseURL string, previousLeader string, expectedFollowers int, timeout time.Duration) stateResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		status, body, err := rawRequest(client, http.MethodGet, baseURL+"/raft/state", nil)
		if err == nil && status == http.StatusOK {
			var state stateResponse
			if err := json.Unmarshal(body, &state); err == nil && state.Leader != "" && state.Leader != previousLeader && len(state.Followers) == expectedFollowers {
				logHTTPResponse(t, "PASS GET /raft/state", status, body)
				return state
			}
			lastErr = fmt.Errorf("gateway failover state not stable yet: %s", formatBody(body))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err == nil {
			lastErr = fmt.Errorf("unexpected state status %d: %s", status, string(body))
		} else {
			lastErr = err
		}
		if err == nil {
			logHTTPResponse(t, "FAIL GET /raft/state", status, body)
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("leader failover did not stabilize within %s: %v", timeout, lastErr)
	return stateResponse{}
}

func getState(t *testing.T, client *http.Client, baseURL string) stateResponse {
	t.Helper()

	status, body, err := rawRequest(client, http.MethodGet, baseURL+"/raft/state", nil)
	if err != nil {
		t.Fatalf("request state: %v", err)
	}
	if status != http.StatusOK {
		logHTTPResponse(t, "FAIL GET /raft/state", status, body)
		t.Fatalf("unexpected state status %d: %s", status, string(body))
	}
	logHTTPResponse(t, "PASS GET /raft/state", status, body)

	var state stateResponse
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("decode state response: %v", err)
	}

	return state
}

func waitForPeerValue(t *testing.T, client *http.Client, addr string, key string, value string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	peerURL := fmt.Sprintf("http://%s/kv/%s", addr, key)

	for time.Now().Before(deadline) {
		status, body, err := rawRequest(client, http.MethodGet, peerURL, nil)
		if err == nil && status == http.StatusOK {
			var response kvResponse
			if err := json.Unmarshal(body, &response); err == nil && response.Key == key && response.Value == value {
				logHTTPResponse(t, "PASS GET /kv/"+key, status, body)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("peer %q did not apply key %q within %s", addr, key, timeout)
}

func putValue(t *testing.T, client *http.Client, baseURL string, key string, value string) kvResponse {
	t.Helper()

	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		t.Fatalf("marshal put body: %v", err)
	}

	status, respBody, err := rawRequest(client, http.MethodPut, baseURL+"/raft/kv/"+key, body)
	if err != nil {
		t.Fatalf("put value: %v", err)
	}
	if status != http.StatusOK {
		logHTTPResponse(t, "FAIL PUT /raft/kv/"+key, status, respBody)
		t.Fatalf("unexpected PUT status %d: %s", status, string(respBody))
	}
	logHTTPResponse(t, "PASS PUT /raft/kv/"+key, status, respBody)

	var response kvResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		t.Fatalf("decode PUT response: %v", err)
	}

	return response
}

func getValue(t *testing.T, client *http.Client, baseURL string, key string) kvResponse {
	t.Helper()

	status, respBody, err := rawRequest(client, http.MethodGet, baseURL+"/raft/kv/"+key, nil)
	if err != nil {
		t.Fatalf("get value: %v", err)
	}
	if status != http.StatusOK {
		logHTTPResponse(t, "FAIL GET /raft/kv/"+key, status, respBody)
		t.Fatalf("unexpected GET status %d: %s", status, string(respBody))
	}
	logHTTPResponse(t, "PASS GET /raft/kv/"+key, status, respBody)

	var response kvResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}

	return response
}

func readPersistentStateFile(t *testing.T, stateDir string, nodeID string) persistedState {
	t.Helper()

	path := filepath.Join(stateDir, nodeID+".json")
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persistent state %s: %v", path, err)
	}

	var state persistedState
	if err := json.Unmarshal(bytes, &state); err != nil {
		t.Fatalf("decode persistent state %s: %v", path, err)
	}

	return state
}

func readSnapshotFile(t *testing.T, stateDir string, nodeID string) persistedSnapshot {
	t.Helper()

	path := filepath.Join(stateDir, nodeID+".snapshot.json")
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot file %s: %v", path, err)
	}

	var snapshot persistedSnapshot
	if err := json.Unmarshal(bytes, &snapshot); err != nil {
		t.Fatalf("decode snapshot file %s: %v", path, err)
	}

	if snapshot.Data == nil {
		snapshot.Data = make(map[string]string)
	}

	return snapshot
}

func waitForSnapshotState(t *testing.T, stateDir string, nodeID string, ready func(persistedSnapshot, persistedState) bool, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastSnapshot persistedSnapshot
	var lastState persistedState

	for time.Now().Before(deadline) {
		snapshotPath := filepath.Join(stateDir, nodeID+".snapshot.json")
		if _, err := os.Stat(snapshotPath); err == nil {
			lastSnapshot = readSnapshotFile(t, stateDir, nodeID)
			lastState = readPersistentStateFile(t, stateDir, nodeID)
			if ready(lastSnapshot, lastState) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("snapshot state for %s did not satisfy condition within %s: snapshot=%+v remainingLog=%d", nodeID, timeout, lastSnapshot, len(lastState.Log))
}

func rawRequest(client *http.Client, method string, url string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}

	return resp.StatusCode, respBody, nil
}

func logHTTPResponse(t *testing.T, label string, status int, body []byte) {
	t.Helper()
	logEvent(t, label, "status=%d body=%s", status, formatBody(body))
}

func logEvent(t *testing.T, label string, format string, args ...any) {
	t.Helper()
	message := fmt.Sprintf("[%s] %s", label, fmt.Sprintf(format, args...))
	fmt.Println(message)
}

func formatBody(body []byte) string {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return "<empty>"
	}

	var compact bytes.Buffer
	if json.Compact(&compact, trimmed) == nil {
		return compact.String()
	}

	return string(trimmed)
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal log payload: %v", err)
	}
	return string(encoded)
}
