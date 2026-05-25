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

type clusterProcess struct {
	name   string
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func TestGatewayClientFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	defer logTestResult(t)

	repoRoot := repoRoot(t)
	binaries := buildBinaries(t, repoRoot)
	ports := reservePorts(t, 4)
	gatewayPort := ports[0]
	peerPorts := ports[1:]
	cluster := clusterAddrs(peerPorts)

	processes := startCluster(t, repoRoot, binaries, gatewayPort, peerPorts, cluster)
	defer stopCluster(t, processes)

	client := &http.Client{Timeout: time.Second}
	baseURL := fmt.Sprintf("http://localhost:%d", gatewayPort)

	state := waitForLeader(t, client, baseURL, 10*time.Second)
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

	key := "test-key"
	value := "test-value"

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
	return filepath.Dir(filepath.Dir(filePath))
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

func startCluster(t *testing.T, repoRoot string, binaries map[string]string, gatewayPort int, peerPorts []int, cluster []string) []*clusterProcess {
	t.Helper()

	processes := make([]*clusterProcess, 0, len(peerPorts)+1)
	clusterFlag := strings.Join(cluster, ",")

	for idx, port := range peerPorts {
		proc := startProcess(t, repoRoot, fmt.Sprintf("peer-%d", idx+1), binaries["peer"],
			"-port", strconv.Itoa(port),
			"-nodeId", fmt.Sprintf("node%d", idx+1),
			"-cluster", clusterFlag,
		)
		processes = append(processes, proc)
	}

	processes = append(processes, startProcess(t, repoRoot, "gateway", binaries["gateway"],
		"-port", strconv.Itoa(gatewayPort),
		"-cluster", clusterFlag,
	))

	for _, port := range peerPorts {
		waitForEndpoint(t, fmt.Sprintf("http://localhost:%d/health", port), 5*time.Second)
	}
	waitForEndpoint(t, fmt.Sprintf("http://localhost:%d/raft/state", gatewayPort), 5*time.Second)

	return processes
}

func startProcess(t *testing.T, repoRoot string, name string, binary string, args ...string) *clusterProcess {
	t.Helper()

	cmd := exec.Command(binary, args...)
	cmd.Dir = repoRoot

	proc := &clusterProcess{name: name, cmd: cmd}
	cmd.Stdout = &proc.stdout
	cmd.Stderr = &proc.stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}

	return proc
}

func stopCluster(t *testing.T, processes []*clusterProcess) {
	t.Helper()

	for _, proc := range processes {
		if proc.cmd.Process == nil {
			continue
		}
		_ = proc.cmd.Process.Kill()
		_ = proc.cmd.Wait()
		if t.Failed() {
			t.Logf("%s stdout:\n%s", proc.name, proc.stdout.String())
			t.Logf("%s stderr:\n%s", proc.name, proc.stderr.String())
		}
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

func waitForLeader(t *testing.T, client *http.Client, baseURL string, timeout time.Duration) stateResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		status, body, err := rawRequest(client, http.MethodGet, baseURL+"/raft/state", nil)
		if err == nil && status == http.StatusOK {
			var state stateResponse
			if err := json.Unmarshal(body, &state); err == nil && state.Leader != "" {
				logHTTPResponse(t, "PASS GET /raft/state", status, body)
				return state
			}
			lastErr = fmt.Errorf("gateway state did not include a leader yet")
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
func putValue(t *testing.T, client *http.Client, baseURL string, key string, value string) kvResponse {
	t.Helper()

	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		t.Fatalf("marshal put body: %v", err)
	}

	status, respBody, err := rawRequest(client, http.MethodPut, baseURL+"/raft/update/"+key, body)
	if err != nil {
		t.Fatalf("put value: %v", err)
	}
	if status != http.StatusOK {
		logHTTPResponse(t, "FAIL PUT /raft/update/"+key, status, respBody)
		t.Fatalf("unexpected PUT status %d: %s", status, string(respBody))
	}
	logHTTPResponse(t, "PASS PUT /raft/update/"+key, status, respBody)

	var response kvResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		t.Fatalf("decode PUT response: %v", err)
	}

	return response
}

func getValue(t *testing.T, client *http.Client, baseURL string, key string) kvResponse {
	t.Helper()

	status, respBody, err := rawRequest(client, http.MethodGet, baseURL+"/raft/get/"+key, nil)
	if err != nil {
		t.Fatalf("get value: %v", err)
	}
	if status != http.StatusOK {
		logHTTPResponse(t, "FAIL GET /raft/get/"+key, status, respBody)
		t.Fatalf("unexpected GET status %d: %s", status, string(respBody))
	}
	logHTTPResponse(t, "PASS GET /raft/get/"+key, status, respBody)

	var response kvResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}

	return response
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
