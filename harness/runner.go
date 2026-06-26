package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	RequestTimeout      time.Duration
	StartupTimeout      time.Duration
	PollingInterval     time.Duration
	Gateway             string
	Followers           []string
	Leader              string
	PeerPublicAddresses map[string]string
}

type Runner struct {
	cfg    Config
	client *http.Client
}

type StateResponse struct {
	Leader    string   `json:"leader"`
	Followers []string `json:"followers"`
}

type KVResponse struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

type Func func() error

func initRunner(cfg Config) *Runner {
	return &Runner{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
	}
}


var r *Runner
var scenarios map[string]Func

func main() {
	cfg := Config{
		RequestTimeout:  2 * time.Second,
		StartupTimeout:  20 * time.Second,
		PollingInterval: 1 * time.Second,
		Gateway:         "http://localhost:7000",
		PeerPublicAddresses: map[string]string{
			"raft-peer-0.raft-peer:8080": "http://localhost:18080",
			"raft-peer-1.raft-peer:8080": "http://localhost:18081",
			"raft-peer-2.raft-peer:8080": "http://localhost:18082",
		},
	}

	scenarios = map[string]Func{
		"scenarioOne":   scenarioOne,
		"scenarioTwo":   scenarioTwo,
		"scenarioThree": scenarioThree,
		"scenarioFour":  scenarioFour,
		"scenarioFive":  scenarioFive,
		"scenarioSix":   scenarioSix,
	}

	err := portForward()

	if err != nil {
		fail("portForward", err)
	}

	//wait for all port-forwardings
	time.Sleep(10 * time.Second)

	r = initRunner(cfg)

	if err := waitForLeader(r.cfg.StartupTimeout); err != nil {
		fail("waitForLeader", err)
	}

	runScenarios()
	cleanupPortForwards()
}

func runScenarios() {
	for key, fun := range scenarios {
		if err := fun(); err != nil {
			fail(key, err)
		}
	}
}

// gateway request handlers
func (r *Runner) state(ctx context.Context, reqId string) (StateResponse, error) {
	var out StateResponse

	err := r.request(ctx, nil, http.MethodGet, r.cfg.Gateway+"/raft/state", &out, reqId)

	return out, err
}

func (r *Runner) get(ctx context.Context, key string, reqId string) (KVResponse, error) {
	var out KVResponse
	err := r.request(ctx, nil, http.MethodGet, r.cfg.Gateway+"/raft/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) put(ctx context.Context, key, value string, reqId string) (KVResponse, error) {
	var out KVResponse
	body, err := json.Marshal(map[string]string{"value": value})

	if err != nil {
		return out, err
	}

	err = r.request(ctx, body, http.MethodPut, r.cfg.Gateway+"/raft/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) delete(ctx context.Context, key, reqId string) (KVResponse, error) {
	var out KVResponse
	err := r.request(ctx, nil, http.MethodDelete, r.cfg.Gateway+"/raft/kv/"+key, &out, reqId)
	return out, err
}

// peer request handlers
func (r *Runner) getFromPeer(ctx context.Context, addr, key, reqId string) (KVResponse, error) {
	var out KVResponse
	publicAddr, ok := r.cfg.PeerPublicAddresses[addr]
	if !ok {
		return out, fmt.Errorf("no public address configured for peer %s", addr)
	}
	err := r.request(ctx, nil, http.MethodGet, publicAddr+"/kv/"+key, &out, reqId)
	return out, err
}

func (r *Runner) deleteFromPeer(ctx context.Context, addr, key, reqId string) (KVResponse, error) {
	var out KVResponse
	publicAddr, ok := r.cfg.PeerPublicAddresses[addr]
	if !ok {
		return out, fmt.Errorf("no public address configured for peer %s", addr)
	}
	err := r.request(ctx, nil, http.MethodDelete, publicAddr+"/kv/"+key, &out, reqId)
	return out, err
}

// poll peer for requests
func pollGetFromPeer(addr, key, value, reqId string) error {
	timeout := time.Now().Add(10 * time.Second)

	for time.Now().Before(timeout) {
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
		res, err := r.getFromPeer(ctx, addr, key, reqId)
		cancel()

		if err == nil && res.Key == key && res.Value == value {
			fmt.Printf("[get-peer] entry: %s:%s\n", res.Key, res.Value)
			return nil
		}

		time.Sleep(r.cfg.PollingInterval)
	}
	return fmt.Errorf("[get] peer %s doesn't have the replicated entry %s:%s", addr, key, value)
}

func pollIsDeletedFromPeer(addr, key, reqId string) error {
	timeout := time.Now().Add(10 * time.Second)

	for time.Now().Before(timeout) {
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
		_, err := r.deleteFromPeer(ctx, addr, key, reqId)
		cancel()

		if err != nil {
			return nil
		}

		time.Sleep(r.cfg.PollingInterval)
	}
	return fmt.Errorf("[delete] peer %s still has the deleted key %s", addr, key)
}

func scenarioOne() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("==Scenario One==")

	key := "1"
	value := "hello"
	reqId := getReqId()

	putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer putCancel()

	res, err := r.put(putCtx, key, value, reqId)

	if err != nil {
		return err
	}

	if res.Key != key || res.Value != value {
		return fmt.Errorf("[put] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
	}

	fmt.Printf("response from put: %s:%s\n", res.Key, res.Value)

	getCtx, getCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer getCancel()

	res, err = r.get(getCtx, key, reqId)

	if err != nil {
		return err
	}

	if res.Key != key || res.Value != value {
		return fmt.Errorf("[get] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
	}

	fmt.Printf("response from get: %s:%s\n\n", res.Key, res.Value)

	return nil
}

// put and delete kv, then try to get it
func scenarioTwo() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}
	fmt.Println("==Scenario Two==")

	key := "1"
	value := "hello"

	putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer putCancel()

	res, err := r.put(putCtx, key, value, getReqId())

	if err != nil {
		return err
	}

	if res.Key != key || res.Value != value {
		return fmt.Errorf("[put] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
	}

	fmt.Printf("response from put: %s:%s\n", res.Key, res.Value)

	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer deleteCancel()

	res, err = r.delete(deleteCtx, key, getReqId())

	if err != nil {
		return err
	}

	if res.Key != key || !res.Deleted {
		return fmt.Errorf("[delete] requested entry didn't delete: %s, %t", res.Key, res.Deleted)
	}

	fmt.Printf("response from delete: %s:%t\n", res.Key, res.Deleted)

	getCtx, getCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer getCancel()

	res, err = r.get(getCtx, key, getReqId())

	if err == nil {
		return fmt.Errorf("Deleted key shouldn't be present")
	}

	fmt.Printf("Deleted entry: %s:%s doesn't exist.\nError: %s\n\n", key, value, err.Error())
	return nil
}

func scenarioThree() error {
	if err := waitForLeader(5 * time.Second); err != nil {
		return err
	}

	fmt.Println("==Scenario Three==")

	key := "1"
	value := "hello"

	putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer putCancel()

	res, err := r.put(putCtx, key, value, getReqId())

	if err != nil {
		return err
	}

	if res.Key != key || res.Value != value {
		return fmt.Errorf("[put] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
	}

	fmt.Printf("response from put: %s:%s\n", res.Key, res.Value)

	//now check if all followers replicate the value or not
	for _, follower := range r.cfg.Followers {
		if err := pollGetFromPeer(follower, key, value, getReqId()); err != nil {
			return err
		}
	}
	fmt.Printf("All followers replicated the entry %s:%s\n\n", key, value)

	//now delete and check if every follower replicates the delete
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer deleteCancel()

	res, err = r.delete(deleteCtx, key, getReqId())

	if err != nil {
		return err
	}

	if res.Key != key || !res.Deleted {
		return fmt.Errorf("[delete] requested entry didn't delete: %s, %t", res.Key, res.Deleted)
	}

	fmt.Printf("response from delete: %s:%t\n", res.Key, res.Deleted)

	//now check the followers
	for _, follower := range r.cfg.Followers {
		if err := pollIsDeletedFromPeer(follower, key, getReqId()); err != nil {
			return err
		}
	}

	fmt.Printf("All followers deleted the entry %s:%s\n\n", key, value)

	return nil
}

func scenarioFour() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("===Scenario Four===")

	addr := r.cfg.Leader
	podName, _, _ := strings.Cut(addr, ".")

	if err := killPod(podName); err != nil {
		return err
	}

	if err := execCommand("kubectl", append([]string{}, "wait", "--for=condition=Ready", "pod/"+podName, "--timeout="+(30*time.Second).String())); err != nil {
		return err
	}

	//a potential issue with port-forwarding here

	//restart port-forwarding for the killed leader
	if err := execBgCommand("kubectl", append([]string{}, "port-forward", "pod/"+podName, getPortPairs(addr))); err != nil {
		return err
	}

	//put new entry
	key := "2"
	value := "failedYetAdded"

	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer putCancel()

	res, err := r.put(putCtx, key, value, getReqId())

	if err != nil {
		return err
	}

	if res.Key != key || res.Value != value {
		return fmt.Errorf("[put] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
	}

	fmt.Printf("response from put: %s:%s\n", res.Key, res.Value)

	if err := pollGetFromPeer(addr, key, value, getReqId()); err != nil {
		return err
	}

	fmt.Printf("old leader replicated entry from new leader:%s\n\n", addr)

	return nil
}

func scenarioFive() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("===Scenario Five===")

	set := map[string]string{
		"1": "value1",
		"2": "value2",
	}

	addr := r.cfg.Followers[0]
	podName, _, _ := strings.Cut(addr, ".")

	//kill a follower pod
	if err := killPod(podName); err != nil {
		return err
	}

	for key, value := range set {
		putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
		res, err := r.put(putCtx, key, value, getReqId())
		putCancel()

		if err != nil {
			return err
		}

		if res.Key != key || res.Value != value {
			return fmt.Errorf("[put] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
		}
	}

	if err := execCommand("kubectl", append([]string{}, "wait", "--for=condition=Ready", "pod/"+podName, "--timeout="+(30*time.Second).String())); err != nil {
		return err
	}

	//restart port-forwarding for the killed leader
	if err := execBgCommand("kubectl", append([]string{}, "port-forward", "pod/"+podName, getPortPairs(addr))); err != nil {
		return err
	}

	for key, value := range set {
		if err := pollGetFromPeer(addr, key, value, getReqId()); err != nil {
			return err
		}
	}

	fmt.Printf("Follower caught up with leader after restarting:%s\n%v\n\n", podName, set)

	return nil
}

func scenarioSix() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("===Scenario Six===")

	//multiple req with same request-id but diff values, should only keep the 1st one
	key := "key1"
	reqId := getReqId()

	for i := range 3 {
		value := "v" + strconv.Itoa(i)

		putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
		_, err := r.put(putCtx, key, value, reqId)
		putCancel()

		if err != nil {
			return err
		}
	}

	getCtx, getCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer getCancel()

	res, err := r.get(getCtx, key, getReqId())

	if err != nil {
		return err
	}

	fmt.Printf("[get-idempotency] entry matches first put request: %s:%s", res.Key, res.Value)

	return nil
}
