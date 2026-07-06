package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
var scenarios []Func

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

	scenarios = []Func{
		scenarioOne,
		scenarioTwo,
		scenarioThree,
		scenarioFour,
		scenarioFive,
		scenarioSix,
		scenarioSeven,
		scenarioEight,
	}

	err := portForward()

	if err != nil {
		fail(err)
	}
	defer cleanupPortForwards() //cleanup after shutdown

	//wait for all port-forwardings
	time.Sleep(10 * time.Second)

	r = initRunner(cfg)

	if err := waitForLeader(r.cfg.StartupTimeout); err != nil {
		fail(err)
	}

	runScenarios()
}

func runScenarios() {
	for _, fun := range scenarios {
		if err := fun(); err != nil {
			fail(err)
		}
	}
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
		res, err := r.getFromPeer(ctx, addr, key, reqId)
		cancel()

		var httpError *HTTPError

		if errors.As(err, &httpError) && httpError.StatusCode == http.StatusNotFound {
			return nil
		}

		if res.Key != key {
			return fmt.Errorf("Key isn't deleted from the peer: %s", addr)
		}

		time.Sleep(r.cfg.PollingInterval)
	}
	return fmt.Errorf("[delete] peer %s still has the deleted key %s", addr, key)
}

func scenarioOne() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario One==")

	key := "1"
	value := "hello"
	reqId := getReqId()

	res, err := putContext(key, value, getReqId())

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

	fmt.Printf("response from get: %s:%s", res.Key, res.Value)

	return nil
}

func scenarioTwo() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}
	fmt.Println("\n\n==Scenario Two==")

	key := "1"
	value := "hello"

	res, err := putContext(key, value, getReqId())

	if err != nil {
		return err
	}

	if res.Key != key || res.Value != value {
		return fmt.Errorf("[put] Request attr didn't match the response attr: %s:%s :: %s:%s", key, value, res.Key, res.Value)
	}

	fmt.Printf("response from put: %s:%s\n", res.Key, res.Value)

	res, err = deleteContext(key, getReqId())

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

	fmt.Printf("Deleted entry: %s:%s doesn't exist.\nError: %s", key, value, err.Error())
	return nil
}

func scenarioThree() error {
	if err := waitForLeader(5 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario Three==")

	key := "1"
	value := "hello"

	res, err := putContext(key, value, getReqId())

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

	fmt.Printf("All followers deleted the entry %s:%s", key, value)

	return nil
}

func scenarioFour() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario Four===")

	addr := r.cfg.Leader
	podName, _, _ := strings.Cut(addr, ".")

	//free the port
	if err := stopPortForward(addr); err != nil {
		return err
	}

	//then kill the pod
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

	res, err := putContext(key, value, getReqId())

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

	fmt.Printf("old leader replicated entry from new leader:%s", addr)

	return nil
}

func scenarioFive() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario Five===")

	set := map[string]string{
		"1": "value1",
		"2": "value2",
	}

	addr := r.cfg.Followers[0]
	podName, _, _ := strings.Cut(addr, ".")

	//free the port first
	if err := stopPortForward(addr); err != nil {
		return err
	}

	//then kill a follower pod
	if err := killPod(podName); err != nil {
		return err
	}

	for key, value := range set {
		res, err := putContext(key, value, getReqId())

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

	//restart port-forwarding for the killed follower
	if err := execBgCommand("kubectl", append([]string{}, "port-forward", "pod/"+podName, getPortPairs(addr))); err != nil {
		return err
	}

	for key, value := range set {
		if err := pollGetFromPeer(addr, key, value, getReqId()); err != nil {
			return err
		}
	}

	fmt.Printf("Follower caught up with leader after restarting:%s\n%v", podName, set)

	return nil
}

func scenarioSix() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario Six===")

	//multiple req with same request-id but diff values, should only keep the 1st one
	key := "key1"
	reqId := getReqId()

	for i := range 3 {
		value := "v" + strconv.Itoa(i)

		res, err := putContext(key, value, reqId)

		if err != nil {
			return err
		}

		if res.Key != key || res.Value != value {
			return fmt.Errorf("[put-idempotency] put failed: Expected: %s:%s :: Received: %s:%s\n", key, value, res.Key, res.Value)
		}
	}

	getCtx, getCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer getCancel()

	res, err := r.get(getCtx, key, getReqId())

	if err != nil {
		return err
	}

	if res.Value != "v0" || res.Key != key {
		fail(fmt.Errorf("[put-idempotency] idempontency isn't working in raft. Expected: %s:%s :: Received: %s:%s\n", key, "v0", res.Key, res.Value))
	}

	fmt.Printf("[put-idempotency] entry matches first put request: %s:%s\n", res.Key, res.Value)

	//now we do it for delete as well

	reqId = getReqId() //new ID for delete

	for range 3 {
		res, err := deleteContext(key, reqId)

		if err != nil {
			return err
		}

		if !res.Deleted {
			return fmt.Errorf("[delete-idempotency] delete failed: %s:%v\n", res.Key, res.Deleted)
		}
	}

	getCtx, getCancel = context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer getCancel()

	res, err = r.get(getCtx, key, getReqId())

	var httpError *HTTPError

	if err != nil && (!errors.As(err, &httpError) || httpError.StatusCode != http.StatusNotFound) {
		return err
	}

	if res.Key != "" {
		fail(fmt.Errorf("[delete-idempotency] idempontency isn't working in raft. expected key:%s :: Received key:%s\n", key, res.Key))
	}

	return nil
}

func scenarioSeven() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario Seven===")

	var wg sync.WaitGroup

	n := 20

	errs := make(chan error, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("conkey-%d", i)
			value := fmt.Sprintf("convalue-%d", i)

			res, err := putContext(key, value, getReqId())

			if err != nil {
				errs <- err
				return
			}

			if res.Key != key || res.Value != value {
				errs <- fmt.Errorf("[put-concurrent] Request attr didn't match the response attr: %s:%s :: %s:%s\n", key, value, res.Key, res.Value)
				return
			}

			fmt.Printf("[put-concurrent] Write for %s:%s for %d iteration runs fine\n", key, value, i)
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	fmt.Println()

	//now we check each value is in the store or not
	errs = make(chan error, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("conkey-%d", i)
			value := fmt.Sprintf("convalue-%d", i)

			res, err := getContext(key, getReqId())

			if err != nil {
				errs <- err
				return
			}

			if res.Key != key || res.Value != value {
				errs <- fmt.Errorf("[get-concurrent] Request attr didn't match the response attr: %s:%s :: %s:%s\n", key, value, res.Key, res.Value)
				return
			}

			fmt.Printf("[get-concurrent] Read for %s:%s for %d iteration runs fine\n", key, value, i)
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	fmt.Printf("Concurrent writes and then reads work fine for %d runs\n", n)

	return nil
}

func scenarioEight() error {
	if err := waitForLeader(10 * time.Second); err != nil {
		return err
	}

	fmt.Println("\n\n==Scenario Eight===")

	var wg sync.WaitGroup

	n := 20
	key := "conkey"

	errs := make(chan error, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			value := fmt.Sprintf("%d", i)

			res, err := putContext(key, value, getReqId())

			if err != nil {
				errs <- err
				return
			}

			if res.Key != key || res.Value != value {
				errs <- fmt.Errorf("[put-concurrent] Request attr didn't match the response attr: %s:%s :: %s:%s\n", key, value, res.Key, res.Value)
				return
			}
			fmt.Printf("[put-concurrent] Read for %s:%s for %d iteration runs fine\n", key, value, i)

		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	res, err := getContext(key, getReqId())

	if err != nil {
		return err
	}

	val, err := strconv.Atoi(res.Value)

	if err != nil {
		return err
	}

	if res.Key != key || val > n || val < 0 {
		return fmt.Errorf("[get-value-concurrent] Response isn't matching expected entry: %s:%s\n", res.Key, res.Value)
	}

	//now we check if followers have replicated the same values
	for _, follower := range r.cfg.Followers {
		if err := pollGetFromPeer(follower, key, res.Value, getReqId()); err != nil {
			return err
		}
	}
	fmt.Printf("All followers replicated the entry %s:%s\n\n", key, res.Value)

	return nil
}
