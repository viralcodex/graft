package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

var portForwards []*exec.Cmd

func getReqId() string {
	return ulid.Make().String()
}

func getPortPairs(addr string) string {
	publicAddr := strings.Split(r.cfg.PeerPublicAddresses[addr], ":")
	publicPort := publicAddr[len(publicAddr)-1]

	address := strings.Split(addr, ":")
	port := address[len(address)-1]

	return publicPort + ":" + port
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
}

func execCommand(command string, args []string) error {
	cmd := exec.Command(command, args...)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func execBgCommand(command string, args []string) error {
	cmd := exec.Command(command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return err
	}

	portForwards = append(portForwards, cmd)

	//release process in new goroutine
	go func() {
		if err := cmd.Wait(); err != nil {
			fmt.Fprintf(os.Stderr, "%s %v exited with error: %v\n", command, args, err)
		}
	}()

	return nil
}

func portForward() error {
	k8s := "kubectl"

	//gateway
	if err := execBgCommand(k8s, append([]string{}, "port-forward", "svc/gateway", "7000:7000")); err != nil {
		return err
	}

	//peer1
	if err := execBgCommand(k8s, append([]string{}, "port-forward", "pod/raft-peer-0", "18080:8080")); err != nil {
		return err
	}

	// peer2
	if err := execBgCommand(k8s, append([]string{}, "port-forward", "pod/raft-peer-1", "18081:8080")); err != nil {
		return err
	}

	// peer3
	if err := execBgCommand(k8s, append([]string{}, "port-forward", "pod/raft-peer-2", "18082:8080")); err != nil {
		return err
	}

	return nil
}

func waitForLeader(timeout time.Duration) error {
	startupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		//if startup ctx timed out before cluster stabilises
		select {
		case <-startupCtx.Done():
			fmt.Println("cluster did not become ready before startup timeout")
			return startupCtx.Err()
		default:
		}

		reqCtx, reqCancel := context.WithTimeout(startupCtx, r.cfg.RequestTimeout)
		res, err := r.state(reqCtx, getReqId())
		reqCancel()

		if err == nil && res.Leader != "" && len(res.Followers) > 0 {
			r.cfg.Followers = append([]string{}, res.Followers...)
			r.cfg.Leader = res.Leader
			break
		}

		fmt.Printf("Polling leader=%q err=%v\n", res.Leader, err)
		select {
		case <-startupCtx.Done():
		case <-time.After(r.cfg.PollingInterval):
		}
	}

	if r.cfg.Leader == "" {
		return fmt.Errorf("No leader found/cluster not ready\n")
	}

	return nil
}

func killPod(podName string) error {
	if err := execCommand("kubectl", append([]string{}, "delete", "pod", podName)); err != nil {
		return err
	}
	return nil
}

func cleanupPortForwards() {
	fmt.Println("\n\n==Closing open port forwards==")
	for _, cmd := range portForwards {
		if cmd == nil || cmd.Process == nil {
			continue
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			continue
		}
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			fmt.Printf("Couldn't stop process %d: %v\n", cmd.Process.Pid, err)
		}
	}
}

//context wrappers for requests
func getContext(key, reqId string) (KVResponse, error) {
	getCtx, getCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer getCancel()

	if reqId == "" {
		reqId = getReqId()
	}

	res, err := r.get(getCtx, key, getReqId())

	return res, err
}

func putContext(key, value, reqId string) (KVResponse, error) {
	putCtx, putCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer putCancel()
	
	if reqId == "" {
		reqId = getReqId()
	}
	
	res, err := r.put(putCtx, key, value, reqId)

	return res, err
}

func deleteContext(key, reqId string) (KVResponse, error) {
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), r.cfg.RequestTimeout)
	defer deleteCancel()

	if reqId == "" {
		reqId = getReqId()
	}

	res, err := r.delete(deleteCtx, key, getReqId())

	return res, err
}