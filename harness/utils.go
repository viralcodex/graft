package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

var portForwards []*exec.Cmd

func fail(scenario string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", scenario, err)
}

func getReqId() string {
	return ulid.Make().String()
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

func (r *Runner) request(ctx context.Context, body []byte, method, url string, resObj any, reqId string) error {
	var reader io.Reader

	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reader)

	if err != nil {
		return err
	}

	if body != nil {
		req.Header.Set("content-type", "application/json")
		req.Header.Set("X-Request-Id", reqId)
	}

	res, err := r.client.Do(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		data, err := io.ReadAll(res.Body)

		if err != nil {
			return err
		}

		return fmt.Errorf("%s %s failed: status=%d body=%s", method, url, res.StatusCode, string(data))
	}

	if resObj == nil {
		return nil
	}

	return json.NewDecoder(res.Body).Decode(resObj)
}

func getPortPairs(addr string) string {
	publicAddr := strings.Split(r.cfg.PeerPublicAddresses[addr], ":")
	publicPort := publicAddr[len(publicAddr)-1]

	address := strings.Split(addr, ":")
	port := address[len(address)-1]

	return publicPort + ":" + port
}

func cleanupPortForwards() {
	fmt.Println("\n\n===Closing open port forwards===")
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
