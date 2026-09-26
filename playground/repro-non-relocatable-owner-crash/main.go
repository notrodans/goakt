// MIT License
//
// Copyright (c) 2022-2026 GoAkt Team
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package main reproduces cluster lookup returning a named non-relocatable actor
// from an endpoint that membership already knows has departed.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery/static"
	inet "github.com/tochemey/goakt/v4/internal/net"
	goaktlog "github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
)

const (
	host       = "127.0.0.1"
	systemName = "non-relocatable-crash-repro"
	actorName  = "stable-worker"

	childModeEnv       = "GOAKT_CRASH_REPRO_CHILD"
	parentDiscoveryEnv = "GOAKT_CRASH_REPRO_PARENT_DISCOVERY"
	childDiscoveryEnv  = "GOAKT_CRASH_REPRO_CHILD_DISCOVERY"
	childPeersEnv      = "GOAKT_CRASH_REPRO_CHILD_PEERS"
	childRemoteEnv     = "GOAKT_CRASH_REPRO_CHILD_REMOTE"

	peerWait       = 20 * time.Second
	lookupWait     = 10 * time.Second
	operationLimit = time.Second
)

type worker struct{}

var _ actor.Actor = (*worker)(nil)

func (*worker) PreStart(*actor.Context) error {
	return nil
}

func (*worker) Receive(ctx *actor.ReceiveContext) {
	ctx.Unhandled()
}

func (*worker) PostStop(*actor.Context) error {
	return nil
}

func main() {
	if os.Getenv(childModeEnv) == "1" {
		runOwner()
		return
	}

	runSurvivor()
}

func runSurvivor() {
	ctx := context.Background()
	ports := inet.Get(6)

	parentDiscovery, parentPeers, parentRemote := ports[0], ports[1], ports[2]
	childDiscovery, childPeers, childRemote := ports[3], ports[4], ports[5]
	hosts := []string{
		fmt.Sprintf("%s:%d", host, parentDiscovery),
		fmt.Sprintf("%s:%d", host, childDiscovery),
	}

	survivor := newSystem(parentDiscovery, parentPeers, parentRemote, hosts)
	if err := survivor.Start(ctx); err != nil {
		fatal("start survivor: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = survivor.Stop(stopCtx)
	}()

	executable, err := os.Executable()
	if err != nil {
		fatal("resolve executable: %v", err)
	}

	command := exec.Command(executable)
	command.Env = append(
		os.Environ(),
		childModeEnv+"=1",
		parentDiscoveryEnv+"="+strconv.Itoa(parentDiscovery),
		childDiscoveryEnv+"="+strconv.Itoa(childDiscovery),
		childPeersEnv+"="+strconv.Itoa(childPeers),
		childRemoteEnv+"="+strconv.Itoa(childRemote),
	)

	stdout, err := command.StdoutPipe()
	if err != nil {
		fatal("open owner stdout: %v", err)
	}

	var stderr bytes.Buffer
	command.Stderr = &stderr

	if err := command.Start(); err != nil {
		fatal("start owner process: %v", err)
	}
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()

	if err := waitReady(stdout, 15*time.Second); err != nil {
		fatal("%v; owner stderr: %s", err, stderr.String())
	}

	waitForPeerCount(ctx, survivor, 1, peerWait)

	owner := waitForActor(ctx, survivor, actorName, lookupWait)
	if owner.IsLocal() {
		fatal("actor unexpectedly resolved locally before owner loss")
	}
	fmt.Printf("owner before crash: %s (remote=%t)\n", owner.ID(), owner.IsRemote())

	if err := command.Process.Kill(); err != nil {
		fatal("kill owner process: %v", err)
	}
	_ = command.Wait()

	waitForPeerCount(ctx, survivor, 0, peerWait)
	fmt.Println("owner process killed; survivor membership now reports zero peers")

	lookupBroken := false

	lookupCtx, cancel := context.WithTimeout(ctx, operationLimit)
	stalePID, lookupErr := survivor.ActorOf(lookupCtx, actorName)
	cancel()
	switch {
	case lookupErr == nil && stalePID != nil:
		fmt.Printf("ActorOf after departure: %s (remote=%t)\n", stalePID.ID(), stalePID.IsRemote())
		if stalePID.IsRemote() {
			lookupBroken = true
		}
	default:
		fmt.Printf("ActorOf after departure: err=%v\n", lookupErr)
	}

	existsCtx, cancel := context.WithTimeout(ctx, operationLimit)
	exists, existsErr := survivor.ActorExists(existsCtx, actorName)
	cancel()
	if existsErr != nil {
		fmt.Printf("ActorExists after departure: err=%v\n", existsErr)
	} else {
		fmt.Printf("ActorExists after departure: %t\n", exists)
		if exists {
			lookupBroken = true
		}
	}

	if lookupBroken {
		fmt.Println("REPRO (broken): lookup still reports an actor owned by a departed endpoint after membership reports zero peers")
		os.Exit(1)
	}

	fmt.Println("OK: lookup did not report an actor owned by the departed endpoint")
}

func runOwner() {
	ctx := context.Background()
	parentDiscovery := mustEnvInt(parentDiscoveryEnv)
	childDiscovery := mustEnvInt(childDiscoveryEnv)
	childPeers := mustEnvInt(childPeersEnv)
	childRemote := mustEnvInt(childRemoteEnv)

	hosts := []string{
		fmt.Sprintf("%s:%d", host, parentDiscovery),
		fmt.Sprintf("%s:%d", host, childDiscovery),
	}

	owner := newSystem(childDiscovery, childPeers, childRemote, hosts)
	if err := owner.Start(ctx); err != nil {
		fatal("start owner: %v", err)
	}

	waitForPeerCount(ctx, owner, 1, peerWait)

	pid, err := owner.Spawn(
		ctx,
		actorName,
		&worker{},
		actor.WithRelocationDisabled(),
	)
	if err != nil {
		fatal("spawn owner actor: %v", err)
	}

	fmt.Printf("ready %s\n", pid.ID())

	// The parent terminates this process with Process.Kill. Do not call Stop:
	// graceful cleanup would remove the condition this reproducer needs.
	select {}
}

func newSystem(discoveryPort, peersPort, remotePort int, hosts []string) actor.ActorSystem {
	clusterConfig := actor.NewClusterConfig().
		WithDiscovery(static.NewDiscovery(&static.Config{Hosts: hosts})).
		WithDiscoveryPort(discoveryPort).
		WithPeersPort(peersPort).
		WithKinds(&worker{}).
		WithPartitionCount(7).
		WithReplicaCount(2).
		WithMinimumPeersQuorum(1).
		WithBootstrapTimeout(time.Second).
		WithReadTimeout(250 * time.Millisecond).
		WithWriteTimeout(250 * time.Millisecond).
		WithClusterStateSyncInterval(500 * time.Millisecond).
		WithConvergenceTimeout(2 * time.Second).
		WithShutdownTimeout(2 * time.Second).
		WithNetworkProfile(actor.NetworkProfileLocal)

	system, err := actor.NewActorSystem(
		systemName,
		actor.WithLogger(goaktlog.DiscardLogger),
		actor.WithRemote(remote.NewConfig(host, remotePort)),
		actor.WithCluster(clusterConfig),
	)
	if err != nil {
		fatal("construct actor system: %v", err)
	}

	return system
}

func waitReady(reader io.Reader, timeout time.Duration) error {
	result := make(chan error, 1)

	go func() {
		line, err := bufio.NewReader(reader).ReadString('\n')
		if err != nil {
			result <- fmt.Errorf("read owner readiness: %w", err)
			return
		}
		if len(line) < len("ready ") || line[:len("ready ")] != "ready " {
			result <- fmt.Errorf("unexpected owner readiness %q", line)
			return
		}
		result <- nil
	}()

	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("timed out waiting for owner readiness")
	}
}

func waitForPeerCount(ctx context.Context, system actor.ActorSystem, count int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		peers, err := system.Peers(ctx, operationLimit)
		if err == nil && len(peers) == count {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	fatal("peer count did not become %d within %s", count, timeout)
}

func waitForActor(ctx context.Context, system actor.ActorSystem, name string, timeout time.Duration) *actor.PID {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		lookupCtx, cancel := context.WithTimeout(ctx, operationLimit)
		pid, err := system.ActorOf(lookupCtx, name)
		cancel()
		if err == nil && pid != nil {
			return pid
		}
		time.Sleep(100 * time.Millisecond)
	}

	fatal("actor %q did not become visible within %s", name, timeout)
	return nil
}

func mustEnvInt(name string) int {
	value := os.Getenv(name)
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 {
		fatal("invalid %s=%q", name, value)
	}
	return port
}

func fatal(format string, args ...any) {
	fmt.Printf("SETUP FAILURE: "+format+"\n", args...)
	os.Exit(2)
}
