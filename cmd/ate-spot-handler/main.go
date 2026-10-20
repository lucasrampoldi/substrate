// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main implements ate-spot-handler, a node-local daemon running on GKE Spot VMs.
// It continuously polls the GCP Compute Engine metadata server for preemption notices
// (the 30-second warning) and selectively executes SuspendActor for stateful actors
// assigned to the local node so their memory and disk diffs are durably preserved in GCS.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	nodeNameFlag       = pflag.String("node-name", "", "Kubernetes node name to monitor (defaults to NODE_NAME env var).")
	checkIntervalFlag  = pflag.Duration("check-interval", 2*time.Second, "Polling frequency for GCP preemption notice.")
	metadataURLFlag    = pflag.String("metadata-url", "http://metadata.google.internal/computeMetadata/v1/instance/preempted", "GCP metadata preemption endpoint.")
	maintURLFlag       = pflag.String("maintenance-url", "http://metadata.google.internal/computeMetadata/v1/instance/maintenance-event", "GCP metadata maintenance endpoint.")
	atelBackendURLFlag = pflag.String("atel-backend-url", "http://atel-backend-svc.atel-system.svc.cluster.local", "URL of atel-backend for actor policy lookups.")
	endpointFlag       = pflag.String("endpoint", "", "gRPC endpoint of ate-api-server (e.g. api.ate-system.svc:443).")
	kubeconfigFlag     = pflag.String("kubeconfig", "", "Path to kubeconfig file (defaults to in-cluster).")
	k8sContextFlag     = pflag.String("context", "", "Kubeconfig context to use.")
	tokenFileFlag      = pflag.String("token-file", "", "Path to bearer token for ate-api authentication.")
	dryRunFlag         = pflag.Bool("dry-run", false, "Simulate suspension without calling ate-api-server.")
	versionFlag        = pflag.BoolP("version", "v", false, "Print version and exit.")
)

type AtelActorPolicy struct {
	ID            string `json:"id"`
	Atespace      string `json:"atespace"`
	SaveOnPreempt bool   `json:"saveOnPreempt"`
}

func main() {
	pflag.Parse()

	if *versionFlag {
		fmt.Println("ate-spot-handler v1.0.0")
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	nodeName := *nodeNameFlag
	if nodeName == "" {
		nodeName = os.Getenv("NODE_NAME")
	}
	if nodeName == "" {
		slog.Error("node-name must be specified or NODE_NAME environment variable set")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.InfoContext(ctx, "Starting ate-spot-handler daemon",
		slog.String("node_name", nodeName),
		slog.Duration("check_interval", *checkIntervalFlag),
		slog.String("metadata_url", *metadataURLFlag),
		slog.String("endpoint", *endpointFlag),
		slog.Bool("dry_run", *dryRunFlag),
	)

	client, err := ateclient.NewClient(
		ctx,
		*kubeconfigFlag,
		*k8sContextFlag,
		*endpointFlag,
		*tokenFileFlag,
		false,
	)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to ate-api-server", slog.Any("err", err))
		os.Exit(1)
	}
	defer client.Close()

	httpClient := &http.Client{Timeout: 3 * time.Second}
	ticker := time.NewTicker(*checkIntervalFlag)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "ate-spot-handler stopping")
			return
		case <-ticker.C:
			preempted := checkGCPPreemption(ctx, httpClient, *metadataURLFlag, *maintURLFlag)
			if !preempted {
				continue
			}

			slog.WarnContext(ctx, "🚨 GCP SPOT PREEMPTION NOTICE DETECTED! 30s grace period initiated",
				slog.String("node_name", nodeName),
			)

			// Execute preemption save workflow
			handlePreemption(ctx, client, httpClient, *atelBackendURLFlag, nodeName, *dryRunFlag)

			slog.InfoContext(ctx, "Preemption handling completed. Waiting for node shutdown...")
			// Sleep indefinitely until node termination to avoid repeated loops
			<-ctx.Done()
			return
		}
	}
}

func checkGCPPreemption(ctx context.Context, client *http.Client, preemptURL, maintURL string) bool {
	// 1. Check preempted endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, preemptURL, nil)
	if err == nil {
		req.Header.Set("Metadata-Flavor", "Google")
		resp, err := client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if strings.TrimSpace(string(body)) == "TRUE" {
				return true
			}
		}
	}

	// 2. Check maintenance event (host termination)
	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, maintURL, nil)
	if err == nil {
		req2.Header.Set("Metadata-Flavor", "Google")
		resp, err := client.Do(req2)
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			val := strings.TrimSpace(string(body))
			if val == "TERMINATE_ON_HOST_MAINTENANCE" {
				return true
			}
		}
	}

	return false
}

func fetchActorPolicies(ctx context.Context, client *http.Client, backendURL string) map[string]bool {
	policies := make(map[string]bool)
	if backendURL == "" {
		return policies
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, backendURL+"/api/v1/internal/actors", nil)
	if err != nil {
		return policies
	}

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return policies
	}
	defer resp.Body.Close()

	var actors []AtelActorPolicy
	if err := json.NewDecoder(resp.Body).Decode(&actors); err != nil {
		return policies
	}

	for _, a := range actors {
		policies[fmt.Sprintf("%s/%s", a.Atespace, a.ID)] = a.SaveOnPreempt
		policies[a.ID] = a.SaveOnPreempt
	}
	return policies
}

func handlePreemption(ctx context.Context, cli *ateclient.Client, httpCli *http.Client, backendURL, nodeName string, dryRun bool) {
	startTime := time.Now()
	policies := fetchActorPolicies(ctx, httpCli, backendURL)

	resp, err := cli.ListActors(ctx, &ateapipb.ListActorsRequest{
		Options: &ateapipb.ListOptions{PageSize: 500},
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to list actors during preemption", slog.Any("err", err))
		return
	}

	var statefulCount, skippedCount int

	for _, actor := range resp.GetActors() {
		assignedNode := actor.GetStatus().GetAssignedNode()
		if assignedNode != "" && nodeName != "" && assignedNode != nodeName {
			continue
		}
		if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
			continue
		}

		atespace := actor.GetMetadata().GetAtespace()
		name := actor.GetMetadata().GetName()
		fullName := fmt.Sprintf("%s/%s", atespace, name)

		// Check policy
		saveOnPreempt := true
		if p, ok := policies[fullName]; ok {
			saveOnPreempt = p
		} else if p, ok := policies[name]; ok {
			saveOnPreempt = p
		}

		if !saveOnPreempt {
			slog.InfoContext(ctx, "Skipping ephemeral actor on preempted node",
				slog.String("actor", fullName),
			)
			skippedCount++
			continue
		}

		statefulCount++
		if dryRun {
			slog.InfoContext(ctx, "[DRY-RUN] Would suspend actor on preemption",
				slog.String("actor", fullName),
			)
			continue
		}

		slog.InfoContext(ctx, "Suspending stateful actor to GCS before node shutdown",
			slog.String("actor", fullName),
		)

		snapStart := time.Now()
		actorRef := resources.ActorRef{Atespace: atespace, Name: name}
		_, err := cli.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor: actorRef.ToObjectRef(),
		})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to suspend actor during preemption",
				slog.String("actor", fullName),
				slog.Any("err", err),
			)
		} else {
			slog.InfoContext(ctx, "Actor successfully persisted to GCS",
				slog.String("actor", fullName),
				slog.Duration("duration", time.Since(snapStart)),
			)
		}
	}

	slog.InfoContext(ctx, "Finished spot preemption actions",
		slog.Int("stateful_persisted", statefulCount),
		slog.Int("ephemeral_skipped", skippedCount),
		slog.Duration("total_duration", time.Since(startTime)),
	)
}
