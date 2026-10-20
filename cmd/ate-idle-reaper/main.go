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

// Package main implements ate-idle-reaper, a background service that continuously
// inspects RUNNING actors in Substrate and suspends any that have been inactive
// for longer than the configured idle timeout.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/idlereaper"
)

var (
	idleTimeoutFlag     = pflag.Duration("idle-timeout", 1*time.Minute, "Duration of inactivity before an actor is considered idle and suspended.")
	checkIntervalFlag   = pflag.Duration("check-interval", 1*time.Minute, "Frequency of idle reaper reconciliation passes.")
	atespaceFlag        = pflag.StringP("atespace", "a", "", "Atespace to monitor. If empty, monitors all atespaces.")
	dryRunFlag          = pflag.Bool("dry-run", false, "Log what would be reaped without calling SuspendActor.")
	routerStatusURLFlag = pflag.String("router-status-url", "", "URL of atenet-router /statusz to cross-reference recent traffic.")
	endpointFlag        = pflag.String("endpoint", "", "gRPC endpoint of ate-api-server (e.g. localhost:8080 or k8s:///api.ate-system.svc:443).")
	kubeconfigFlag      = pflag.String("kubeconfig", "", "Path to kubeconfig file (defaults to in-cluster or ~/.kube/config).")
	k8sContextFlag      = pflag.String("context", "", "Kubeconfig context to use.")
	tokenFileFlag       = pflag.String("token-file", "", "Path to bearer token for ate-api authentication.")
	traceEnabledFlag    = pflag.Bool("trace", false, "Enable gRPC trace logging.")
	versionFlag         = pflag.BoolP("version", "v", false, "Print version and exit.")
)

func main() {
	pflag.Parse()

	if *versionFlag {
		fmt.Println("ate-idle-reaper v1.0.0")
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.InfoContext(ctx, "Initializing ate-idle-reaper",
		slog.Duration("idle_timeout", *idleTimeoutFlag),
		slog.Duration("check_interval", *checkIntervalFlag),
		slog.String("atespace", *atespaceFlag),
		slog.Bool("dry_run", *dryRunFlag),
		slog.String("endpoint", *endpointFlag),
	)

	client, err := ateclient.NewClient(
		ctx,
		*kubeconfigFlag,
		*k8sContextFlag,
		*endpointFlag,
		*tokenFileFlag,
		*traceEnabledFlag,
	)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to ate-api-server", slog.Any("err", err))
		os.Exit(1)
	}
	defer client.Close()

	cfg := idlereaper.Config{
		IdleTimeout:     *idleTimeoutFlag,
		Atespace:        *atespaceFlag,
		DryRun:          *dryRunFlag,
		RouterStatusURL: *routerStatusURLFlag,
	}

	reaper := idlereaper.New(client, cfg)

	if err := reaper.Run(ctx, *checkIntervalFlag); err != nil && err != context.Canceled {
		slog.ErrorContext(ctx, "Reaper daemon terminated with error", slog.Any("err", err))
		os.Exit(1)
	}

	slog.InfoContext(ctx, "ate-idle-reaper shut down cleanly")
}
