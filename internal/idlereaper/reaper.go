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

package idlereaper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Config configures the Idle Reaper behavior.
type Config struct {
	// IdleTimeout is the duration of inactivity after which a RUNNING actor is suspended.
	IdleTimeout time.Duration

	// Atespace filters the reap operation to a single atespace. Empty means all atespaces.
	Atespace string

	// DryRun logs what would be reaped without calling SuspendActor.
	DryRun bool

	// RouterStatusURL optionally points to an atenet-router /statusz?format=json endpoint
	// to cross-check recent ingress query timestamps.
	RouterStatusURL string

	// HTTPClient is the client used to query RouterStatusURL.
	HTTPClient *http.Client
}

// ActorClient is the subset of ateapi RPCs needed by the Idle Reaper.
type ActorClient interface {
	ListActors(ctx context.Context, in *ateapipb.ListActorsRequest, opts ...grpc.CallOption) (*ateapipb.ListActorsResponse, error)
	SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error)
}

// Reaper checks for idle RUNNING actors and suspends them.
type Reaper struct {
	client ActorClient
	cfg    Config
}

// New creates a new Reaper instance.
func New(client ActorClient, cfg Config) *Reaper {
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 1 * time.Minute
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Reaper{
		client: client,
		cfg:    cfg,
	}
}

// NewFromATEClient creates a Reaper wrapping the standard ateclient.Client.
func NewFromATEClient(cli *ateclient.Client, cfg Config) *Reaper {
	return New(cli, cfg)
}

// ReapReport summarizes the outcome of one reap cycle.
type ReapReport struct {
	ScannedTotal   int           `json:"scanned_total"`
	RunningActors  int           `json:"running_actors"`
	ReapedActors   []ActorReaped `json:"reaped_actors"`
	SkippedActors  int           `json:"skipped_actors"`
	CycleDuration  time.Duration `json:"cycle_duration"`
}

// ActorReaped records an actor that was suspended (or would be suspended under DryRun).
type ActorReaped struct {
	Atespace     string        `json:"atespace"`
	Name         string        `json:"name"`
	IdleDuration time.Duration `json:"idle_duration"`
	DryRun       bool          `json:"dry_run"`
}

// ReapOnce runs a single pass over actors and suspends eligible idle ones.
func (r *Reaper) ReapOnce(ctx context.Context) (*ReapReport, error) {
	start := time.Now()
	report := &ReapReport{
		ReapedActors: make([]ActorReaped, 0),
	}

	// 1. Fetch recent queries from router if configured
	lastActivityMap := r.fetchRecentActivity(ctx)

	// 2. List all actors
	pageToken := ""
	var running []*ateapipb.Actor

	for {
		resp, err := r.client.ListActors(ctx, &ateapipb.ListActorsRequest{
			Options:  &ateapipb.ListOptions{PageSize: 500, PageToken: pageToken},
			Atespace: r.cfg.Atespace,
		})
		if err != nil {
			return nil, fmt.Errorf("list actors failed: %w", err)
		}

		for _, actor := range resp.GetActors() {
			report.ScannedTotal++
			if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
				running = append(running, actor)
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	report.RunningActors = len(running)
	now := time.Now()

	// 3. Evaluate each running actor
	for _, actor := range running {
		atespace := actor.GetMetadata().GetAtespace()
		name := actor.GetMetadata().GetName()
		fullName := fmt.Sprintf("%s/%s", atespace, name)

		// Base idle duration from updateTime (when it entered RUNNING or was last modified)
		updateTime := actor.GetMetadata().GetUpdateTime().AsTime()
		if updateTime.IsZero() {
			updateTime = actor.GetMetadata().GetCreateTime().AsTime()
		}

		lastActive := updateTime
		// If router recorded more recent traffic for this actor, use that
		if routerTime, ok := lastActivityMap[fullName]; ok && routerTime.After(lastActive) {
			lastActive = routerTime
		}

		idleDuration := now.Sub(lastActive)

		if idleDuration < r.cfg.IdleTimeout {
			report.SkippedActors++
			continue
		}

		// Actor has exceeded IdleTimeout -> reap it
		if r.cfg.DryRun {
			slog.InfoContext(ctx, "[DRY-RUN] Would suspend idle actor",
				slog.String("actor", fullName),
				slog.Duration("idle_duration", idleDuration.Round(time.Second)),
				slog.Duration("idle_timeout", r.cfg.IdleTimeout),
			)
			report.ReapedActors = append(report.ReapedActors, ActorReaped{
				Atespace:     atespace,
				Name:         name,
				IdleDuration: idleDuration,
				DryRun:       true,
			})
			continue
		}

		slog.InfoContext(ctx, "Suspending idle actor",
			slog.String("actor", fullName),
			slog.Duration("idle_duration", idleDuration.Round(time.Second)),
			slog.Duration("idle_timeout", r.cfg.IdleTimeout),
		)

		actorRef := resources.ActorRef{Atespace: atespace, Name: name}
		_, err := r.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor: actorRef.ToObjectRef(),
		})
		if err != nil {
			slog.ErrorContext(ctx, "Failed to suspend idle actor",
				slog.String("actor", fullName),
				slog.Any("err", err),
			)
			continue
		}

		report.ReapedActors = append(report.ReapedActors, ActorReaped{
			Atespace:     atespace,
			Name:         name,
			IdleDuration: idleDuration,
			DryRun:       false,
		})
	}

	report.CycleDuration = time.Since(start)
	return report, nil
}

// Run executes the reaper continuously on ticker intervals until ctx is canceled.
func (r *Reaper) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 1 * time.Minute
	}

	slog.InfoContext(ctx, "Starting Idle Reaper daemon",
		slog.Duration("idle_timeout", r.cfg.IdleTimeout),
		slog.Duration("interval", interval),
		slog.String("atespace", r.cfg.Atespace),
		slog.Bool("dry_run", r.cfg.DryRun),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial pass immediately
	if report, err := r.ReapOnce(ctx); err != nil {
		slog.WarnContext(ctx, "Initial reap pass failed", slog.Any("err", err))
	} else if len(report.ReapedActors) > 0 {
		slog.InfoContext(ctx, "Initial reap pass completed",
			slog.Int("reaped", len(report.ReapedActors)),
			slog.Int("running", report.RunningActors),
		)
	}

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Idle Reaper stopping")
			return ctx.Err()
		case <-ticker.C:
			report, err := r.ReapOnce(ctx)
			if err != nil {
				slog.WarnContext(ctx, "Reap cycle error", slog.Any("err", err))
				continue
			}
			if len(report.ReapedActors) > 0 {
				slog.InfoContext(ctx, "Reap cycle completed",
					slog.Int("reaped", len(report.ReapedActors)),
					slog.Int("running", report.RunningActors),
					slog.Duration("duration", report.CycleDuration.Round(time.Millisecond)),
				)
			}
		}
	}
}

// fetchRecentActivity queries the router status endpoint if configured.
func (r *Reaper) fetchRecentActivity(ctx context.Context) map[string]time.Time {
	activity := make(map[string]time.Time)
	if r.cfg.RouterStatusURL == "" {
		return activity
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.RouterStatusURL, nil)
	if err != nil {
		return activity
	}

	resp, err := r.cfg.HTTPClient.Do(req)
	if err != nil {
		return activity
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return activity
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return activity
	}

	type statuszPayload struct {
		Queries []struct {
			Timestamp string `json:"timestamp"`
			Target    string `json:"target"`
		} `json:"queries"`
	}

	var data statuszPayload
	if err := json.Unmarshal(body, &data); err != nil {
		return activity
	}

	for _, q := range data.Queries {
		if q.Target == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, q.Timestamp)
		if err != nil {
			continue
		}
		if existing, ok := activity[q.Target]; !ok || t.After(existing) {
			activity[q.Target] = t
		}
	}

	return activity
}
