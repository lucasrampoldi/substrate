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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type mockActorClient struct {
	actors    []*ateapipb.Actor
	suspended []string
}

func (m *mockActorClient) ListActors(_ context.Context, _ *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	return &ateapipb.ListActorsResponse{
		Actors: m.actors,
	}, nil
}

func (m *mockActorClient) SuspendActor(_ context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	ref := in.GetActor()
	name := ref.GetAtespace() + "/" + ref.GetName()
	m.suspended = append(m.suspended, name)
	return &ateapipb.SuspendActorResponse{}, nil
}

func TestReapOnce_SuspendsIdleActors(t *testing.T) {
	now := time.Now()

	// Actor 1: idle for 20m (should be reaped)
	actorIdle := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "openclaw",
			Name:       "agent-old",
			UpdateTime: timestamppb.New(now.Add(-20 * time.Minute)),
		},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}

	// Actor 2: updated 2m ago (active, should NOT be reaped)
	actorActive := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "openclaw",
			Name:       "agent-active",
			UpdateTime: timestamppb.New(now.Add(-2 * time.Minute)),
		},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}

	// Actor 3: already suspended (should be ignored)
	actorSuspended := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "openclaw",
			Name:       "agent-suspended",
			UpdateTime: timestamppb.New(now.Add(-30 * time.Minute)),
		},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}

	mock := &mockActorClient{
		actors: []*ateapipb.Actor{actorIdle, actorActive, actorSuspended},
	}

	reaper := New(mock, Config{
		IdleTimeout: 10 * time.Minute,
		DryRun:      false,
	})

	report, err := reaper.ReapOnce(context.Background())
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	if report.ScannedTotal != 3 {
		t.Errorf("got ScannedTotal=%d, want 3", report.ScannedTotal)
	}
	if report.RunningActors != 2 {
		t.Errorf("got RunningActors=%d, want 2", report.RunningActors)
	}
	if len(report.ReapedActors) != 1 {
		t.Fatalf("got %d reaped actors, want 1", len(report.ReapedActors))
	}
	if report.ReapedActors[0].Name != "agent-old" {
		t.Errorf("reaped actor name = %q, want agent-old", report.ReapedActors[0].Name)
	}
	if len(mock.suspended) != 1 || mock.suspended[0] != "openclaw/agent-old" {
		t.Errorf("mock.suspended = %v, want [openclaw/agent-old]", mock.suspended)
	}
}

func TestReapOnce_DryRun(t *testing.T) {
	now := time.Now()

	actorIdle := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "openclaw",
			Name:       "agent-idle",
			UpdateTime: timestamppb.New(now.Add(-15 * time.Minute)),
		},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}

	mock := &mockActorClient{actors: []*ateapipb.Actor{actorIdle}}

	reaper := New(mock, Config{
		IdleTimeout: 10 * time.Minute,
		DryRun:      true,
	})

	report, err := reaper.ReapOnce(context.Background())
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	if len(report.ReapedActors) != 1 {
		t.Fatalf("got %d reaped actors, want 1", len(report.ReapedActors))
	}
	if !report.ReapedActors[0].DryRun {
		t.Errorf("expected DryRun=true in report")
	}
	if len(mock.suspended) != 0 {
		t.Errorf("expected 0 suspended calls in dry-run mode, got %d", len(mock.suspended))
	}
}

func TestReapOnce_TrafficActivityFromRouter(t *testing.T) {
	now := time.Now()

	// Actor whose updateTime was 20m ago, but has recent router activity 1m ago
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "openclaw",
			Name:       "agent-with-traffic",
			UpdateTime: timestamppb.New(now.Add(-20 * time.Minute)),
		},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}

	// Fake router /statusz HTTP server
	routerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"queries": [
				{
					"timestamp": "` + now.Add(-1*time.Minute).Format(time.RFC3339) + `",
					"target": "openclaw/agent-with-traffic"
				}
			]
		}`))
	}))
	defer routerSrv.Close()

	mock := &mockActorClient{actors: []*ateapipb.Actor{actor}}

	reaper := New(mock, Config{
		IdleTimeout:     10 * time.Minute,
		RouterStatusURL: routerSrv.URL,
	})

	report, err := reaper.ReapOnce(context.Background())
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	// Since traffic occurred 1m ago, it should NOT be reaped!
	if len(report.ReapedActors) != 0 {
		t.Errorf("expected 0 reaped actors due to recent traffic, got %d", len(report.ReapedActors))
	}
	if report.SkippedActors != 1 {
		t.Errorf("expected 1 skipped actor, got %d", report.SkippedActors)
	}
}
