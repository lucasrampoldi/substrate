package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	ctx := context.Background()
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}
	k8sContext := "gke_agents-511713_europe-west1-b_prod-cluster"

	fmt.Println("Connecting to Substrate API...")
	client, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, "", "", false)
	if err != nil {
		log.Fatalf("Failed to create ateclient: %v", err)
	}
	defer client.Close()

	actorRef := &ateapipb.ObjectRef{
		Atespace: "openclaw",
		Name:     "openclaw-agent-1",
	}

	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "openclaw",
			Name:     "default",
		},
		Rules: []*ateapipb.EgressRule{
			{
				Http: &ateapipb.HttpRule{
					Hostnames: []string{"*"},
					Ports:     &ateapipb.Ports{All: &ateapipb.AllPorts{}},
				},
			},
			{
				Https: &ateapipb.HttpsRule{
					Hostnames: []string{"*"},
				},
			},
		},
	}

	fmt.Println("Creating EgressPolicy for openclaw/openclaw-agent-1...")
	created, err := client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: policy,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			fmt.Println("Policy already exists, updating...")
			updated, uerr := client.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{
				Actor:        actorRef,
				EgressPolicy: policy,
			})
			if uerr != nil {
				log.Fatalf("UpdateActorEgressPolicy failed: %v", uerr)
			}
			fmt.Printf("Successfully updated EgressPolicy: %+v\n", updated.GetMetadata())
			return
		}
		log.Fatalf("CreateActorEgressPolicy failed: %v", err)
	}

	fmt.Printf("Successfully created EgressPolicy: %+v\n", created.GetMetadata())
}
