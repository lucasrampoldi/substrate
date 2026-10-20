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

package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/internal/idlereaper"
)

var (
	reapAtespaceFlag     string
	reapIdleTimeoutFlag  time.Duration
	reapDryRunFlag       bool
	reapDaemonFlag       bool
	reapIntervalFlag     time.Duration
	reapRouterStatusFlag string
)

var reapCmd = &cobra.Command{
	Use:   "reap",
	Short: "Reap idle resources",
}

var reapActorsCmd = &cobra.Command{
	Use:     "actor",
	Aliases: []string{"actors"},
	Short:   "Reap idle running actors by suspending them",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := newAPIClient(cmd)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		cfg := idlereaper.Config{
			IdleTimeout:     reapIdleTimeoutFlag,
			Atespace:        reapAtespaceFlag,
			DryRun:          reapDryRunFlag,
			RouterStatusURL: reapRouterStatusFlag,
		}

		reaper := idlereaper.New(apiClient, cfg)

		if reapDaemonFlag {
			return reaper.Run(ctx, reapIntervalFlag)
		}

		report, err := reaper.ReapOnce(ctx)
		if err != nil {
			return fmt.Errorf("reap failed: %w", err)
		}

		out := cmd.OutOrStdout()
		fmt.Fprintf(out, "Scanned: %d actors (%d running, %d skipped)\n",
			report.ScannedTotal, report.RunningActors, report.SkippedActors)

		if len(report.ReapedActors) == 0 {
			fmt.Fprintln(out, "No idle actors to reap.")
			return nil
		}

		for _, a := range report.ReapedActors {
			prefix := "SUSPENDED"
			if a.DryRun {
				prefix = "[DRY-RUN] WOULD SUSPEND"
			}
			fmt.Fprintf(out, "%s: %s/%s (idle for %s)\n",
				prefix, a.Atespace, a.Name, a.IdleDuration.Round(time.Second))
		}
		return nil
	},
}

func init() {
	reapActorsCmd.Flags().StringVarP(&reapAtespaceFlag, "atespace", "a", "", "the atespace to inspect (default: all)")
	reapActorsCmd.Flags().DurationVar(&reapIdleTimeoutFlag, "idle-timeout", 1*time.Minute, "inactivity timeout before an actor is considered idle")
	reapActorsCmd.Flags().BoolVar(&reapDryRunFlag, "dry-run", false, "display what would be reaped without calling suspend")
	reapActorsCmd.Flags().BoolVar(&reapDaemonFlag, "daemon", false, "run continuously as a background loop")
	reapActorsCmd.Flags().DurationVar(&reapIntervalFlag, "interval", 1*time.Minute, "frequency of checks when running in daemon mode")
	reapActorsCmd.Flags().StringVar(&reapRouterStatusFlag, "router-status-url", "", "URL to atenet-router /statusz to verify traffic")

	reapCmd.AddCommand(reapActorsCmd)
	rootCmd.AddCommand(reapCmd)
}
