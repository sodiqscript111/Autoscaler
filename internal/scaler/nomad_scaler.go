package scaler

import (
	"context"
	"fmt"
	"autoscaler/internal/config"

	nomad "github.com/hashicorp/nomad/api"
)

type NomadScaler struct {
	client *nomad.Client
	config config.NomadConfig
}

func NewNomadScaler(cfg config.NomadConfig, _ any) (*NomadScaler, error) {
	if !cfg.Enabled {
		return &NomadScaler{}, nil
	}

	nomadConfig := nomad.DefaultConfig()
	nomadConfig.Address = cfg.Address
	client, err := nomad.NewClient(nomadConfig)
	if err != nil {
		return nil, err
	}

	return &NomadScaler{
		client: client,
		config: cfg,
	}, nil
}

func (n *NomadScaler) ApplyNomadScaling(ctx context.Context, decision Decision, maxedOut, minOut bool) {
	if !n.config.Enabled {
		return
	}

	if !maxedOut && !minOut {

		return
	}

	// No locking needed since this runs in a standalone singleton controller

	job, _, err := n.client.Jobs().Info(n.config.JobName, nil)
	if err != nil {
		fmt.Printf("[nomad] failed to fetch job info: %v\n", err)
		return
	}

	var targetGroup *nomad.TaskGroup
	for _, group := range job.TaskGroups {
		if *group.Name == n.config.GroupName {
			targetGroup = group
			break
		}
	}

	if targetGroup == nil {
		fmt.Printf("[nomad] task group %s not found in job %s\n", n.config.GroupName, n.config.JobName)
		return
	}

	currentScale := *targetGroup.Count
	var newScale int

	if maxedOut && decision.ScaleUp {
		newScale = currentScale + 1
		if newScale > n.config.MaxScale {
			fmt.Printf("[nomad] nomad scale limit reached (max: %d), enabling backpressure\n", n.config.MaxScale)

			return
		}
	} else if minOut && decision.ScaleDown {
		newScale = currentScale - 1
		if newScale < 1 {
			newScale = 1
		}
	}

	if newScale == currentScale {
		return
	}

	*targetGroup.Count = newScale
	_, _, err = n.client.Jobs().Register(job, nil)
	if err != nil {
		fmt.Printf("[nomad] failed to scale job: %v\n", err)
		return
	}

	action := "scale_up"
	if newScale < currentScale {
		action = "scale_down"
	}
	fmt.Printf("[nomad] %s job %s group %s allocations %d->%d\n", action, n.config.JobName, n.config.GroupName, currentScale, newScale)
}
