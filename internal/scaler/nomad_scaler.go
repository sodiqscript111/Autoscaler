package scaler

import (
	"context"
	"fmt"
	"time"

	"autoscaler/internal/config"
	"autoscaler/internal/redisx"

	nomad "github.com/hashicorp/nomad/api"
)

type NomadScaler struct {
	client      *nomad.Client
	redisClient *redisx.Client
	config      config.NomadConfig
}

func NewNomadScaler(cfg config.NomadConfig, redisClient *redisx.Client) (*NomadScaler, error) {
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
		client:      client,
		redisClient: redisClient,
		config:      cfg,
	}, nil
}

func (n *NomadScaler) ApplyNomadScaling(ctx context.Context, decision Decision, maxedOut, minOut bool) {
	if !n.config.Enabled {
		return
	}

	if !maxedOut && !minOut {

		return
	}

	lockKey := "autoscaler:nomad_leader_lock"
	acquired, err := n.redisClient.SetNX(ctx, lockKey, "locked", 5*time.Second)
	if err != nil {
		fmt.Printf("[nomad] failed to acquire leader lock: %v\n", err)
		return
	}
	if !acquired {

		return
	}

	defer n.redisClient.SetNX(ctx, lockKey, "released", 1*time.Millisecond)

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
