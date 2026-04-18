package scaler

import (
	"os"

	"github.com/shirou/gopsutil/v3/process"
)

type ProcessMetrics struct {
	CPUUsage    float64
	MemoryUsage float32
}

var currentProcess *process.Process

func InitProcessMonitor() error {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return err
	}

	currentProcess = p
	return nil
}

func CurrentProcessCPUAndMemory() (ProcessMetrics, error) {
	if currentProcess == nil {
		if err := InitProcessMonitor(); err != nil {
			return ProcessMetrics{}, err
		}
	}

	cpuPercent, err := currentProcess.CPUPercent()
	if err != nil {
		return ProcessMetrics{}, err
	}

	memPercent, err := currentProcess.MemoryPercent()
	if err != nil {
		return ProcessMetrics{}, err
	}

	return ProcessMetrics{
		CPUUsage:    cpuPercent,
		MemoryUsage: memPercent,
	}, nil
}
