package main

import (
	log "github.com/sirupsen/logrus"
)

// Workflow is responsible for managing sequential execution steps of a process
type Workflow struct {
	Steps      []step
	RowCounter int64
}

// step represents a single unit of work in the Workflow. Each step function returns nil on success or an error on failure
type step = func() error

var currentWorkflow *Workflow

func (w *Workflow) run() {
	for _, step := range w.Steps {
		err := step()
		if err != nil {
			log.Fatal(err)
		}
	}
}

// GetRowCounter returns the value of RowCounter for the current workflow
func GetRowCounter() int64 {
	return currentWorkflow.RowCounter
}

// IncrementRowCounter increments the RowCounter for the current workflow
func IncrementRowCounter() {
	currentWorkflow.RowCounter++
}

// RunWorkflow execute a workflow with the provided steps
func RunWorkflow(steps []step) {
	currentWorkflow = &Workflow{steps, 0}

	currentWorkflow.run()
}
