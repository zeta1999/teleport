package main

import (
	log "github.com/sirupsen/logrus"
)

// Workflow is responsible for managing sequential execution steps of a process
type Workflow struct {
	Steps []step
}

// step represents a single unit of work in the Workflow. Each step function returns nil on success or an error on failure
type step = func() error

func (w *Workflow) run() {
	for _, step := range w.Steps {
		err := step()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func runWorkflow(steps []step) {
	workflow := Workflow{steps}

	workflow.run()
}
