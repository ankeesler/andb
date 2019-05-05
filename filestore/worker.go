package filestore

import (
	"log"
)

const MaxWorkAttempts = 3

type work struct {
	description string
	action      func() error

	attempts int
}

func newWork(description string, action func() error) *work {
	return &work{
		description: description,
		action:      action,
		attempts:    0,
	}
}

type worker struct {
	workC chan *work
}

func newWorker(workC chan *work) *worker {
	return &worker{
		workC: workC,
	}
}

func (w *worker) start() {
	go func() {
		log.Printf("worker starting")
		for work := range w.workC {
			if err := work.action(); err != nil {
				log.Printf("work failed (%s): %s", work.description, err.Error())
				work.attempts++
				if work.attempts < MaxWorkAttempts {
					w.workC <- work
				} else {
					log.Printf("work hit max attempts (%s)", work.description)
				}
			}
		}
	}()
}
