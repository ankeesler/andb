package filestore

import (
	log "github.com/sirupsen/logrus"
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
		log.Debugf("worker starting")
		for work := range w.workC {
			if err := work.action(); err != nil {
				log.Warnf("work failed (%s): %s", work.description, err.Error())
				work.attempts++
				if work.attempts < MaxWorkAttempts {
					w.workC <- work
				} else {
					log.Warnf("work hit max attempts (%s)", work.description)
				}
			}
		}
	}()
}
