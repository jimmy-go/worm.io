// Package worm contains batch and sheduler tools.
//
// MIT License
//
// Copyright (c) 2016 Angel Del Castillo
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package worm

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron"
)

var (
	// ErrDoerNil error returned when call Register method with nil interface.
	ErrDoerNil = errors.New("worm: doer is nil")
)

// Worm struct.
type Worm struct {
	Doers  map[string]Doer
	croner *cron.Cron

	sync.RWMutex
}

// New will return a new worm ready for register jobs type and queue.
func New() (*Worm, error) {
	c := cron.New()
	x := &Worm{
		Doers:  make(map[string]Doer),
		croner: c,
	}
	c.Start()
	return x, nil
}

// Register register the worker interface for this worm.
func (w *Worm) Register(name string, doer Doer) error {
	if doer == nil {
		return ErrDoerNil
	}
	_, ok := w.Doers[name]
	if ok {
		return errors.New("worm: doer already exists")
	}
	w.Doers[name] = doer
	return nil
}

// MustRegister register the worker interface for this worm.
func (w *Worm) MustRegister(name string, doer Doer) {
	err := w.Register(name, doer)
	if err != nil {
		panic(err)
	}
}

// Queue will cron the job for execution on cronformat.
func (w *Worm) Queue(jobName string, v interface{}) error {
	crons := nowCron(time.Now())
	return w.Sched(crons, jobName, v)
}

// Sched will cron the job for execution on cronformat.
func (w *Worm) Sched(cronformat, jobName string, v interface{}) error {
	w.RLock()
	defer w.RUnlock()

	dd, ok := w.Doers[jobName]
	if !ok {
		return errors.New("worm: doer not found")
	}

	err := w.croner.AddFunc(cronformat, func() {
		ns, err := dd.Run(0, v)
		if err != nil {
			log.Printf("job err [%s]", err)
		}
		log.Printf("job done [%v]", ns)
	})
	if err != nil {
		return err
	}

	return nil
}

// Doer interface. Your implementation must make sure that
// resources are only available there and reusable. For example you 'almost'
// everytime will need only one database connection for resources economy.
type Doer interface {
	// Run executes the job since status. If work is done return status. If fails
	// return last status and error detail.
	Run(int, interface{}) (int, error)
}

// nowCron return cron string with t date. Once execution.
func nowCron(t time.Time) string {
	t = t.Add(3 * time.Second)
	return fmt.Sprintf("%v %v %v %v %v *", t.Second(), t.Minute(), t.Hour(), t.Day(), int(t.Month()))
}
