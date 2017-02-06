// Package worm contains worm.io Queue.
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
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"

	// SQLite driver
	// _ "github.com/mattn/go-sqlite3"

	"github.com/jmoiron/sqlx"
	"github.com/robfig/cron"
	uuid "github.com/satori/go.uuid"
)

// Connect starts a default worm hub.
func Connect(connectURL string) error {
	var err error
	defaultWorm, err = New(connectURL)
	return err
}

// New connects to sqlite database and returns a new Worm hub.
func New(connectURL string) (*Worm, error) {
	db, err := sqlx.Connect("sqlite3", connectURL)
	if err != nil {
		return nil, err
	}

	c := cron.New()
	x := &Worm{
		doers:  make(map[string]Doer),
		Db:     db,
		croner: c,
	}
	c.Start()
	return x, nil
}

var (
	defaultWorm *Worm
)

const (
	// StatusStart default job status.
	StatusStart = 1
	// StatusOK success status code.
	StatusOK = 0
)

// Worm struct.
type Worm struct {
	doers  map[string]Doer
	croner *cron.Cron
	Db     *sqlx.DB
	sync.RWMutex
}

// Register register the worker for this worm. Must be called at init time.
func (w *Worm) Register(workerName string, doer Doer) error {
	if doer == nil {
		return errors.New("nil worker")
	}
	_, ok := w.doers[workerName]
	if ok {
		return errors.New("worm: worker already registered")
	}
	w.doers[workerName] = doer
	return nil
}

// MustRegister register the worker interface for this worm.
func (w *Worm) MustRegister(workerName string, doer Doer) {
	err := w.Register(workerName, doer)
	if err != nil {
		panic(err)
	}
}

// store stores the work data on database.
func (w *Worm) store(workerName string, data []byte) (Doer, string, error) {
	w.RLock()
	doer, ok := w.doers[workerName]
	w.RUnlock()
	if !ok {
		return doer, "", errors.New("worm: doer not found")
	}

	jobID := uuid.NewV4().String()

	_, err := w.Db.Exec(`
		INSERT INTO worm (id,worker_name,status,data,created_at)
		VALUES (?,?,?,?,?);
	`, jobID, workerName, StatusStart, data, time.Now().UTC())
	if err != nil {
		return doer, "", err
	}
	return doer, jobID, nil
}

// Queue will cron the job for execution on cronformat.
func (w *Worm) Queue(workerName string, data []byte) (string, error) {
	return w.Sched(workerName, data, nowCron(time.Now()))
}

// Sched will cron the job for execution on cronformat.
func (w *Worm) Sched(workerName string, data []byte, cronformat string) (string, error) {
	doer, jobID, err := w.store(workerName, data)
	if err != nil {
		return "", err
	}

	err = w.croner.AddFunc(cronformat, func() {
		logOut, status, jobErr := doer.Run(data)
		if jobErr != nil {
			log.Printf("run job : err [%s]", jobErr)
		}
		log.Printf("Sched : job done [%v]", status)

		if err := writeLog(logOut, workerName, jobID); err != nil {
			log.Printf("write log output : err [%s]", err)
		}

		_, err = w.Db.Exec(`
			UPDATE worm SET status=?,error=? WHERE id=?;
		`, status, jobErr, jobID)
		if err != nil {
			log.Printf("Sched : update status : err [%s] job id [%s]", err, jobID)
		}
	})
	if err != nil {
		return "", err
	}
	return jobID, nil
}

func writeLog(src io.Reader, workerName, jobID string) error {
	// TODO; create file in custom dir.
	fname := fmt.Sprintf("%v_%v.log", workerName, jobID)
	log.Printf("writeLog : name [%s]", fname)
	f, err := ioutil.TempFile("", fname)
	if err != nil {
		return err
	}
	log.Printf("writeLog : temp name [%s]", f.Name())
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("writeLog : close file : err [%s]", err)
		}
	}()

	_, err = io.Copy(f, src)
	if err != nil {
		return err
	}

	return nil
}

// Status return the job status by id.
func (w *Worm) Status(ID string) (int, error) {
	var status int
	err := w.Db.Get(&status, `
		SELECT status FROM worm WHERE id=?;
	`, ID)
	if err != nil {
		log.Printf("job err [%s]", err)
		return -1, err
	}
	return status, nil
}

// Close close database connections.
func (w *Worm) Close() error {
	return w.Db.Close()
}

// Register _
func Register(workerName string, doer Doer) error {
	return defaultWorm.Register(workerName, doer)
}

// MustRegister _
func MustRegister(workerName string, doer Doer) {
	defaultWorm.MustRegister(workerName, doer)
}

// Queue _
func Queue(workerName string, data []byte) (string, error) {
	return defaultWorm.Queue(workerName, data)
}

// Sched _
func Sched(workerName string, data []byte, cronformat string) (string, error) {
	return defaultWorm.Sched(workerName, data, cronformat)
}

// Status _
func Status(ID string) (int, error) {
	return defaultWorm.Status(ID)
}

// Close close database connections.
func Close() error {
	return defaultWorm.Close()
}

// Doer interface. Your implementation must make sure that
// resources are only available there and reusable. For example you 'almost'
// everytime will need only one database connection for resources economy.
type Doer interface {
	// Name returns worker name.
	Name() string

	// Run executes the job.
	// If success state returned must be zero.
	Run(data []byte) (logOut io.Reader, state int, err error)
}

// nowCron return cron string with t date. Once execution.
func nowCron(t time.Time) string {
	t = t.Add(1 * time.Second)
	return fmt.Sprintf("%v %v %v %v %v *", t.Second(), t.Minute(), t.Hour(), t.Day(), int(t.Month()))
}
