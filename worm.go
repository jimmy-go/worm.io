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
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	// SQLite driver
	_ "github.com/mattn/go-sqlite3"

	"github.com/jmoiron/sqlx"
	"github.com/robfig/cron"
	uuid "github.com/satori/go.uuid"
)

// Connect starts a default worm hub.
func Connect(connectURL, logDir string) error {
	var err error
	defaultWorm, err = New(connectURL, logDir)
	return err
}

// New connects to sqlite database and returns a new Worm hub.
func New(connectURL, logDir string) (*Worm, error) {
	if len(logDir) < 1 {
		return nil, errors.New("log directory not set")
	}
	db, err := sqlx.Connect("sqlite3", connectURL)
	if err != nil {
		return nil, err
	}

	c := cron.New()
	x := &Worm{
		doers:  make(map[string]Doer),
		Db:     db,
		croner: c,
		logDir: logDir,
		waitc:  make(chan struct{}, 1),
	}
	x.waitc <- struct{}{}
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

	// waitc channel make all the database operations without concurrency.
	// future implementations would have connection pooling.
	// see: https://godoc.org/github.com/mxk/go-sqlite/sqlite3#hdr-Concurrency
	waitc  chan struct{}
	logDir string
	sync.RWMutex
}

// Register register the worker for this worm. Must be called at init time.
func (h *Worm) Register(workerName string, doer Doer) error {
	if doer == nil {
		return errors.New("nil worker")
	}
	_, ok := h.doers[workerName]
	if ok {
		return errors.New("worm: worker already registered")
	}
	h.doers[workerName] = doer
	return nil
}

// MustRegister register the worker interface for this worm.
func (h *Worm) MustRegister(workerName string, doer Doer) {
	err := h.Register(workerName, doer)
	if err != nil {
		panic(err)
	}
}

// store stores the work data on database.
func (h *Worm) store(workerName string, data []byte) (Doer, string, error) {
	h.RLock()
	doer, ok := h.doers[workerName]
	h.RUnlock()
	if !ok {
		return doer, "", errors.New("worm: doer not found")
	}

	jobID := uuid.NewV4().String()

	o := <-h.waitc
	_, err := h.Db.Exec(`
	INSERT INTO worm (id,worker_name,status,data,created_at)
	VALUES (?,?,?,?,?);
	`, jobID, workerName, StatusStart, data, time.Now().UTC())
	h.waitc <- o
	if err != nil {
		return doer, "", err
	}
	return doer, jobID, nil
}

// Queue will cron the job for execution on cronformat.
func (h *Worm) Queue(workerName string, data []byte) (string, error) {
	return h.Sched(workerName, data, nowCron(time.Now()))
}

// Sched will cron the job for execution on cronformat.
func (h *Worm) Sched(workerName string, data []byte, cronformat string) (string, error) {
	doer, jobID, err := h.store(workerName, data)
	if err != nil {
		return "", err
	}

	err = h.croner.AddFunc(cronformat, func() {

		// prepare log file.

		lName, lOut, err := newLog(h.logDir, doer.Name(), jobID)
		if err != nil {
			log.Printf("run job : err [%s]", err)
			return
		}
		defer func() {
			if err := lOut.Close(); err != nil {
				log.Printf("run : close log output file : err [%s]", err)
			}
		}()

		var errMsg string
		status, jobErr := doer.Run(data, lOut)
		if jobErr != nil {
			log.Printf("task fail: %s", jobErr)

			errMsg = fmt.Sprintf("%s", jobErr)
			Printf(lOut, "ERROR: %s", jobErr)
		}
		o := <-h.waitc
		_, err = h.Db.Exec(`
			UPDATE worm SET status=?,error=?,log_file=? WHERE id=?;
		`, status, errMsg, lName, jobID)
		h.waitc <- o
		if err != nil {
			log.Printf("Sched : update status : err [%s] job id [%s]", err, jobID)
		}
	})
	if err != nil {
		return "", err
	}
	return jobID, nil
}

// newLog generates a log output for job. Must be closed.
func newLog(dir string, workerName, jobID string) (string, *os.File, error) {
	fname := filepath.Clean(fmt.Sprintf("%s/%s_%s.log", dir, workerName, jobID))
	f, err := os.Create(fname)
	if err != nil {
		return fname, nil, err
	}
	return fname, f, nil
}

// Detail return the job detail by id.
func (h *Worm) Detail(ID string) (*Job, error) {
	var d Job
	o := <-h.waitc
	err := h.Db.Get(&d, `
		SELECT
			id,
			worker_name,
			status,
			IFNULL(error,'') AS "error",
			data,
			log_file,
			created_at
		FROM worm WHERE id=?;
	`, ID)
	h.waitc <- o
	if err != nil {
		log.Printf("job err [%s]", err)
		return nil, err
	}
	return &d, nil
}

// CopyLog return the job detail by id.
func (h *Worm) CopyLog(w io.Writer, jobID string) error {
	var name string

	o := <-h.waitc
	err := h.Db.Get(&name, `
		SELECT log_file FROM worm WHERE id=?;
	`, jobID)
	h.waitc <- o
	if err != nil {
		log.Printf("CopyLog : locate : err [%s]", err)
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("CopyLog : close file : err [%s]", err)
		}
	}()
	_, err = io.Copy(w, f)
	if err != nil {
		return err
	}
	return nil
}

// Close close database connections.
func (h *Worm) Close() error {
	return h.Db.Close()
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

// Detail _
func Detail(ID string) (*Job, error) {
	return defaultWorm.Detail(ID)
}

// Job struct for database query.
type Job struct {
	ID        string    `db:"id" json:"id"`
	Worker    string    `db:"worker_name" json:"worker_name"`
	Status    int       `db:"status" json:"status"`
	Error     string    `db:"error" json:"error"`
	LogFile   string    `db:"log_file" json:"log_file"`
	Data      string    `db:"data" json:"data"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

// Query _
func Query(before, after time.Time, limit int) ([]*Job, error) {
	db := DB()
	var jobs []*Job
	o := <-defaultWorm.waitc
	err := db.Select(&jobs, `
	SELECT
		id,
		worker_name,
		status,
		IFNULL(error,'') AS "error",
		log_file,
		data,
		created_at
	FROM worm
	WHERE created_at BETWEEN ? AND ? LIMIT ?;
	`, before.Format(time.RFC3339)[:10], after.Format(time.RFC3339)[:10], limit)
	defaultWorm.waitc <- o
	if err != nil {
		log.Printf("Query : retrieve : err [%s]", err)
	}
	return jobs, err
}

// DB returns the default worm Db. Use it only for queries more complicated than
// Query method.
func DB() *sqlx.DB {
	return defaultWorm.Db
}

// Close close database connections.
func Close() error {
	return defaultWorm.Close()
}

// CopyLog write the content of log file to w.
func CopyLog(w io.Writer, jobID string) error {
	return defaultWorm.CopyLog(w, jobID)
}

// Doer interface. Your implementation must make sure that
// resources are only available there and reusable. For example you 'almost'
// everytime will need only one database connection for resources economy.
type Doer interface {
	// Name returns worker name.
	Name() string

	// Run executes the job and Worm writes the logOut to a file and state to
	// database.
	// If success state returned must be zero.
	Run(data []byte, logOutput io.Writer) (state int, err error)
}

// nowCron return cron string with t date. Once execution.
func nowCron(t time.Time) string {
	t = t.Add(1 * time.Second)
	return fmt.Sprintf("%v %v %v %v %v *", t.Second(), t.Minute(), t.Hour(), t.Day(), int(t.Month()))
}

// Printf convenience.
func Printf(w io.Writer, format string, args ...interface{}) {
	fmt.Fprintf(w, format+"\n", args...)
}

// Println convenience.
func Println(w io.Writer, args ...interface{}) {
	fmt.Fprintln(w, args...)
}
