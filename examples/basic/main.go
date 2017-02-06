// Package main contains example for worm server and queue.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/jimmy-go/srest"
	_ "github.com/mattn/go-sqlite3"

	worm "github.com/jimmy-go/worm.io"
)

var (
	port       = flag.Int("port", 8080, "Listen port.")
	connectURL = flag.String("db", "", "SQLite connection URL.")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile)

	err := worm.Connect(*connectURL)
	if err != nil {
		log.Fatal(err)
	}

	// register worker keeping just one connection.

	worm.MustRegister(SampleName, &Xample{})

	// start http server
	m := srest.New(nil)
	m.Post("/add", http.HandlerFunc(jobHandler))
	m.Post("/sched", http.HandlerFunc(schedHandler))
	m.Get("/job", http.HandlerFunc(statusHandler))
	<-m.Run(*port)
	worm.Close()
}

func jobHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		log.Printf("jobHandler : err [%s]", err)
	}

	// TODO; validate model.

	v := &JOB{
		URL: r.Form.Get("url"),
	}
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "can't marshal task", http.StatusInternalServerError)
		return
	}

	jobID, err := worm.Queue(SampleName, b)
	if err != nil {
		http.Error(w, "can't add task", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "job id: %s", jobID)
}

func schedHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		log.Printf("jobHandler : err [%s]", err)
	}

	// TODO; validate model.
	crons := r.Form.Get("cron")

	v := &JOB{
		URL: r.Form.Get("url"),
	}
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "can't marshal task", http.StatusInternalServerError)
		return
	}

	jobID, err := worm.Sched(SampleName, b, crons)
	if err != nil {
		http.Error(w, "can't add task", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "job id: %s", jobID)
}

// JOB struct.
type JOB struct {
	URL string `json:"url"`
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	log.Printf("status : id [%s]", id)

	i, err := worm.Status(id)
	if err != nil {
		http.Error(w, "can't retrieve job status", http.StatusInternalServerError)
		return
	}
	if err := srest.JSON(w, &ResStatus{ID: id, Status: i}); err != nil {
		log.Printf("statusHandler : render : err [%s]", err)
	}
}

// ResStatus struct.
type ResStatus struct {
	ID     string `json:"id"`
	Status int    `json:"status"`
}

// Xample type for sample Doer interface.
type Xample struct{}

const (
	// SampleName the Xample default name.
	SampleName = "sample_worker"
)

// Name implements worm.Doer.
func (x *Xample) Name() string {
	return SampleName
}

// Run implements worm.Doer.
func (x *Xample) Run(data []byte) (io.Reader, int, error) {
	l := bytes.NewBuffer([]byte{})

	var v *JOB
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Fprintf(l, "Run : unmarshal : err [%s]", err)
		return l, 1, errors.New("some error happend with the time second")
	}

	fmt.Fprintf(l, "Run : doing something")

	if time.Now().Second()%2 == 0 {
		fmt.Fprintf(l, "Run : some random error")
		return l, 2, errors.New("some error happend with the time second")
	}

	return l, worm.StatusOK, nil
}
