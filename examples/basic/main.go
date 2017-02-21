// Package main contains example for worm server and queue.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/jimmy-go/srest"

	worm "github.com/jimmy-go/worm.io"
)

var (
	port       = flag.Int("port", 8080, "Listen port.")
	connectURL = flag.String("db", "", "SQLite connection URL.")
	logDir     = flag.String("log-dir", "", "Log output directory.")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Lshortfile)

	err := worm.Connect(*connectURL, *logDir)
	if err != nil {
		log.Fatal(err)
	}

	// register worker keeping just one connection.

	worm.MustRegister(SampleName, &Xample{})

	// start http server
	m := srest.New(nil)
	m.Get("/list", http.HandlerFunc(listHandler))
	m.Get("/detail", http.HandlerFunc(detailHandler))
	m.Get("/log", http.HandlerFunc(logHandler))
	m.Post("/add", http.HandlerFunc(jobHandler))
	m.Post("/sched", http.HandlerFunc(schedHandler))
	<-m.Run(*port)
	if err := worm.Close(); err != nil {
		log.Printf("worm close : err [%s]", err)
	}
}

func listHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		log.Printf("listHandler : err [%s]", err)
	}

	before := time.Now().UTC()
	after := before.AddDate(0, 0, 1)

	list, err := worm.Query(before, after, 100)
	if err != nil {
		http.Error(w, "can't retrieve tasks", http.StatusInternalServerError)
		return
	}
	if err := srest.JSON(w, list); err != nil {
		log.Printf("list : json : err [%s]", err)
	}
}

func jobHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		log.Printf("jobHandler : err [%s]", err)
	}

	// TODO; validate model.

	v := &MyJob{
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
		log.Printf("schedHandler : err [%s]", err)
	}

	// TODO; validate model.
	crons := r.Form.Get("cron")

	v := &MyJob{
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

func detailHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	log.Printf("detail : id [%s]", id)

	jobDetail, err := worm.Detail(id)
	if err != nil {
		http.Error(w, "can't retrieve job status", http.StatusInternalServerError)
		return
	}
	if err := srest.JSON(w, jobDetail); err != nil {
		log.Printf("detailHandler : render : err [%s]", err)
	}
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	log.Printf("job : id [%s]", id)

	err := worm.CopyLog(w, id)
	if err != nil {
		log.Printf("logHandler : write log [%s]", err)
		http.Error(w, "can't retrieve job log", http.StatusInternalServerError)
		return
	}
}

// ResStatus struct.
type ResStatus struct {
	ID     string `json:"id"`
	Status int    `json:"status"`
}

// MyJob struct.
type MyJob struct {
	URL string `json:"url"`
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
func (x *Xample) Run(data []byte, w io.Writer) (int, error) {

	// do some work.

	var v *MyJob
	if err := json.Unmarshal(data, &v); err != nil {
		worm.Printf(w, "Run : unmarshal : err [%s]", err)
		return 1, errors.New("some error happend with the time second")
	}

	worm.Printf(w, "Run : doing something")
	worm.Printf(w, "Run : doing foo")

	if time.Now().Second()%2 == 0 {
		worm.Printf(w, "Run : some random error")
		return 2, errors.New("some error happend with the time second")
	}

	return worm.StatusOK, nil
}
