// Package main contains example for worm server and queue.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"

	"github.com/jimmy-go/srest"
	worm "github.com/jimmy-go/worm.io"
)

var (
	port = flag.Int("port", 8080, "Listen port.")
	whub *worm.Worm
)

const (
	workerName = "sample_worker"
)

func main() {
	flag.Parse()
	var err error
	whub, err = worm.New()
	if err != nil {
		log.Fatal(err)
	}

	// make temporal db for business logic example.

	db, err := prepareDB("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	// register worker keeping just one connection.

	myWorker := &Xample{
		Db: db,
	}
	whub.MustRegister(workerName, myWorker)

	// start http server
	m := srest.New(nil)
	m.Post("/add", http.HandlerFunc(jobHandler))
	m.Post("/sched", http.HandlerFunc(schedHandler))
	m.Get("/job/:id", http.HandlerFunc(statusHandler))
	<-m.Run(*port)
}

func prepareDB(driver, uri string) (*sqlx.DB, error) {
	db, err := sqlx.Connect(driver, uri)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE example (id integer not null primary key, name text, data text)`)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Todo struct.
type Todo struct {
	ID   string
	Data []byte
}

func jobHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	jname := r.Form.Get("name")
	id := r.Form.Get("id")
	data := r.Form.Get("json")

	toDo := &Todo{
		ID:   id,
		Data: []byte(data),
	}

	err := whub.Queue(jname, toDo)
	if err != nil {
		http.Error(w, "can't add task", http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, "OK")
}

func schedHandler(w http.ResponseWriter, r *http.Request) {

}

func statusHandler(w http.ResponseWriter, r *http.Request) {

}

// Item mock.
type Item struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Data string `db:"data"`
}

// Xample type for sample Doer interface.
type Xample struct {
	Db *sqlx.DB
}

// Run implements worm.Doer.
func (x *Xample) Run(status int, v interface{}) (int, error) {
	td, ok := v.(*Todo)
	if !ok {
		return status, errors.New("can't store job")
	}

	// inner logic allow take a job from a certain point.
	switch status {
	case 0:
		_, err := x.Db.Exec(`
			INSERT INTO example (name,data) VALUES (?,?);
		`, td.ID, td.Data)
		if err != nil {
			return status, err
		}
		status++
	case 1:
		b := []byte("data changed")
		_, err := x.Db.Exec(`
			UPDATE example SET data=? WHERE name=?;
		`, b, td.ID)
		if err != nil {
			return status, err
		}
		status++
	case 2: // this status means failure
		b := []byte("fail")
		_, err := x.Db.Exec(`
			UPDATE example SET data=? WHERE name=?;
		`, b, td.ID)
		if err != nil {
			return status, err
		}
		status++
	default:
		return status, errors.New("status not found")
	}
	status++
	return status, nil
}
