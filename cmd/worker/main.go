package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/M-b-a-s/jobQ/internal/queue"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler func(ctx context.Context, job *queue.Job) error

// demoHandler pretends to work, then records that it ran, so we can audit for duplicates.
func demoHandler(pool *pgxpool.Pool, workerID string) Handler {
	return func(ctx context.Context, j *queue.Job) error {
		time.Sleep(time.Duration(50+rand.Intn(250)) * time.Millisecond)
		_, err := pool.Exec(ctx,
			`INSERT INTO job_runs (job_id, worker_id) VALUES ($1, $2)`, j.ID, workerID)
		return err
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func process(q *queue.Queue, handlers map[string]Handler, job *queue.Job) {
	bg := context.Background()

	h, ok := handlers[job.Type]
	if !ok {
		if err := q.Fail(bg, job.ID, "no handler for type "+job.Type); err != nil {
			log.Printf("fail(%d): %v", job.ID, err)
		}
		return
	}
	if err := h(bg, job); err != nil {
		if ferr := q.Fail(bg, job.ID, err.Error()); ferr != nil {
			log.Printf("fail(%d): %v", job.ID, ferr)
		}
		return
	}
	if err := q.Complete(bg, job.ID); err != nil {
		log.Printf("complete(%d): %v", job.ID, err)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	q := queue.New(pool)
	host, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", host, os.Getpid())

	handlers := map[string]Handler{
		"demo": demoHandler(pool, workerID),
	}

	log.Printf("worker %s started", workerID)
	for ctx.Err() == nil {
		job, err := q.Claim(ctx)
		if err != nil {
			log.Printf("claim error: %v", err)
			sleep(ctx, time.Second)
			continue
		}
		if job == nil {
			sleep(ctx, 500*time.Millisecond) // queue empty: wait, then poll again
			continue
		}
		log.Printf("[%s] running job %d", workerID, job.ID)
		process(q, handlers, job)
	}
	log.Printf("worker %s stopped", workerID)
}
