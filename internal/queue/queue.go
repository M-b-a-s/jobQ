package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("job not found")

type Job struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	Attempts  int             `json:"attempts"`
	RunAt     time.Time       `json:"run_at"`
	LockedAt  *time.Time      `json:"locked_at"`
	LastError *string         `json:"last_error"`
}

type Queue struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// insert a pending job and return the job id
func (q *Queue) Enqueue(ctx context.Context, jobType string, payload json.RawMessage) (int64, error) {
	var id int64
	err := q.pool.QueryRow(ctx,
		`INSERT INTO jobs (type, payload) VALUES ($1, $2) RETURNING id`,
		jobType, []byte(payload),
	).Scan(&id)
	return id, err
}

// return a job's current status
func (q *Queue) Get(ctx context.Context, id int64) (*Job, error) {
	var j Job
	err := q.pool.QueryRow(ctx,
		`SELECT id, type, payload, status, attempts, run_at, locked_at, last_error
		 FROM jobs WHERE id = $1`, id,
	).Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.RunAt, &j.LockedAt, &j.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

const claimSQL = `
UPDATE jobs
SET status = 'running', locked_at = now(), attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'pending' AND run_at <= now()
    ORDER BY run_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, type, payload, attempts`

// Claim atomically takes one runnable job. Returns (nil, nil) if there is none.
func (q *Queue) Claim(ctx context.Context) (*Job, error) {
	var j Job
	err := q.pool.QueryRow(ctx, claimSQL).Scan(&j.ID, &j.Type, &j.Payload, &j.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j.Status = "running"
	return &j, nil
}

func (q *Queue) Complete(ctx context.Context, id int64) error {
	_, err := q.pool.Exec(ctx,
		`UPDATE jobs SET status = 'done', finished_at = now() WHERE id = $1`, id)
	return err
}

func (q *Queue) Fail(ctx context.Context, id int64, msg string) error {
	_, err := q.pool.Exec(ctx,
		`UPDATE jobs SET status = 'failed', last_error = $2, finished_at = now() WHERE id = $1`,
		id, msg)
	return err
}
