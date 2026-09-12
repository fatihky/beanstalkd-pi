package main

import "time"

// PersistedJob is the serializable representation of a job for persistence.
// It decouples the in-memory Job struct from storage format.
type PersistedJob struct {
	ID         uint64
	Pri        uint32
	Delay      time.Duration
	TTR        time.Duration
	BodySize   int
	CreatedAt  time.Time
	DeadlineAt time.Time
	ReserveCt  uint32
	TimeoutCt  uint32
	ReleaseCt  uint32
	BuryCt     uint32
	KickCt     uint32
	State      byte
	TubeName   string
	Body       []byte

	// DeadLetteredFrom mirrors Job.DeadLetteredFrom.
	DeadLetteredFrom string
}

// Persistence defines the port for job persistence operations.
// Adapters (WAL, SQLite, file-based, etc.) implement this interface.
type Persistence interface {
	// Init initializes the persistence backend. Called once at startup.
	Init() error

	// Close shuts down the persistence backend gracefully.
	Close() error

	// StoreJob persists a new or updated job.
	StoreJob(job *PersistedJob) error

	// DeleteJob removes a job from persistent storage.
	DeleteJob(id uint64) error

	// LoadAllJobs returns all persisted jobs. Used during recovery.
	LoadAllJobs() ([]*PersistedJob, error)

	// Sync flushes any buffered writes to durable storage.
	Sync() error
}

// ToPersistedJob converts an in-memory Job to its persisted form.
func ToPersistedJob(j *Job) *PersistedJob {
	tubeName := ""
	if j.Tube != nil {
		tubeName = j.Tube.Name
	}
	body := make([]byte, len(j.Body))
	copy(body, j.Body)

	return &PersistedJob{
		ID:               j.ID,
		Pri:              j.Pri,
		Delay:            j.Delay,
		TTR:              j.TTR,
		BodySize:         j.BodySize,
		CreatedAt:        j.CreatedAt,
		DeadlineAt:       j.DeadlineAt,
		ReserveCt:        j.ReserveCt,
		TimeoutCt:        j.TimeoutCt,
		ReleaseCt:        j.ReleaseCt,
		BuryCt:           j.BuryCt,
		KickCt:           j.KickCt,
		State:            j.State,
		TubeName:         tubeName,
		Body:             body,
		DeadLetteredFrom: j.DeadLetteredFrom,
	}
}

// fromPersistedJob converts a PersistedJob back to an in-memory Job.
// Tube is set to a placeholder; caller must assign the real *Tube.
func fromPersistedJob(pj *PersistedJob) *Job {
	body := make([]byte, len(pj.Body))
	copy(body, pj.Body)

	return &Job{
		ID:               pj.ID,
		Pri:              pj.Pri,
		Delay:            pj.Delay,
		TTR:              pj.TTR,
		BodySize:         pj.BodySize,
		CreatedAt:        pj.CreatedAt,
		DeadlineAt:       pj.DeadlineAt,
		ReserveCt:        pj.ReserveCt,
		TimeoutCt:        pj.TimeoutCt,
		ReleaseCt:        pj.ReleaseCt,
		BuryCt:           pj.BuryCt,
		KickCt:           pj.KickCt,
		State:            pj.State,
		Tube:             &Tube{Name: pj.TubeName},
		Body:             body,
		DeadLetteredFrom: pj.DeadLetteredFrom,
	}
}
