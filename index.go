package main

import (
	"sync"
)

// JobIndex provides O(1) job lookup by ID.
type JobIndex struct {
	jobs map[uint64]*Job
	mu   sync.RWMutex
}

func NewJobIndex() *JobIndex {
	return &JobIndex{jobs: make(map[uint64]*Job)}
}

func (ji *JobIndex) Add(j *Job) {
	ji.mu.Lock()
	ji.jobs[j.ID] = j
	ji.mu.Unlock()
}

func (ji *JobIndex) Get(id uint64) *Job {
	ji.mu.RLock()
	j := ji.jobs[id]
	ji.mu.RUnlock()
	return j
}

func (ji *JobIndex) Remove(id uint64) {
	ji.mu.Lock()
	delete(ji.jobs, id)
	ji.mu.Unlock()
}
