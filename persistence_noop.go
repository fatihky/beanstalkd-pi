package main

// NoopPersistence is a no-op adapter that discards all persistence operations.
// Used as the default when no persistence backend is configured.
type NoopPersistence struct{}

func (n *NoopPersistence) Init() error                        { return nil }
func (n *NoopPersistence) Close() error                       { return nil }
func (n *NoopPersistence) StoreJob(_ *PersistedJob) error     { return nil }
func (n *NoopPersistence) DeleteJob(_ uint64) error           { return nil }
func (n *NoopPersistence) LoadAllJobs() ([]*PersistedJob, error) { return nil, nil }
func (n *NoopPersistence) Sync() error                        { return nil }
