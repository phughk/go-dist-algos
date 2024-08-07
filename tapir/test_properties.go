package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// TestProperties server-side properties used for runtime
// artificial failures
type TestProperties struct {
	timeout          time.Duration
	latency          time.Duration
	viewChangePeriod time.Duration
	drop_ping        atomic.Int32
	drop_replica     atomic.Int32
	drop_client      atomic.Int32
	lock             sync.Mutex
}

func (tp *TestProperties) SetLatency(latency time.Duration) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.latency = latency
}

func (tp *TestProperties) GetLatency() time.Duration {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	return tp.latency
}

func (tp *TestProperties) SetTimeout(timeout time.Duration) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.timeout = timeout
}

func (tp *TestProperties) GetTimeout() time.Duration {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	return tp.timeout
}

func (tp *TestProperties) SetViewChangePeriod(viewChangePeriod time.Duration) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.viewChangePeriod = viewChangePeriod
}

func (tp *TestProperties) GetViewChangePeriod() time.Duration {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	return tp.viewChangePeriod
}

func (tp *TestProperties) AddDropPing(drop int) {
	tp.drop_ping.Add(int32(drop))
}

// DecDropPing true if a ping message should be dropped
func (tp *TestProperties) DecDropPing() bool {
	original := tp.drop_ping.Load()
	if original <= 0 {
		return false
	}
	newVal := tp.drop_ping.Add(-1)
	if newVal < 0 {
		// Fix the race condition
		tp.drop_ping.Add(1)
		return false
	}
	return true
}

func (tp *TestProperties) AddDropReplica(drop int) {
	tp.drop_replica.Add(int32(drop))
}

// DecDropReplica true if a replica message should be dropped
func (tp *TestProperties) DecDropReplica() bool {
	original := tp.drop_replica.Load()
	if original <= 0 {
		return false
	}
	newVal := tp.drop_replica.Add(-1)
	if newVal < 0 {
		// Fix the race condition
		tp.drop_replica.Add(1)
		return false
	}
	return true
}

func (tp *TestProperties) AddDropClient(drop int) {
	tp.drop_client.Add(int32(drop))
}

func (tp *TestProperties) DecDropClient() bool {
	original := tp.drop_client.Load()
	if original <= 0 {
		return false
	}
	newVal := tp.drop_client.Add(-1)
	if newVal < 0 {
		// Fix the race condition
		tp.drop_client.Add(1)
		return false
	}
	return true
}
