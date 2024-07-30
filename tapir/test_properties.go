package main

import (
	"sync"
	"time"
)

// TestProperties server-side properties used for runtime
// artificial failures
type TestProperties struct {
	latency      time.Duration
	drop_ping    int
	drop_replica int
	drop_client  int
	lock         sync.Mutex
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

func (tp *TestProperties) AddDropPing(drop int) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.drop_ping += drop
}

func (tp *TestProperties) DecDropPing() bool {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	if tp.drop_ping > 0 {
		tp.drop_ping--
		return true
	}
	return false
}

func (tp *TestProperties) AddDropReplica(drop int) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.drop_replica += drop
}

func (tp *TestProperties) DecDropReplica() bool {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	if tp.drop_replica > 0 {
		tp.drop_replica--
		return true
	}
	return false
}

func (tp *TestProperties) AddDropClient(drop int) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.drop_client += drop
}

func (tp *TestProperties) DecDropClient() bool {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	if tp.drop_client > 0 {
		tp.drop_client--
		return true
	}
	return false
}
