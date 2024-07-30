package main

import (
	"fmt"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"log"
	"sync"
	"time"
)

// DATA_BUCKET Stores client data
var DATA_BUCKET = []byte("client_bucket")

// SYSTEM_BUCKET Replicas add inconsistent operations to their record
// as TENTATIVE and then mark them as FINALIZED once they execute.
// consensus operations are first marked TENTATIVE with the result of
// locally executing the operation, then FINALIZED once the record
// has the consensus result.
var SYSTEM_BUCKET = []byte("system_bucket")

// MASTER_RECORD_BUCKET On synchronization, a single IR node first
// upcalls into the application protocol with Merge, which takes
// records from inconsistent replicas and merges them into a master
// record of successful operations and consensus results. Then, IR
// upcalls into the application protocol with Sync at each replica.
// Sync takes the master record and reconciles application protocol
// state to make the replica consistent with the chosen consensus results.
//
// Replicas add inconsistent operations to their record as TENTATIVE
// and then mark them as FINALIZED once they execute. consensus operations
// are first marked TENTATIVE with the result of locally executing
// the operation, then FINALIZED once the record has the consensus result.
//
// The leader in a view decides the master record that replicas replicate
// from each other.
var MASTER_RECORD_BUCKET = []byte("master_record_bucket")

type StorageEngine struct {
	db    *bbolt.DB
	txMap map[*ClientTxRef]*ClientTx
	txMux sync.Mutex
}

type ClientTxRef struct {
	ClientID      string
	TransactionID string
}

type ClientTx struct {
	ReadSet  []string
	WriteSet map[string][]byte
	Tx       *bbolt.Tx
}

func NewStorageEngine(filepath string) (*StorageEngine, error) {
	db, err := bbolt.Open(filepath, 0600, &bbolt.Options{
		// Timeout for opening the database in case another process has a lock
		Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("error opening database: %e", err)
	}
	// Create the default bucket
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(DATA_BUCKET)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("error creating bucket: %e", err)
	}
	return &StorageEngine{db: db}, nil
}

func (s *StorageEngine) StartTransaction(clientID string) (*ClientTxRef, error) {
	txId := uuid.New().String()
	tx_ref := &ClientTxRef{ClientID: clientID, TransactionID: txId}
	s.txMux.Lock()
	defer s.txMux.Unlock()
	_, exists := s.txMap[tx_ref]
	if exists {
		return nil, fmt.Errorf("client %s already has an active transaction", clientID)
	}
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %e", err)
	}
	real_tx := &ClientTx{
		Tx: tx,
	}
	s.txMap[tx_ref] = real_tx
	return tx_ref, nil
}

func (s *StorageEngine) CommitTransaction(tx_ref *ClientTxRef) error {
	s.txMux.Lock()
	defer s.txMux.Unlock()
	tx, exists := s.txMap[tx_ref]
	if !exists {
		return fmt.Errorf("client %s does not have an active transaction", tx_ref.ClientID)
	}
	err := tx.Tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %e", err)
	}
	delete(s.txMap, tx_ref)
	return nil
}

func (s *StorageEngine) RollbackTransaction(tx_ref *ClientTxRef) error {
	s.txMux.Lock()
	defer s.txMux.Unlock()
	tx, exists := s.txMap[tx_ref]
	if !exists {
		return fmt.Errorf("client %s does not have an active transaction", tx_ref.ClientID)
	}
	err := tx.Tx.Rollback()
	if err != nil {
		return fmt.Errorf("error rolling back transaction: %e", err)
	}
	delete(s.txMap, tx_ref)
	return nil
}

func (s *StorageEngine) Get(tx_ref *ClientTxRef, key []byte) ([]byte, error) {
	s.txMux.Lock()
	defer s.txMux.Unlock()
	tx, exists := s.txMap[tx_ref]
	if !exists {
		return nil, fmt.Errorf("client %s does not have an active transaction", tx_ref.ClientID)
	}
	bucket := tx.Tx.Bucket(DATA_BUCKET)
	if bucket == nil {
		return nil, fmt.Errorf("bucket does not exist")
	}
	tx.ReadSet = append(tx.ReadSet, string(key))
	value := bucket.Get(key)
	return value, nil
}

func (s *StorageEngine) Put(tx_ref *ClientTxRef, key []byte, value []byte) error {
	s.txMux.Lock()
	defer s.txMux.Unlock()
	tx, exists := s.txMap[tx_ref]
	if !exists {
		return fmt.Errorf("client %s does not have an active transaction", tx_ref.ClientID)
	}
	bucket := tx.Tx.Bucket(DATA_BUCKET)
	if bucket == nil {
		return fmt.Errorf("bucket does not exist")
	}
	err := bucket.Put(key, []byte("new_value"))
	if err == nil {
		tx.WriteSet[string(key)] = value
	}
	return err
}

func (s *StorageEngine) Close() {
	err := s.db.Close()
	if err != nil {
		log.Panicf("Error closing database: %e", err)
	}
}
