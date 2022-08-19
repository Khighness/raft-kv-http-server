package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

// @Author Chen Zikang
// @Email  zikang.chen@shopee.com
// @Since  2022-08-16

type kv struct {
	Key string
	Val string
}

func NewKV(key string, val string) *kv {
	return &kv{
		Key: key,
		Val: val,
	}
}

type kvStore struct {
	proposeC    chan<- string
	mu          sync.RWMutex
	kvStore     map[string]string
	snapshotter *snap.Snapshotter
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvStore {
	kvStore := &kvStore{
		proposeC:    proposeC,
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
	}
	snapshot, err := kvStore.loadSnapshot()
	if err != nil {
		logger.Panicf("[store] %v", err)
	}
	if snapshot != nil {
		logger.WithField("term", snapshot.Metadata.Term).
			WithField("index", snapshot.Metadata.Index).
			Info("[store] loading snapshot")
		if err = kvStore.recoverFromSnapshot(snapshot.Data); err != nil {
			logger.Panicf("[store] %v", err)
		}
	}
	// read commits from raft into kvStore map until error
	go kvStore.readCommits(commitC, errorC)
	return kvStore
}

func (s *kvStore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.kvStore[key]
	return val, ok
}

func (s *kvStore) Propose(key string, val string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(NewKV(key, val)); err != nil {
		logger.Fatalf("[store] failed to encode kv by gob: %v", err)
	}
	s.proposeC <- buf.String()
}

func (s *kvStore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvStore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}

func (s *kvStore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		// If commit is nil, then this is the signal telling it to recover data from snapshot.
		// Attempt to load the last snapshot, if the snapshot exists, recover kvStore from the snapshot.
		if commit == nil {
			snapshot, err := s.loadSnapshot()
			if err != nil {
				logger.Panicf("[store] %v", err)
			}
			if snapshot != nil {
				logger.WithField("term", snapshot.Metadata.Term).
					WithField("index", snapshot.Metadata.Index).
					Info("[store] loading snapshot")
				if err = s.recoverFromSnapshot(snapshot.Data); err != nil {
					logger.Panicf("[store] %v", err)
				}
			}
			continue
		}

		// If commit is not empty, then this is the signal telling data proposed by raft.
		// Deserialize the KV from the bytes, lock to add or modify the KV in the kvStore.
		for _, data := range commit.data {
			var kv kv
			if err := gob.NewDecoder(bytes.NewBufferString(data)).Decode(&kv); err != nil {
				logger.Fatalf("[raft] failed to decode kv by gob: %v", err)
			}

			s.mu.Lock()
			s.kvStore[kv.Key] = kv.Val
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
}
