package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// @Author Chen Zikang
// @Email  parakovo@gmail.com
// @Since  2022-08-16

type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPut:
		val, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorf("[http] failed to read on PUT: %v", err)
			http.Error(w, "Failed to PUT", http.StatusBadRequest)
			return
		}

		h.store.Propose(key, string(val))
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if val, ok := h.store.Lookup(key); ok {
			w.Write([]byte(val))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case http.MethodPost:
		url, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorf("[http] failed to read on POST: %v", err)
			http.Error(w, "Failed to POST", http.StatusBadRequest)
			return
		}

		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			logger.Errorf("[http] failed to convert ID for conf change: %v", err)
			http.Error(w, "Failed to POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeId,
			Context: url,
		}
		h.confChangeC <- cc
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			logger.Errorf("[http] failed to convert ID for conf change: %v", err)
			http.Error(w, "Failed to POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeC <- cc
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Add("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func serveHttpKVAPI(kvStore *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	addr := fmt.Sprintf("127.0.0.1:%v", port)
	httpServer := http.Server{
		Addr: addr,
		Handler: &httpKVAPI{
			store:       kvStore,
			confChangeC: confChangeC,
		},
	}
	go func() {
		logger.Infof("[http] server is listening at %s", addr)
		if err := httpServer.ListenAndServe(); err != nil {
			logger.Fatalln(err)
		}
	}()
	if err, ok := <-errorC; ok {
		logger.Fatalln(err)
	}
}
