package main

import (
	"flag"
	"strings"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// @Author Chen Zikang
// @Email  zikang.chen@shopee.com
// @Since  2022-08-20

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvPort := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var kvStore *kvStore
	getSnapshot := func() ([]byte, error) { return kvStore.getSnapshot() }
	commitC, errorC, snapshotterReady := NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvStore = NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)
	serveHttpKVAPI(kvStore, *kvPort, confChangeC, errorC)
}
