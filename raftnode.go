package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

// @Author Chen Zikang
// @Email  zikang.chen@shopee.com
// @Since  2022-08-16

type commit struct {
	data       []string
	applyDoneC chan<- struct{}
}

type raftNode struct {
	proposeC    <-chan string            // proposed messages (k, v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *commit           // entries committed to log (k, v)
	errorC      chan<- error             // errors from raft session

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	walDir      string   // path to WAL directory
	snapDir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter

	snapCount uint64
	transport *rafthttp.Transport
	stopC     chan struct{}
	httpStopC chan struct{}
	httpDoneC chan struct{}

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error),
	proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) (<-chan *commit, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *commit)
	errorC := make(chan error)

	node := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		walDir:      fmt.Sprintf("raft-kv-svc-%d", id),
		snapDir:     fmt.Sprintf("raft-kv-svc-snaoshot%d", id),
		getSnapshot: getSnapshot,
		confState:   raftpb.ConfState{},

		snapshotterReady: make(chan *snap.Snapshotter, 1),

		snapCount: defaultSnapshotCount,
		stopC:     make(chan struct{}),
		httpStopC: make(chan struct{}),
		httpDoneC: make(chan struct{}),
		logger:    zap.NewExample(),
	}

	go node.startRaft()
	return commitC, errorC, node.snapshotterReady
}

func (r *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	if err := r.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := r.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return r.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (r *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > r.appliedIndex+1 {
		logger.Fatalf("[raft] fisrt index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, r.appliedIndex)
	}
	if r.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[r.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and
// returns all entries could be published.
func (r *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			r.confState = *r.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					r.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
					logger.Infof("[raft] node <%d> is added to the cluster", r.id)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(r.id) {
					logger.Infof("[raft] node <%d> is removed from the cluster. shutting down.", r.id)
					return nil, false
				}
				r.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case r.commitC <- &commit{data, applyDoneC}:
		case <-r.stopC:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	r.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

func (r *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(r.walDir) {
		walSnaps, err := wal.ValidSnapshotEntries(r.logger, r.walDir)
		if err != nil {
			logger.Fatalf("[raft] error listening snapshots: %v", err)
		}
		snapshot, err := r.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			logger.Fatalf("[raft] error loading snapshit: %v", err)
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (r *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(r.walDir) {
		err := os.Mkdir(r.walDir, 0750)
		if err != nil {
			logger.Fatalf("[raft] cannot create dir for wal: %v", err)
		}
		w, err := wal.Create(zap.NewExample(), r.walDir, nil)
		if err != nil {
			logger.Fatalf("[raft] create dir for error: %v", err)
		}
		w.Close()
	}

	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	logger.Infof("[raft] loading WAL at term %d and index %d", walSnap.Term, walSnap.Index)
	w, err := wal.Open(zap.NewExample(), r.walDir, walSnap)
	if err != nil {
		logger.Fatalf("[raft] error loading wal: %v", err)
	}
	return w
}

// replayWAL replays WAL entries into the raft instance.
func (r *raftNode) replayWAL() *wal.WAL {
	logger.Infof("[raft] replaying WAL of member %d", r.id)
	snapshot := r.loadSnapshot()
	w := r.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		logger.Fatalf("[raft] failed to read WAL: %v", err)
	}
	r.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		r.raftStorage.ApplySnapshot(*snapshot)
	}
	r.raftStorage.SetHardState(st)

	// append to storage so raft starts st the right place in log
	r.raftStorage.Append(ents)

	return w
}

func (r *raftNode) writeError(err error) {
	r.stopHTTP()
	close(r.commitC)
	r.errorC <- err
	close(r.errorC)
	r.node.Stop()
}

func (r *raftNode) startRaft() {
	if !fileutil.Exist(r.snapDir) {
		if err := os.Mkdir(r.snapDir, 0750); err != nil {
			logger.Fatalf("[raft] failed to create dir for dnapshot: %v", err)
		}
	}
	r.snapshotter = snap.New(zap.NewExample(), r.snapDir)

	oldWal := wal.Exist(r.walDir)
	r.wal = r.replayWAL()

	// signal replay has finished
	r.snapshotterReady <- r.snapshotter

	rpeers := make([]raft.Peer, len(r.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(r.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   r.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxUncommittedEntriesSize: 256,
		MaxInflightMsgs:           1 << 30,
	}

	if oldWal || r.join {
		r.node = raft.RestartNode(c)
	} else {
		r.node = raft.StartNode(c, rpeers)
	}

	r.transport = &rafthttp.Transport{
		Logger:      r.logger,
		TLSInfo:     transport.TLSInfo{},
		ID:          types.ID(r.id),
		ClusterID:   0x1000,
		Raft:        r,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(r.id)),
		ErrorC:      make(chan error),
	}

	r.transport.Start()
	for i := range r.peers {
		if i+1 != r.id {
			r.transport.AddPeer(types.ID(i+1), []string{r.peers[i]})
		}
	}

	go r.serveRaft()
	go r.serveChannels()
}

func (r *raftNode) stop() {
	r.stopHTTP()
	close(r.commitC)
	close(r.errorC)
	r.node.Stop()
}

func (r *raftNode) stopHTTP() {
	r.transport.Stop()
	close(r.httpStopC)
	<-r.httpStopC
}

func (r *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	logger.Infof("[raft] publishing snapshot at index %d", r.snapshotIndex)
	defer logger.Infof("[raft] snapshot index [%d] shuold > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, r.appliedIndex)
	r.commitC <- nil

	r.confState = snapshotToSave.Metadata.ConfState
	r.snapshotIndex = snapshotToSave.Metadata.Index
	r.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatupUpEntriesN uint64 = 10000

func (r *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	if r.appliedIndex-r.snapshotIndex <= r.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-r.stopC:
			return
		}
	}

	logger.Printf("[raft] start snapshot [applied index: %v, last snapshot index: %d]", r.appliedIndex, r.snapshotIndex)
	data, err := r.getSnapshot()
	if err != nil {
		logger.Panic(err)
	}
	snapshot, err := r.raftStorage.CreateSnapshot(r.appliedIndex, &r.confState, data)
	if err != nil {
		logger.Panic(err)
	}
	if err = r.saveSnap(snapshot); err != nil {
		logger.Panic(err)
	}

	compactIndex := uint64(1)
	if r.appliedIndex > snapshotCatupUpEntriesN {
		compactIndex = r.appliedIndex - snapshotCatupUpEntriesN
	}
	if err = r.raftStorage.Compact(compactIndex); err != nil {
		logger.Panic(err)
	}

	logger.Infof("compacted log at index %d", compactIndex)
	r.snapshotIndex = r.appliedIndex
}

func (r *raftNode) serveChannels() {
	snapshot, err := r.raftStorage.Snapshot()
	if err != nil {
		logger.Panic(err)
	}
	r.confState = snapshot.Metadata.ConfState
	r.snapshotIndex = snapshot.Metadata.Index
	r.appliedIndex = snapshot.Metadata.Index

	defer r.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for r.proposeC != nil && r.confChangeC != nil {
			select {
			case prop, ok := <-r.proposeC:
				if !ok {
					r.proposeC = nil
				} else {
					r.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-r.confChangeC:
				if !ok {
					r.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					r.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not ready
		close(r.stopC)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			r.node.Tick()

			// store raft entries to wal, then publish over commit channel
		case rd := <-r.node.Ready():
			// must save the snapshot file and WAL snapshot entry before saving any other entries
			// or hard state to ensure that recovery after a snapshot restore is possible.

			if !raft.IsEmptySnap(rd.Snapshot) {
				r.saveSnap(rd.Snapshot)
			}
			r.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				r.raftStorage.ApplySnapshot(rd.Snapshot)
				r.publishSnapshot(rd.Snapshot)
			}
			r.raftStorage.Append(rd.Entries)
			r.transport.Send(r.processMessages(rd.Messages))
			applyDoneC, ok := r.publishEntries(r.entriesToApply(rd.CommittedEntries))
			if !ok {
				r.stop()
				return
			}
			r.maybeTriggerSnapshot(applyDoneC)
			r.node.Advance()

		case err = <-r.transport.ErrorC:
			r.writeError(err)
			return

		case <-r.stopC:
			r.stop()
			return
		}
	}
}

// When there is a `raftpb.EntryConfChange` after creating the snapshot,
// then the confState included in the snapshot is out of date, so we need
// to update the confState before sending a snapshot to a follower.
func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = r.confState
		}
	}
	return ms
}

func (r *raftNode) serveRaft() {
	url, err := url.Parse(r.peers[r.id-1])
	if err != nil {
		logger.Fatalf("[raft] failed to parse url: %v", err)
	}

	listener, err := newStoppableListener(url.Host, r.httpStopC)
	if err != nil {
		logger.Fatalf("[raft] failed ti listen rafthttp: %v", err)
	}

	logger.Infof("[raft] http is listeing at %s", url.Host)
	err = (&http.Server{Handler: r.transport.Handler()}).Serve(listener)
	select {
	case <-r.httpStopC:
	default:
		logger.Fatalf("[raft] failed to serve rafthttp: %v", err)
	}
	close(r.httpStopC)
}

func (r *raftNode) Process(ctx context.Context, m raftpb.Message) error { return r.node.Step(ctx, m) }
func (r *raftNode) IsIDRemoved(id uint64) bool                          { return false }
func (r *raftNode) ReportUnreachable(id uint64)                         { r.node.ReportUnreachable(id) }
func (r *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	r.node.ReportSnapshot(id, status)
}
