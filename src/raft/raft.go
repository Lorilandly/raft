package raft

// 14-736 Lab 2 Raft implementation in go

import (
	"encoding/gob"
	"math/rand"
	"remote" // feel free to change to "remote" if appropriate for your dev environment
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Status -- this is a custom type that you can use to represent the current status of a Raft peer.
const (
	FOLLOWER int32 = iota
	CANDIDATE
	LEADER
	DOWN
)

// StatusReport struct sent from Raft node to Controller in response to command and status requests.
// this is needed by the Controller, so do not change it. make sure you give it to the Controller
// when requested
type StatusReport struct {
	Index     int
	Term      int
	Leader    bool
	CallCount int
}

// RaftInterface -- this is the "service interface" that is implemented by each Raft peer using the
// remote library from Lab 1.  it supports five remote methods that you must define and implement.
// these methods are described as follows:
//
//  1. RequestVote -- this is one of the remote calls defined in the Raft paper, and it should be
//     supported as such.  you will need to include whatever argument types are needed per the Raft
//     algorithm, and you can package the return values however you like, as long as the last return
//     type is `remote.RemoteObjectError`, since that is required for the remote library use.
//
//  2. AppendEntries -- this is one of the remote calls defined in the Raft paper, and it should be
//     supported as such and defined in a similar manner to RequestVote above.
//
//  3. GetCommittedCmd -- this is a remote call that is used by the Controller in the test code. it
//     allows the Controller to check the value of a commmitted log entry at a given index. the
//     type of the function is given below, and it must be implemented as given, otherwise the test
//     code will not function correctly.  more detail about this method is available later in this
//     starter code file.
//
//  4. GetStatus -- this is a remote call that is used by the Controller to collect status information
//     about the Raft peer.  the struct type that it returns is defined above, and it must be implemented
//     as given, or the Controller and test code will not function correctly.  more detail below.
//
//  5. NewCommand -- this is a remote call that is used by the Controller to emulate submission of
//     a new command value by a Raft client.  upon receipt, it will initiate processing of the command
//     and reply back to the Controller with a StatusReport struct as defined above. it must be
//     implemented as given, or the test code will not function correctly.  more detail below
type RaftInterface struct {
	RequestVote     func(term uint64, candidateId int, lastLogIndex int, lastLogTerm uint64) (int, bool, remote.RemoteObjectError)
	AppendEntries   func(term uint64, leaderId int, prevLogIndex int, prevLogTerm uint64, entries []Entry, leaderCommit uint64) (int, bool, remote.RemoteObjectError) // TODO: define function type
	GetCommittedCmd func(int) (int, remote.RemoteObjectError)
	GetStatus       func() (StatusReport, remote.RemoteObjectError)
	NewCommand      func(int) (StatusReport, remote.RemoteObjectError)
}

// you will need to define a struct that contains the parameters/variables that define and
// explain the current status of each Raft peer.  it doesn't matter what you call this struct,
// and the test code doesn't really care what state it contains, so this part is up to you.
// TODO: define a struct to maintain the local state of a single Raft peer
type RaftPeer struct {
	// persistent state
	id          int
	currentTerm atomic.Uint64
	log         []Entry
	// volatile state on all
	commitIndex atomic.Uint64
	lastApplied atomic.Uint64
	// volatile state on leader
	nextIndex  []int
	matchIndex []int
	// states
	service         *remote.Service
	entryCh         chan Entry
	status          int32
	peers           []*RaftInterface
	electionTimeout *time.Timer
	mu              *sync.RWMutex
}

type Entry struct {
	Term uint64
	Cmd  int
}

// `NewRaftPeer` -- this method should create an instance of the above struct and return a pointer
// to it back to the Controller, which calls this method.  this allows the Controller to create,
// interact with, and control the configuration as needed.  this method takes three parameters:
// -- port: this is the service port number where this Raft peer will listen for incoming messages
// -- id: this is the ID (or index) of this Raft peer in the peer group, ranging from 0 to num-1
// -- num: this is the number of Raft peers in the peer group (num > id)
func NewRaftPeer(port int, id int, num int) *RaftPeer { // TODO: <---- change the return type
	// TODO: create a new raft peer and return a pointer to it

	// when a new raft peer is created, its initial state should be populated into the corresponding
	// struct entries, and its `remote.Service` and `remote.StubFactory` components should be created,
	// but the Service should not be started (the Controller will do that when ready).
	//
	// the `remote.Service` should be bound to port number `port`, as given in the input argument.
	// each `remote.StubFactory` will be used to interact with a different Raft peer, and different
	// port numbers are used for each Raft peer.  the Controller assigns these port numbers sequentially
	// starting from peer with `id = 0` and ending with `id = num-1`, so any peer who knows its own
	// `id`, `port`, and `num` can determine the port number used by any other peer.
	gob.Register([]Entry{})
	initLog()

	// create client stubs for all other peers

	sobj := &RaftPeer{
		id:          id,
		currentTerm: atomic.Uint64{},
		log:         []Entry{},
		commitIndex: atomic.Uint64{},
		lastApplied: atomic.Uint64{},
		nextIndex:   []int{},
		matchIndex:  []int{},
		service:     nil,
		mu:          &sync.RWMutex{},
		status:      FOLLOWER,
		entryCh:     make(chan Entry, 1),
		// vote:        make(chan bool, 1),
		peers:           []*RaftInterface{},
		electionTimeout: nil,
	}

	timer := rand.Intn(150) + 150
	sobj.electionTimeout = time.AfterFunc(time.Duration(timer)*time.Millisecond, sobj.startElection)

	for i := port - id; i < port-id+num; i++ {
		if i == port {
			continue
		}
		stubAddr := "localhost:" + strconv.Itoa(i)
		stub := &RaftInterface{}
		err := remote.StubFactory(stub, stubAddr, false, false)
		if err != nil {
			panic(err)
		}
		sobj.peers = append(sobj.peers, stub)
	}

	service, err := remote.NewService(&RaftInterface{}, sobj, port, false, false)
	if err != nil {
		panic(err)
	}

	sobj.service = service

	// create a new raft peer
	Debug(dInfo, "S%d created\n", id)

	return sobj
}

// `Activate` -- this method operates on your Raft peer struct and initiates functionality
// to allow the Raft peer to interact with others.  before the peer is activated, it can
// have internal algorithm state, but it cannot make remote calls using its stubs or receive
// remote calls using its underlying remote.Service interface.  in essence, when not activated,
// the Raft peer is "sleeping" from the perspective of any other Raft peer.
//
// this method is used exclusively by the Controller whenever it needs to "wake up" the Raft
// peer and allow it to start interacting with other Raft peers.  this is used to emulate
// connecting a new peer to the network or recovery of a previously failed peer.
//
// when this method is called, the Raft peer should do whatever is necessary to enable its
// remote.Service interface to support remote calls from other Raft peers as soon as the method
// returns (i.e., if it takes time for the remote.Service to start, this method should not
// return until that happens).  the method should not otherwise block the Controller, so it may
// be useful to spawn go routines from this method to handle the on-going operation of the Raft
// peer until the remote.Service stops.
//
// given an instance `rf` of your Raft peer struct, the Controller will call this method
// as `rf.Activate()`, so you should define this method accordingly. NOTE: this is _not_
// a remote call using the `remote.Service` interface of the Raft peer.  it uses direct
// method calls from the Controller, and is used purely for the purposes of the test code.
// you should not be using this method for any messaging between Raft peers.
//
// TODO: implement the `Activate` method
func (rf *RaftPeer) Activate() {
	Debug(dWarn, "S%d activated\n", rf.id)
	rf.service.Start()
	atomic.SwapInt32(&rf.status, FOLLOWER)

	rf.ResetElectionTimeout()
}

// `Deactivate` -- this method performs the "inverse" operation to `Activate`, namely to emulate
// disconnection / failure of the Raft peer.  when called, the Raft peer should effectively "go
// to sleep", meaning it should stop its underlying remote.Service interface, including shutting
// down the listening socket, causing any further remote calls to this Raft peer to fail due to
// connection error.  when deactivated, a Raft peer should not make or receive any remote calls,
// and any execution of the Raft protocol should effectively pause.  however, local state should
// be maintained, meaning if a Raft node was the LEADER when it was deactivated, it should still
// believe it is the leader when it reactivates.
//
// given an instance `rf` of your Raft peer struct, the Controller will call this method
// as `rf.Deactivate()`, so you should define this method accordingly. Similar notes / details
// apply here as with `Activate`
//
// TODO: implement the `Deactivate` method
func (rf *RaftPeer) Deactivate() {
	Debug(dWarn, "S%d deactivated\n", rf.id)
	atomic.SwapInt32(&rf.status, DOWN)
	rf.service.Stop()
	rf.electionTimeout.Stop()
}

func (rf *RaftPeer) LeaderThread() {
	rf.electionTimeout.Stop()
	// 150-300ms timeout
	timer := time.NewTimer(time.Duration(rand.Intn(50)+50) * time.Millisecond)

	entries := []Entry{}
	for atomic.LoadInt32(&rf.status) == LEADER {
		select {
		case <-timer.C:
			rf.sendAppendEntries(entries)
			entries = []Entry{}
			timer.Stop()
			timer.Reset(time.Duration(rand.Intn(50)+50) * time.Millisecond)
		case entry := <-rf.entryCh:
			entries = append(entries, entry)
		}
	}
	timer.Stop()
	Debug(dLeader, "S%d stop leader thread\n", rf.id)
}

func (rf *RaftPeer) sendAppendEntries(entries []Entry) {
	Debug(dInfo, "S%d sendAppendEntries\n", rf.id)
	rf.mu.Lock()
	if len(entries) > 0 {
		Debug(dLog, "S%d num entry %d\n", rf.id, len(rf.log))
	}
	rf.log = append(rf.log, entries...)
	rf.mu.Unlock()
	rf.commitIndex.Add(uint64(len(entries)))
	for _, p := range rf.peers {
		go func(p *RaftInterface) {
			_, _, err := p.AppendEntries(rf.currentTerm.Load(), rf.id, 0, 0, entries, rf.commitIndex.Load())
			if err != (remote.RemoteObjectError{}) {
				Debug(dError, "S%d sendAppendEntries error: %s\n", rf.id, err.Err)
			}
		}(p)
	}
}

// TODO: implement remote method calls from other Raft peers:
//
// # RequestVote -- as described in the Raft paper, called by other Raft peers
//
// # AppendEntries -- as described in the Raft paper, called by other Raft peers
//
// GetCommittedCmd -- called (only) by the Controller.  this method provides an input argument
// `index`.  if the Raft peer has a log entry at the given `index`, and that log entry has been
// committed (per the Raft algorithm), then the command stored in the log entry should be returned
// to the Controller.  otherwise, the Raft peer should return the value 0, which is not a valid
// command number and indicates that no committed log entry exists at that index
//
// GetStatus -- called (only) by the Controller.  this method takes no arguments and is essentially
// a "getter" for the state of the Raft peer, including the Raft peer's current term, current last
// log index, role in the Raft algorithm, and total number of remote calls handled since starting.
// the method returns a `StatusReport` struct as defined at the top of this file.
//
// NewCommand -- called (only) by the Controller.  this method emulates submission of a new command
// by a Raft client to this Raft peer, which should be handled and processed according to the rules
// of the Raft algorithm.  once handled, the Raft peer should return a `StatusReport` struct with
// the updated status after the new command was handled.
// service implementation for RaftInterface

func (rf *RaftPeer) RequestVote(term uint64, candidateId int, lastLogIndex int, lastLogTerm uint64) (int, bool, remote.RemoteObjectError) {
	// if term < currentTerm, reject
	var currentTerm uint64
	for {
		currentTerm = rf.currentTerm.Load()
		if term > currentTerm {
			if rf.currentTerm.CompareAndSwap(currentTerm, term) {
				break
			}
		} else {
			Debug(dDrop, "S%d requestVote term %d <= %d, ignore\n", rf.id, term, currentTerm)
			return int(currentTerm), false, remote.RemoteObjectError{
				Err: "term is less than current term",
			}
		}
	}
	Debug(dVote, "S%d vote for %d\n", rf.id, candidateId)
	atomic.SwapInt32(&rf.status, FOLLOWER)
	rf.ResetElectionTimeout()
	return int(currentTerm), true, remote.RemoteObjectError{}
}

func (rf *RaftPeer) AppendEntries(term uint64, leaderId int, prevLogIndex int, prevLogTerm uint64, entries []Entry, leaderCommit uint64) (int, bool, remote.RemoteObjectError) {
	// if term == -1, it means the leader is sending heartbeat
	// to reset timer
	var currentTerm uint64
	for {
		currentTerm = rf.currentTerm.Load()
		if term >= currentTerm {
			if rf.currentTerm.CompareAndSwap(currentTerm, term) {
				break
			}
		} else {
			Debug(dDrop, "S%d appendEntries term %d <= %d, ignore\n", rf.id, term, currentTerm)
			return int(rf.currentTerm.Load()), false, remote.RemoteObjectError{
				Err: "term is less than current term",
			}
		}
	}

	rf.mu.Lock()
	Debug(dInfo, "S%d heartbeet <-%d, %d\n", rf.id, len(rf.log), len(entries))
	rf.log = append(rf.log, entries...)
	rf.mu.Unlock()
	rf.commitIndex.Add(uint64(len(entries)))

	rf.ResetElectionTimeout()
	return int(currentTerm), true, remote.RemoteObjectError{}
}

func (rf *RaftPeer) GetCommittedCmd(commitIndex int) (int, remote.RemoteObjectError) {

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	Debug(dLog, "S%d get committed cmd %d, %d\n", rf.id, commitIndex, len(rf.log))
	if len(rf.log) != int(rf.commitIndex.Load()) {
		Debug(dWarn, "S%d inconsistent commitIndex %d, %d\n", rf.id, len(rf.log), rf.commitIndex.Load())
	}
	if commitIndex < int(rf.commitIndex.Load()) {
		return rf.log[commitIndex].Cmd, remote.RemoteObjectError{}
	}
	return -1, remote.RemoteObjectError{
		Err: "commitIndex is greater than log length",
	}
}

func (rf *RaftPeer) GetStatus() (StatusReport, remote.RemoteObjectError) {
	status := StatusReport{
		Index:     int(rf.commitIndex.Load()),
		Term:      int(rf.currentTerm.Load()),
		Leader:    atomic.LoadInt32(&rf.status) == LEADER,
		CallCount: rf.service.GetCount(),
	}
	return status, remote.RemoteObjectError{}
}

func (rf *RaftPeer) NewCommand(cmd int) (StatusReport, remote.RemoteObjectError) {
	Debug(dClient, "S%d new command %d\n", rf.id, cmd)

	if atomic.LoadInt32(&rf.status) != LEADER {
		return StatusReport{}, remote.RemoteObjectError{
			Err: "not leader",
		}
	}
	rf.entryCh <- Entry{
		Term: rf.currentTerm.Load(),
		Cmd:  cmd,
	}

	status := StatusReport{
		Index:     int(rf.commitIndex.Load()),
		Term:      int(rf.currentTerm.Load()),
		Leader:    atomic.LoadInt32(&rf.status) == LEADER,
		CallCount: rf.service.GetCount(),
	}

	return status, remote.RemoteObjectError{}
}

func (rf *RaftPeer) ResetElectionTimeout() {
	Debug(dTimer, "S%d reset timer\n", rf.id)
	timer := rand.Intn(150) + 150
	rf.electionTimeout.Reset(time.Duration(timer) * time.Millisecond)

}

func (rf *RaftPeer) startElection() {
	// reset timer
	// become candidate
	Debug(dTimer, "S%d timeout, become candidate\n", rf.id)
	rf.ResetElectionTimeout()
	atomic.SwapInt32(&rf.status, CANDIDATE)
	term := rf.currentTerm.Add(1)
	var votedCount atomic.Uint64
	votedCount.Store(1)

	// send requestVote to all other peers
	for _, peer := range rf.peers {
		go func(peer *RaftInterface) {
			// todo: implement requestVote
			_, accept, _ := peer.RequestVote(term, rf.id, 0, 0)

			if accept {
				votes := votedCount.Add(1)
				if int(votes) == (len(rf.peers)+1)/2+1 {
					// become leader
					Debug(dLeader, "S%d received more than half votes, become leader\n", rf.id)
					atomic.SwapInt32(&rf.status, LEADER)
					// timer stop
					Debug(dTimer, "S%d stop timer\n", rf.id)
					go rf.LeaderThread()
					return
				}
			}
		}(peer)
	}
}

// general notes:
//
// - you are welcome to use additional helper functions to handle aspects of the Raft algorithm logic
//   within the scope of a single Raft peer.  you should not need to create any additional remote
//   calls between Raft peers or the Controller.  if there is a desire to create additional remote
//   calls, please talk with the course staff before doing so.
//
// - please make sure to read the Raft paper (https://raft.github.io/raft.pdf) before attempting
//   any coding for this lab.  you will most likely need to refer to it many times during your
//   implementation and testing tasks, so please consult the paper for algorithm details.
//
// - each Raft peer will accept a lot of remote calls from other Raft peers and the Controller,
//   so use of locks / mutexes is essential.  you are expected to use locks correctly in order to
//   prevent race conditions in your implementation.  the Makefile supports testing both without
//   and with go's race detector, and the final auto-grader will enable the race detector, which will
//   cause tests to fail if any race conditions are encountered.
