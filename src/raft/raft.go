package raft

// 14-736 Lab 2 Raft implementation in go

import (
	"fmt"
	"math/rand"
	"remote" // feel free to change to "remote" if appropriate for your dev environment
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Status -- this is a custom type that you can use to represent the current status of a Raft peer.
type Status int

const (
	FOLLOWER Status = iota
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

// TODO -- define any other structs or types you need to use to implement Raft
type logEntry struct {
	Term int
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
	RequestVote     func(term int, candidateId int, lastLogIndex int, lastLogTerm int) (int, bool, remote.RemoteObjectError)                               // TODO: define function type
	AppendEntries   func(term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []int, leaderCommit int) (int, bool, remote.RemoteObjectError) // TODO: define function type
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
	currentTerm int
	votedFor    int
	log         []int
	// volatile state
	commitIndex int
	lastApplied int
	votedCount  atomic.Uint32
	mu          *sync.Mutex
	// leader state
	nextIndex  []int
	matchIndex []int
	status     Status
	service    *remote.Service
	// peers
	peers           []*RaftInterface
	electionTimeout *time.Timer
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

	initLog()

	// create client stubs for all other peers

	sobj := &RaftPeer{
		id:          id,
		currentTerm: 0,
		votedFor:    -1,
		log:         []int{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   []int{},
		matchIndex:  []int{},
		service:     nil,
		mu:          &sync.Mutex{},
		status:      FOLLOWER,
		votedCount:  atomic.Uint32{},
		// vote:        make(chan bool, 1),
		peers:           []*RaftInterface{},
		electionTimeout: nil,
	}

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
	rf.status = FOLLOWER

	// 150-300ms timeout
	timer := rand.Intn(150) + 150
	rf.ResetElectionTimeout()

	// if is the follower, listen to appendEntries and requestVote

	go func() {
		for {
			rf.mu.Lock()
			if rf.status == FOLLOWER {
				// if is follower, listen to appendEntries and requestVote
				// fmt.Printf("peer %d is follower\n", rf.id)
			} else if rf.status == LEADER {
				// if is leader, send heartbeat to all other peers
				Debug(dInfo, "S%d heartbeet ->\n", rf.id)
				for _, p := range rf.peers {
					go func(p *RaftInterface) {
						p.AppendEntries(-1, rf.id, 0, 0, []int{}, 0)

						// if success {
						// 	// rf.mu.Lock()
						// 	// // rf.nextIndex[rf.id] = len(rf.log)
						// 	// // rf.matchIndex[rf.id] = len(rf.log)
						// 	// rf.mu.Unlock()
						// }

					}(p)
				}
				// sleep until next heartbeat
				time.Sleep(time.Duration(timer) * time.Millisecond / 3)
			}
			rf.mu.Unlock()
		}
	}()

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
	rf.status = DOWN
	rf.service.Stop()
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

func (rf *RaftPeer) RequestVote(term int, candidateId int, lastLogIndex int, lastLogTerm int) (int, bool, remote.RemoteObjectError) {
	// if term < currentTerm, reject

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term < rf.currentTerm || rf.status == CANDIDATE {
		return rf.currentTerm, false, remote.RemoteObjectError{}
	}
	fmt.Print(rf.id)
	fmt.Println(" vote for ", candidateId)
	rf.currentTerm = term
	rf.votedFor = candidateId
	rf.ResetElectionTimeout()
	return rf.currentTerm, true, remote.RemoteObjectError{}
}

func (rf *RaftPeer) AppendEntries(term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []int, leaderCommit int) (int, bool, remote.RemoteObjectError) {
	// if term == -1, it means the leader is sending heartbeat
	// to reset timer
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.newEntry <- logEntry{Term: term}
	if term > rf.currentTerm {
		rf.currentTerm = term
	}

	Debug(dInfo, "S%d heartbeet <-\n", rf.id)

	rf.ResetElectionTimeout()
	return 0, true, remote.RemoteObjectError{}
}

func (rf *RaftPeer) GetCommittedCmd(commitIndex int) (int, remote.RemoteObjectError) {
	return 0, remote.RemoteObjectError{}
}

func (rf *RaftPeer) GetStatus() (StatusReport, remote.RemoteObjectError) {
	status := StatusReport{
		Index:     rf.commitIndex,
		Term:      rf.currentTerm,
		Leader:    rf.status == LEADER,
		CallCount: rf.service.GetCount(),
	}
	return status, remote.RemoteObjectError{}
}

func (rf *RaftPeer) NewCommand(cmd int) (StatusReport, remote.RemoteObjectError) {
	return StatusReport{}, remote.RemoteObjectError{}
}

func (rf *RaftPeer) ResetElectionTimeout() {
	Debug(dTimer, "S%d reset timer\n", rf.id)
	timer := rand.Intn(150) + 150
	if rf.electionTimeout != nil {
		// 150-300ms timeout
		rf.electionTimeout.Reset(time.Duration(timer) * time.Millisecond)
	} else {
		rf.electionTimeout = time.AfterFunc(time.Duration(timer)*time.Millisecond, rf.startElection)
	}

}

func (rf *RaftPeer) startElection() {
	// reset timer
	// become candidate
	fmt.Print(rf.id)
	Debug(dTimer, "S%d timeout, become candidate\n", rf.id)
	rf.mu.Lock()
	rf.status = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.votedCount.Store(1)
	rf.mu.Unlock()

	// send requestVote to all other peers
	for _, peer := range rf.peers {
		go func(peer *RaftInterface) {
			// todo: implement requestVote
			term, accept, _ := peer.RequestVote(rf.currentTerm, rf.id, 0, 0)

			if accept {
				rf.votedCount.Add(1)
				if int(rf.votedCount.Load()) > (len(rf.peers)+1)/2 {
					Debug(dLeader, "S%d become leader\n", rf.id)
					rf.status = LEADER
					// timer stop
					Debug(dTimer, "S%d stop timer\n", rf.id)
					rf.electionTimeout.Stop()
					return
				}
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			if term > rf.currentTerm {
				rf.currentTerm = term
				rf.status = FOLLOWER
			}
			rf.mu.Unlock()
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
