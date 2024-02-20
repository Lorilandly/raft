package raft

// Raft test suite, using Controller to mimic client interaction
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.

import (
    "../remote"     // NOTE: you **can** change this line as needed for your dev environment
    "fmt"
    "math/rand"
    "strconv"
    "sync"
    "testing"
    "time"
)

const (
    ELECTION_TIMEOUT time.Duration = time.Second
)

type Controller struct {
    mu          sync.Mutex      // mutex control for Controller
    ch          []chan int      // channels to tell Raft peers to stop/start
    t           *testing.T      // allow Controller to affect test
    numPeers    int             // number of Raft peers created
    basePort    int             // starting service port number, +1 for each peer
    peers       []*RaftInterface // stubs for communicating with Raft peers
}

// create a Controller to facilitate tests
func NewController(t *testing.T, num int, prt int) *Controller {
    ctrl := &Controller{}
    ctrl.t = t
    ctrl.numPeers = num
    ctrl.basePort = prt
    ctrl.peers = make([]*RaftInterface, num)
    ctrl.ch = make([]chan int, num)

    for i:=0; i<num; i++ {
        ctrl.ch[i] = make(chan int)
        portI := ctrl.basePort + i
        go RaftControl(portI, i, num, ctrl.ch[i])
        
        ctrl.peers[i] = &RaftInterface{}
        errI := remote.StubFactory(ctrl.peers[i], "localhost:" + strconv.Itoa(portI), false, false)
        if errI != nil {
            ctrl.cleanup()
            ctrl.t.Fatalf("Cannot create Controller stub for Raft peer: " + errI.Error())
        }
    }
    
    // wait for raft peers to get themselves going
    time.Sleep(5 * time.Second)
    return ctrl
}

func RaftControl(prt int, ix int, num int, ch chan int) {
    rf := NewRaftPeer(prt, ix, num)
    active := true
    alive := true

    for alive {
        rf.Activate()
        for alive && active {
            select {
                case i := <-ch:
                    if i == 0 {
                        alive = false
                    } else if i == -1 {
                        active = false
                    }
                
                default:
                    time.Sleep(20 * time.Millisecond)
            }
        }
        rf.Deactivate()
        for alive && !active {
            select {
                case i := <-ch:
                    if i == 0 {
                        alive = false
                    } else if i == 1 {
                        active = true
                    }
                default:
                    time.Sleep(20 * time.Millisecond)
            }
        }
    }
}

// disconnect a Raft peer by stopping its underlying Service
func (ctrl *Controller) disconnect(i int) {
    ctrl.ch[i] <- -1
    time.Sleep(100 * time.Millisecond)
}

// reconnect a disconnected Raft peer by restarting its underlying Service
func (ctrl *Controller) connect(i int) {
    ctrl.ch[i] <- 1
    time.Sleep(100 * time.Millisecond)
}

// count how many Raft peers have committed the same command at index idx
func (ctrl *Controller) committedLogIndex(idx int) (int, int) {
    var count int = 0
    var cmd int = -1
    
    for i:=0; i<ctrl.numPeers; i++ {
        logcmd, roe := ctrl.peers[i].GetCommittedCmd(idx)
        if roe.Error() != "" {
            continue
        }
        if logcmd > 0 {
            cmdi := logcmd
            if count > 0 && cmd != cmdi {
                ctrl.cleanup()
                ctrl.t.Fatalf("Committed values do not match: index %d %d %d", idx, cmd, cmdi)
            }
            count++
            cmd = cmdi
        }
    }
    return count, cmd
}

// check that there is exactly one leader right now
func (ctrl *Controller) checkOneLeader() int {
    lastTermWithLeader := -1
    
    for iter:=0; iter<10; iter++ {
        time.Sleep(500 * time.Millisecond)
        
        leaders := make(map[int][]int)
        for i:=0; i<ctrl.numPeers; i++ {
            sr, roe := ctrl.peers[i].GetStatus()
            if roe.Error() != "" {
                continue
            }
            if sr.Leader {
                leaders[sr.Term] = append(leaders[sr.Term], i)
            }
        }
        lastTermWithLeader = -1
        for trm, leads := range leaders {
            if len(leads) > 1 {
                ctrl.cleanup()
                ctrl.t.Fatalf("term %d has %d (>1) leaders", trm, len(leads))
            }
            if trm > lastTermWithLeader {
                lastTermWithLeader = trm
            }
        }
        
        if lastTermWithLeader != -1 {
            return leaders[lastTermWithLeader][0]
        }
    }
    ctrl.cleanup()
    ctrl.t.Fatalf("no leader found!")
    return -1
}

// check that everyone agrees on the term
func (ctrl *Controller) checkTerms() int {
    term := -1
    for i:=0; i<ctrl.numPeers; i++ {
        sr, roe := ctrl.peers[i].GetStatus()
        if roe.Error() != "" {
            continue
        }
        if sr.Term > 0 {
            if term == -1 {
                term = sr.Term
            } else if term != sr.Term {
                ctrl.cleanup()
                ctrl.t.Fatalf("Peers disagree about term!")
            }
        }
    }
    return term
}

// check that there is no leader
func (ctrl *Controller) checkNoLeader() {
    time.Sleep(500 * time.Millisecond)
    for i:=0; i<ctrl.numPeers; i++ {
        sr, roe := ctrl.peers[i].GetStatus()
        if roe.Error() != "" {
            continue
        }
        if sr.Leader {
            ctrl.cleanup()
            ctrl.t.Fatalf("Fatal: Expected no leader, but %d claims to be leader", i)
        }
    }
}

// issue a new command to a single peer
//  - idx -- index of peer getting command
//  - cmd -- the command
func (ctrl *Controller) issueCommand(idx int, cmd int) StatusReport {
    sr, roe := ctrl.peers[idx].NewCommand(cmd)
    if roe.Error() != "" {
        return StatusReport{}
    }
    return sr
}

// run through a full commit. it might choose the wrong leader
// initially and start over if so, entirely giving up after
// about 10 seconds. indirectly checks peer agreement, since
// committedLogIndex checks this.
//  - cmd -- command being logged
//  - expectedPeers -- #peers that should commit
// returns commit index.
func (ctrl *Controller) startCommit(cmd int, expectedPeers int) int {
    t0 := time.Now()
    
    for time.Since(t0).Seconds() < 20 {
        // find index of leader, if there is one
        index := -1
        for i:=0; i<ctrl.numPeers; i++ {
            sr := ctrl.issueCommand(i, cmd)
            if sr.Index > 0 && sr.Leader {
                index = sr.Index
                break
            }
        }
        // if we found a leader, keep going to see how they handled the command
        if index != -1 {
            t1 := time.Now()
            // wait for a bit before giving up
            for time.Since(t1).Seconds() < 3 {
                ct, cd := ctrl.committedLogIndex(index)
                if cd == cmd && ct >= expectedPeers {
                    // this is exactly what we wanted!
                    return index
                }
                time.Sleep(200 * time.Millisecond)
            }
        } else {
            time.Sleep(250 * time.Millisecond)
        }
    }
    ctrl.cleanup()
    ctrl.t.Fatalf("startCommit(%d) failed to reach agreement", cmd)
    return -1
}

// wait for at least n Raft peers to commit index starting
// from term startTerm, but don't wait forever.
func (ctrl *Controller) wait(index int, n int, startTerm int) int {
    t0 := 10
    for iter:=0; iter<30; iter++ {
        ct, _ := ctrl.committedLogIndex(index)
        if ct >= n {
            break
        }
        time.Sleep(time.Duration(t0) * time.Millisecond)
        if t0 < 1000 {
            t0 *= 2
        }
        if startTerm > -1 {
            for i:=0; i<ctrl.numPeers; i++ {
                sr, roe := ctrl.peers[i].GetStatus()
                if roe.Error() != "" {
                    continue
                }
                if sr.Term > startTerm {
                    // someone moved on, can't guarantee we'll "win"
                    return -1
                }
            }
        }
    }

    ct, cd := ctrl.committedLogIndex(index)
    if ct < n {
        ctrl.cleanup()
        ctrl.t.Fatalf("only %d decided for index %d; wanted %d", ct, index, n)
    }
    return cd
}

// send all Raft peers the command to stop running
func (ctrl *Controller) cleanup() {
    for i:=0; i<ctrl.numPeers; i++ {
        ctrl.ch[i] <- 0
        time.Sleep(50 * time.Millisecond)
    } 
}

// helper function that returns the CallCount entry from a remotely fetched status
func (ctrl *Controller) getCallCount(idx int) int {
    sr, roe := ctrl.peers[idx].GetStatus()
    if roe.Error() != "" {
        return 0
    } else {
        return sr.CallCount
    }
}

// ---------------- test cases begin here ---------------- //

// test initial setup and use of remote interfaces
func TestCheckpoint_Setup(t *testing.T) {
    numPeers := 1
    port := 7000 + rand.Intn(10000)
    
    fmt.Print("Checking controller creation ... ")
    ctrl := NewController(t, numPeers, port)
    fmt.Println("ok")

    fmt.Print("Checking controller can get peer status ... ")
    _, roe := ctrl.peers[0].GetStatus()
    if roe.Error() != "" {
        ctrl.cleanup()
        t.Fatalf("remote error getting status from Raft peer")
    }
    fmt.Println("ok")

    ctrl.cleanup()
}




// test initial election process for new Raft peers, specifically:
// -- is a leader elected?
// -- if no failure, does leadership change?
// -- after multiple timeout durations, is there still a leader?
func TestCheckpoint_InitialElection(t *testing.T) {
    numPeers := 3
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    // is a leader elected?
    fmt.Print("Checking for leader ... ")
    ctrl.checkOneLeader()
    fmt.Println("ok")
    
    fmt.Print("Checking for term agreement ... ")
    term1 := ctrl.checkTerms()
    if term1 < 1 {
        ctrl.cleanup()
        t.Fatalf("term is %d, but should be at least 1", term1)
    }
    fmt.Println("ok")
    
    // does the leader+term stay the same if there is no network failure?
    time.Sleep(2 * ELECTION_TIMEOUT)
    term2 := ctrl.checkTerms()
    if term1 != term2 {
        fmt.Println("warning: term changed with no failures")
    }
    
    // there should still be a leader.
    fmt.Print("Checking there's still a leader ... ")
    ctrl.checkOneLeader()
    fmt.Println("ok")
    
    ctrl.cleanup()
}


// test re-election process when failure is involved, specifically:
// -- is a leader elected?
// -- if it disconnects, is a new one elected?
// -- if the old leader rejoins, does it affect the new leader?
// -- if > 1/2 peers disconnect, is there no leader?
// -- if one reconnects, is there a leader?
// -- if another reconnects, is there still a leader?
func TestCheckpoint_ReElection(t *testing.T) {
    numPeers := 3
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    fmt.Print("Checking for leader ... ")
    leader1 := ctrl.checkOneLeader()
    fmt.Println("ok")

    // if the leader disconnects, a new one should be elected.
    fmt.Print("Checking for new leader after disconnecting previous leader ... ")
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.disconnect(leader1)
    ctrl.checkOneLeader()
    fmt.Println("ok")

    // if the old leader rejoins, that shouldn't disturb the current leader.
    fmt.Print("Checking for leader after previous leader reconnects ... ")
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.connect(leader1)
    leader2 := ctrl.checkOneLeader()
    fmt.Println("ok")

    // if there's no quorum, no leader should be elected.
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.disconnect(leader2)
    ctrl.disconnect((leader2 + 1) % numPeers)
    fmt.Print("Checking for no leader after majority disconnects ... ")
    ctrl.checkNoLeader()
    fmt.Println("ok")

    // if a quorum arises, it should elect a leader.
    fmt.Print("Checking for leader after follower reconnection ... ")
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.connect((leader2 + 1) % numPeers)
    ctrl.checkOneLeader()
    fmt.Println("ok")

    // re-join of previous leader shouldn't disturb the newly elected leader.
    fmt.Print("Checking for leader after previous leader reconnection ... ")
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.connect(leader2)
    ctrl.checkOneLeader()
    fmt.Println("ok")

    ctrl.cleanup()
}

// test basic agreement among Raft peers, specifically:
// -- does anyone commit anything before startCommit is called?
// -- if we call startCommit a few times, does each lead to a commitment?
func TestCheckpoint_BasicAgree(t *testing.T) {
    numPeers := 5
    numIters := 3
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    for i:=1; i<=numIters; i++ {
        fmt.Print("Checking for no early commit ... ")
        n, _ := ctrl.committedLogIndex(i)
        if n > 0 {
            ctrl.cleanup()
            t.Fatalf("Peers committed before start! (i = %d, n = %d)", i, n)
        }
        fmt.Println("ok")

        fmt.Print("Checking for correct commit ... ")
        ind := ctrl.startCommit(i*100, numPeers)
        if ind != i {
            ctrl.cleanup()
            t.Fatalf("Got index %d instead of %d", ind, i)
        }
        fmt.Println("ok")
    }
    ctrl.cleanup()
}

// test agreement among Raft peers with disconnection, specifically:
// -- do a basic commit and check for leader
// -- can we still get agreement with a disconnected peer?
// -- after peer reconnects, can we continue to get agreement?
func TestFinal_FailAgree(t *testing.T) {
    numPeers := 3
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    fmt.Print("Checking initial commit ... ")
    ctrl.startCommit(101, numPeers)
    // simulate a Raft peer failure
    failed := (ctrl.checkOneLeader() + 1) % numPeers
    ctrl.disconnect(failed)
    fmt.Println("ok")
    
    // agree despite one disconnected peer?
    fmt.Print("Checking agreement with one failed Raft peer ... ")
    ctrl.startCommit(102, numPeers-1)
    ctrl.startCommit(103, numPeers-1)
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.startCommit(104, numPeers-1)
    ctrl.startCommit(105, numPeers-1)
    fmt.Println("ok")

    // re-connect
    ctrl.connect(failed)

    // agree with full set of peers?
    fmt.Print("Checking agreement with reconnected peer ... ")
    ctrl.startCommit(106, numPeers)
    time.Sleep(ELECTION_TIMEOUT)
    ctrl.startCommit(107, numPeers)
    fmt.Println("ok")

    ctrl.cleanup()
}

// test non-agreement among Raft peers with disconnection, specifically:
// -- do we get any agreement when a majority disconnects?
// -- make sure logs are made consistent after everyone reconnects
// -- after reconnection, can we continue to get agreement?
func TestFinal_FailNoAgree(t *testing.T) {
    numPeers := 5
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    ctrl.startCommit(10, numPeers)
    leader1 := ctrl.checkOneLeader()

    fmt.Print("Checking for no agreement with majority of followers fail ... ")
    ctrl.disconnect((leader1 + 1) % numPeers)
    ctrl.disconnect((leader1 + 2) % numPeers)
    ctrl.disconnect((leader1 + 3) % numPeers)
   
    sr1 := ctrl.issueCommand(leader1, 20)
    if !sr1.Leader {
        ctrl.cleanup()
        t.Fatalf("Leader rejected a client command")
    }
    if sr1.Index != 2 {
        ctrl.cleanup()
        t.Fatalf("Got index %d instead of 2", sr1.Index)
    }
    time.Sleep(2 * ELECTION_TIMEOUT)

    n, _ := ctrl.committedLogIndex(sr1.Index)
    if n > 0 {
        ctrl.cleanup()
        t.Fatalf("%d peers committed without a majority", n)
    }
    fmt.Println("ok")

    // repair
    ctrl.connect((leader1 + 1) % numPeers)
    ctrl.connect((leader1 + 2) % numPeers)
    ctrl.connect((leader1 + 3) % numPeers)
    
    fmt.Print("Checking for consistent logs after everyone reconnects ... ")
    leader2 := ctrl.checkOneLeader()
    sr2 := ctrl.issueCommand(leader2, 30)
    if !sr2.Leader {
        ctrl.cleanup()
        t.Fatalf("Leader rejected a client command")
    }

    // the disconnected majority may have chosen a leader from among their
    // own ranks, forgetting 2nd entry, otherwise it will still be there
    if sr2.Index < 2 || sr2.Index > 3 {
        ctrl.cleanup()
        t.Fatalf("Unexpected index %d, should be 2 or 3", sr2.Index)
    }
    fmt.Println("ok")

    // run one more commit after everyone is all back together
    fmt.Print("Checking that more commits can be done after recovery ... ")
    ctrl.startCommit(100, numPeers)
    fmt.Println("ok")

    ctrl.cleanup()
}

// test reconnection of leader who kept getting requests while disconnected from others:
// -- find the initial leader, partition them, and send them some new commands
// -- send some new commands to the new leader, then disconnect them too
// -- then reconnect old leader to see if conflicts can be resolved with new submissions
// -- after everyone is reconnected, submit one more thing to commit
func TestFinal_Rejoin(t *testing.T) {
    numPeers := 3
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    // find the leader, partition everyone else, send leader some commands
    leader1 := ctrl.checkOneLeader()
    ctrl.disconnect((leader1 + 1) % numPeers)
    ctrl.disconnect((leader1 + 2) % numPeers)
    ctrl.issueCommand(leader1, 102)
    ctrl.issueCommand(leader1, 103)
    ctrl.issueCommand(leader1, 104)

    // disconnect the leader, reconnect others, give them a little time to elect a leader
    ctrl.disconnect(leader1)
    ctrl.connect((leader1 + 1) % numPeers)
    ctrl.connect((leader1 + 2) % numPeers)
    time.Sleep(ELECTION_TIMEOUT)

    fmt.Print("Checking commitment after original leader failed ... ")
    // new leader commits for index 2
    ctrl.startCommit(103, numPeers-1)
    fmt.Println("ok")
    
    fmt.Print("Checking log consistency after new leader fails and partitioned leader reconnects ... ")
    // new leader network failure
    leader2 := ctrl.checkOneLeader()
    ctrl.disconnect(leader2)

    // old leader connected again
    ctrl.connect(leader1)
    ctrl.startCommit(104, numPeers-1)
    fmt.Println("ok")

    fmt.Print("Checking log consistency after everyone rejoins ... ")
    // all together now
    ctrl.connect(leader2)
    ctrl.startCommit(105, numPeers)
    fmt.Println("ok")

    ctrl.cleanup()
}

// test resolution of logs populated with lots of uncommitted entries:
// -- find the initial leader, partition them with a follower, and send them lots of new commands
// -- then send lots of new commands to the larger partition, then disconnect that leader with a follower
//    and send lots more commands to the disconnected leader
// -- then reconnect the original leader and send them lots of new commands
// -- reconnect everyone, submit one more thing to commit, ensure consistency of logs
func TestFinal_Backup(t *testing.T) {
    numPeers := 5
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    ctrl.startCommit(109, numPeers)

    fmt.Print("checking commitment among separate partitions ... ")
    // disconnect everyone except leader and one follower.
    leader1 := ctrl.checkOneLeader()
    ctrl.disconnect((leader1 + 2) % numPeers)
    ctrl.disconnect((leader1 + 3) % numPeers)
    ctrl.disconnect((leader1 + 4) % numPeers)
    
    // submit lots of commands to the leader, which should not commit
    for i:=0; i<25; i++ {
        ctrl.issueCommand(leader1, 110+i)
    }
    
    // "swap" connectivity to the other partition.
    ctrl.disconnect(leader1)
    ctrl.disconnect((leader1 + 1) % numPeers)
    ctrl.connect((leader1 + 2) % numPeers)
    ctrl.connect((leader1 + 3) % numPeers)
    ctrl.connect((leader1 + 4) % numPeers)

    time.Sleep(ELECTION_TIMEOUT)
    
    // submit lots of commands to the new group, which should commit
    for i:=0; i<25; i++ {
        ctrl.startCommit(160+i, numPeers / 2 + 1)
    }
    fmt.Println("ok")

    fmt.Print("checking consistency after small partition recovers above majority ... ")
    // now another partitioned leader and one follower
    leader2 := ctrl.checkOneLeader()
    other := (leader1 + 2) % numPeers
    if leader2 == other {
        other = (leader2 + 1) % numPeers
    }
    ctrl.disconnect(other)

    // lots more commands that won't commit
    for i := 0; i < 25; i++ {
        ctrl.issueCommand(leader2, 210+i)
    }

    // bring original leader back to life
    for i:=0; i<numPeers; i++ {
        ctrl.disconnect(i)
    }
    ctrl.connect(leader1)
    ctrl.connect((leader1 + 1) % numPeers)
    ctrl.connect(other)

    time.Sleep(ELECTION_TIMEOUT)

    // lots of successful commands
    for i := 0; i < 25; i++ {
        ctrl.startCommit(260 + i, numPeers / 2 + 1)
    }
    fmt.Println("ok")

    fmt.Print("checking overall consistency after everyone reconnects ... ")
    // now everyone
    for i:=0; i<numPeers; i++ {
        ctrl.connect(i)
    }
    ctrl.startCommit(500, numPeers)
    fmt.Println("ok")

    ctrl.cleanup()
}

// test that number of remote calls made when using Raft is reasonable:
// -- checks that the #calls for election is in [7, 75]
// -- checks that the #calls for commitment is < 120
// -- checks that the #calls during a 1s idle is < 20
// -- along the way, checks that everything else is working correctly, just in case
func TestFinal_Count(t *testing.T) {
    numPeers := 3
    port := 7000 + rand.Intn(10000)
    ctrl := NewController(t, numPeers, port)

    var total1 int

    fmt.Print("checking #calls for election is suitable ... ")
    ctrl.checkOneLeader()
    for i:=0; i<numPeers; i++ {
        total1 += ctrl.getCallCount(i)
    }

    if total1 < 7 || total1 > 75 {
        ctrl.cleanup()
        t.Fatalf("Too many or too few remote calls to elect a leader")
    }
    fmt.Println("ok")

    var success bool
    var total2 int

    fmt.Print("checking #calls for commitment is suitable ... ")
tryLoop:
    for try:=0; try<5; try++ {
        if try > 0 {
            // give solution some time to settle after first try
            time.Sleep(3 * time.Second)
        }
        
        leader := ctrl.checkOneLeader()
        total1 = 0
        for i:=0; i<numPeers; i++ {
            total1 += ctrl.getCallCount(i)
        }

        iters := 10
        
        sr1 := ctrl.issueCommand(leader, 1)
        if !sr1.Leader {
            // leader moved on too quickly, go to next try
            continue tryLoop
        }

        cmds := []int{}
        for i:=1; i<iters+2; i++ {
            x := int(rand.Int31())
            cmds = append(cmds, x)
            
            sr2:= ctrl.issueCommand(leader, x)
            
            if sr2.Term != sr1.Term || !sr2.Leader {
                // term changed while starting or no longer leader
                continue tryLoop
            }            

            if sr1.Index + i != sr2.Index {
                ctrl.cleanup()
                t.Fatalf("issueCommand() failed")
            }
        }

        for i:=1; i<iters+1; i++ {
            cmd := ctrl.wait(sr1.Index + i, numPeers, sr1.Term)
            if cmd != cmds[i-1] {
                if cmd == -1 {
                    // term changed -- try again
                    continue tryLoop
                }
                ctrl.cleanup()
                t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, sr1.Index+1, cmds)
            }
        }

        failed := false
        total2 = 0
        for k:=0; k<numPeers; k++ {
            sr, roe := ctrl.peers[k].GetStatus()
            if roe.Error() != "" {
                continue
            }   
            
            if sr.Term != sr1.Term {
                // term changed -- can't expect low RPC counts
                // need to keep going to update total2
                failed = true
            }
            total2 += sr.CallCount
        }

        if failed {
            continue tryLoop
        }

        fmt.Println("#calls for commitment: ", total2 - total1)
        
        if total2 - total1 > 200 {
            ctrl.cleanup()
            t.Fatalf("Too many remote calls for commitment")
        }
        fmt.Println("ok")
        
        success = true
        break
    }

    if !success {
        ctrl.cleanup()
        t.Fatalf("commitment failed (term changed too often)")
    }

    time.Sleep(ELECTION_TIMEOUT)
    fmt.Print("checking #calls while idle is suitable ... ")

    total3 := 0
    for k:=0; k<numPeers; k++ {
        total3 += ctrl.getCallCount(k)
    }

    fmt.Println("#calls for idle: ", total3 - total2)
    if total3 - total2 > 20 {
        ctrl.cleanup()
        t.Fatalf("Too many remote calls for 1 second idle")
    }
    fmt.Println("ok")
    
    ctrl.cleanup()
}

