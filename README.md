## Potential Failure Scenarios & Limitation
Our implementation of the raft algorithm is mostly robust. One of the failures that we spotted
is that when there are inconsistencies in the logs, the peer should only delete one term in the 
log at a time, but it instead deletes everything. This does not hurt the correctness of our 
implementation, however, as the logs will still be replaced by the reference from the leader. 
Nevertheless, it will affect the performance as the packet size grows larger

### Repository contents

The top-level directory includes:
* This `README.md` file
* The `Makefile`, described in detail later
* The `src` source directory, containing the `raft` source directory along with the test code

Visually, this looks roughly like the following:
```
\---lab2-go
    +---src
        +---raft
        |   +---raft.go
        |   \---raft_test.go
    +---Makefile
    \---README.md
```
The details of each of these will hopefully become clear after reading the rest of this file.


### Implementing Raft

The first things you'll do in this lab is to copy your `remote` library code from Lab 1 into a directory in your 
Lab 2 repository at the location `src/remote` (i.e., a sibling directory to `src/raft`).  It is ok if you need to 
make changes to your `remote` library for Lab 2.  You do not need to copy the test code from lab 1, just the `.go` 
files comprising the remote object library itself.

The `raft` package initially includes a rough outline of what you are required to implement, mainly based on the
Raft paper but also adhering to the needs of our test suite (see below).  Your primary task is to complete the
implementation of the Raft protocol according to the specifications given in the Canvas assignment.  You are free
to create additional source files within the `raft` package as needed, and you can use whatever data structures, 
functions, and go routines that you desire to implement the protocol.  However, you cannot change the test suite
(or at least, your code must work with an unmodified test suite), so the provided interactions between `raft.go`
and `raft_test.go` must be maintained.  You are welcome (and encouraged) to read through the test code to see how
the test `Controller` and the test cases work and what they are testing.


### Testing Raft Implementation

Once you're at the point where you want to run any of the provided tests, you can either use the appropriate `go test`
commands or the provided `make` rules `checkpoint`, `final`, and `all` as used in the previous labs.  The `Makefile`
 also includes rules that run Go's race detector to ensure that your implementation is thread safe.  These rules are 
`checkpoint-race`, `test-race`, and `all-race`, and these rules are used in the auto-grader.


### Generating documentation

We want you to get in the habit of documenting your code in a way that leads to detailed, easy-to-read/-navigate 
package documentation for your code package. Our Makefile includes a `docs` rule that will pipe the output of the 
`go doc` command into a text file that is reasonably easy to read.  We will use this output for the manually graded 
parts of the lab, so good comments are valuable.

