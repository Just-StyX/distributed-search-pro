package org.example.structures;

/*
* Implementing the
Raft Node State Machine is where we transition from a local log to a "Living Cluster." In Raft, a node is always in one of three states: Follower, Candidate, or Leader.
 */

public enum RaftState {
    FOLLOWER,   // Passive: responds to RPCs from Leaders/Candidates
    CANDIDATE,  // Active: attempting to become a Leader
    LEADER      // Dominant: handles client requests and replicates logs
}
