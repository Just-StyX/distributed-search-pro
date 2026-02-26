package org.example.service;

import io.grpc.stub.StreamObserver;
import org.example.raft.rpc.*;
import org.example.structures.RaftCommand;
import org.example.structures.RaftNode;
import org.example.structures.RaftState;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode node;

    public RaftServiceImpl(RaftNode node) {
        this.node = node;
    }

    @Override
    public void appendEntries(AppendEntriesRequest request,
                              StreamObserver<AppendEntriesResponse> responseObserver) {
        long term = request.getTerm();
        boolean success = false;

        if (term >= node.getCurrentTerm()) {
            node.transitionToFollower(term, request.getLeaderId());
            success = true;

            // --- NEW: STEP 4 - APPEND NEW ENTRIES ---
            if (request.getEntriesCount() > 0) {
                for (LogEntry entry : request.getEntriesList()) {
                    // Parse the "TYPE:WORD:VALUE:URL" string back into a RaftCommand
                    RaftCommand cmd = node.getLogManager().deserializeCommand(
                            entry.getCommand(), entry.getTerm()
                    );

                    // Save to the follower's local raft.log and memoryLog
                    node.getLogManager().appendOnly(cmd);
                }
                System.out.println("[" + node.getNodeId() + "] Appended " + request.getEntriesCount() + " logs.");
            }
        }

        // --- STEP 5 - UPDATE COMMIT INDEX ---
        if (success && request.getLeaderCommit() > node.getLogManager().getCommitIndex()) {
            long newCommitIndex = Math.min(request.getLeaderCommit(), node.getLogManager().getLastLogIndex());
            node.getLogManager().setCommitIndex(newCommitIndex);
            node.applyCommits();
        }

        responseObserver.onNext(AppendEntriesResponse.newBuilder()
                .setTerm(node.getCurrentTerm())
                .setSuccess(success).build());
        responseObserver.onCompleted();
    }


    @Override
    public void requestVote(RequestVoteRequest request,
                            StreamObserver<RequestVoteResponse> responseObserver) {

        long candidateTerm = request.getTerm();
        String candidateId = request.getCandidateId();
        boolean voteGranted = false;

        synchronized (node) {
            long currentTerm = node.getCurrentTerm();

            // 1. If candidate's term is higher, we must step down to FOLLOWER
            if (candidateTerm > currentTerm) {
                node.transitionToFollower(candidateTerm);
                currentTerm = candidateTerm;
            }

            // 2. Voting Logic
            // We grant the vote if:
            // a) Candidate's term is current (or we just updated to it)
            // b) We haven't voted for anyone else in this term (votedFor is null or the candidate)
            // c) Candidate's log is at least as up-to-date as ours (Safety Check)
            if (candidateTerm == currentTerm &&
                    (node.getVotedFor() == null || node.getVotedFor().equals(candidateId))) {

                if (isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {
                    voteGranted = true;
                    node.setVotedFor(candidateId);
                    node.resetElectionTimer(); // Granting a vote resets our timer
                    System.out.println(node.getNodeId() + " granted vote to " + candidateId);
                }
            }
        }

        RequestVoteResponse response = RequestVoteResponse.newBuilder()
                .setTerm(node.getCurrentTerm())
                .setVoteGranted(voteGranted)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // 3. Log Up-to-Date Safety Check
    private boolean isLogUpToDate(long lastLogIndex, long lastLogTerm) {
        long myLastLogTerm = node.getLogManager().getLastLogTerm();
        long myLastLogIndex = node.getLogManager().getLastLogIndex();

        if (lastLogTerm != myLastLogTerm) {
            return lastLogTerm > myLastLogTerm;
        }
        return lastLogIndex >= myLastLogIndex;
    }

    @Override
    public void handleBoost(BoostRequest req, StreamObserver<BoostResponse> observer) {
        BoostResponse.Builder response = BoostResponse.newBuilder();

        // Get the current leader ID safely
        String leaderId = node.getLeaderId();
        if (leaderId == null) {
            leaderId = ""; // Protobuf prefers empty strings over nulls
        }

        if (node.getState() != RaftState.LEADER) {
            response.setSuccess(false);
            response.setLeaderId(leaderId);
        } else {
            boolean result = node.handleClientRequest(req.getWord());
            response.setSuccess(result);
            response.setLeaderId(node.getNodeId());
        }

        observer.onNext(response.build());
        observer.onCompleted();
    }


    // Inside RaftServiceImpl.java
    @Override
    public void installSnapshot(InstallSnapshotRequest request,
                                StreamObserver<InstallSnapshotResponse> responseObserver) {

        if (request.getTerm() < node.getCurrentTerm()) {
            responseObserver.onNext(InstallSnapshotResponse.newBuilder()
                    .setTerm(node.getCurrentTerm()).build());
            responseObserver.onCompleted();
            return;
        }

        // Reset election timer because we heard from a valid leader
        node.transitionToFollower(request.getTerm(), request.getLeaderId());

        // Tell the LogManager to overwrite everything with this snapshot
        node.getLogManager().applySnapshot(
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                request.getData().toByteArray()
        );

        responseObserver.onNext(InstallSnapshotResponse.newBuilder()
                .setTerm(node.getCurrentTerm()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void handleSearch(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
        SearchResponse.Builder response = SearchResponse.newBuilder();

        // 1. Get results from the Node (Local TST)
        // We use 'true' for allowStale to make searches ultra-fast on any node
        var results = node.handleClientRead(request.getQuery(), request.getLimit(), true);

        if (results != null) {
            for (var item : results) {
                response.addResults(SearchResultItem.newBuilder()
                        .setWord(item.word())
                        .setUrl(item.url() != null ? item.url() : "")
                        .build());
            }
            response.setSuccess(true);
        } else {
            // If we don't allow stale reads and aren't leader, point to the leader
            response.setSuccess(false);
            response.setLeaderId(node.getLeaderId() != null ? node.getLeaderId() : "");
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void handleInsert(InsertRequest req, StreamObserver<InsertResponse> responseObserver) {
        InsertResponse.Builder response = InsertResponse.newBuilder();

        if (node.getState() != RaftState.LEADER) {
            response.setSuccess(false);
            response.setLeaderId(node.getLeaderId() != null ? node.getLeaderId() : "");
        } else {
            // Create an INSERT command for the Raft log
            // Note: You'll need to update RaftNode.handleClientRequest to accept type/weight/url
            boolean result = node.handleClientInsert(req.getWord(), req.getWeight(), req.getUrl());
            response.setSuccess(result);
            response.setLeaderId(node.getNodeId());
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }



}
