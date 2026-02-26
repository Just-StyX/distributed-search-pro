package org.example.structures;

import io.grpc.stub.StreamObserver;
import org.example.raft.rpc.*;
import org.example.service.RaftClient;
import org.example.service.RaftServiceImpl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
* This class manages the "Terms" (logical clocks) and the transitions between states. It uses a ScheduledExecutorService for the "Heartbeat" and "Election" timers.
 */

public class RaftNode {
    private long lastApplied = 0;
    private final String nodeId;
    private RaftState state = RaftState.FOLLOWER;

    private io.grpc.Server grpcServer;

    private String leaderId = null; // The ID of the current leader

    // For each follower, the index of the next log entry to send (initialized to leader's last index + 1)
    private Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    // For each follower, the index of the highest log entry known to be replicated
    private Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Raft Persistent State
    private final AtomicLong currentTerm = new AtomicLong(0);
    private String votedFor = null;

    // Timers
    private ScheduledFuture<?> electionTimer;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private final RaftLogManager logManager;
    private final long electionTimeout; // e.g., 150-300ms randomized
    private final Map<String, RaftClient> peers = new ConcurrentHashMap<>();

    public void initializePeers(Map<String, String> peerAddresses) {
        for (Map.Entry<String, String> entry : peerAddresses.entrySet()) {
            String peerId = entry.getKey();
            String address = entry.getValue(); // e.g., "localhost:50052"

            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            peers.put(peerId, new RaftClient(peerId, host, port));
        }
    }


    public RaftNode(String nodeId, RaftLogManager logManager) {
        this.nodeId = nodeId;
        this.logManager = logManager;
        this.electionTimeout = ThreadLocalRandom.current().nextLong(150, 300);
        resetElectionTimer();
    }

    public long getCurrentTerm() {
        // Returns the current logical clock (term) of this node
        return currentTerm.get();
    }

    public synchronized String getVotedFor() { return votedFor; }
    public synchronized void setVotedFor(String candidateId) { this.votedFor = candidateId; }
    public String getNodeId() { return nodeId; }
    public RaftLogManager getLogManager() { return logManager; }

    public RaftState getState() {
        return state;
    }

    public void setState(RaftState state) {
        this.state = state;
    }
    // --- STATE TRANSITIONS ---

    private synchronized void transitionToCandidate() {
        System.out.println(nodeId + " transitioning to CANDIDATE for term " + currentTerm.incrementAndGet());
        this.state = RaftState.CANDIDATE;
        this.votedFor = nodeId; // Vote for self
        startElection();
    }

//    private synchronized void transitionToLeader() {
//        System.out.println(nodeId + " became LEADER for term " + currentTerm.get());
//        this.state = RaftState.LEADER;
//        stopElectionTimer();
//        startHeartbeat(); // Start sending empty AppendEntries to followers
//    }

    public synchronized void transitionToFollower(long newTerm) {
        this.state = RaftState.FOLLOWER;
        this.currentTerm.set(newTerm);
        this.votedFor = null;
        resetElectionTimer();
    }

    public void start(Map<String, String> peerAddresses, int port) {
        // 1. Initialize the phonebook
        initializePeers(peerAddresses);

        // 2. Start the gRPC Server so others can talk to us
        startGrpcServer(port);

        // 3. Kick off the election timer (This starts the Raft logic)
        resetElectionTimer();

        System.out.println("Node " + nodeId + " started on port " + port);
    }

    // --- TIMERS ---

    public void resetElectionTimer() {
        stopElectionTimer();
        electionTimer = scheduler.schedule(this::transitionToCandidate,
                electionTimeout, TimeUnit.MILLISECONDS);
    }

    private void stopElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
    }

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            if (state == RaftState.LEADER) {
                sendHeartbeats();
            }
        }, 0, 50, TimeUnit.MILLISECONDS); // Heartbeat every 50ms
    }

    public synchronized void startElection() {
        if (state == RaftState.LEADER) return; // Already leader

        // 1. Transition to Candidate
        this.state = RaftState.CANDIDATE;
        this.currentTerm.incrementAndGet();
        this.votedFor = nodeId; // Vote for self

        System.out.println(nodeId + " starting election for term " + currentTerm.get());

        // 2. Prepare the Request
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(currentTerm.get())
                .setCandidateId(nodeId)
                .setLastLogIndex(logManager.getLastLogIndex())
                .setLastLogTerm(logManager.getLastLogTerm())
                .build();

        // 3. Track votes (start with 1 for self)
        AtomicInteger votesReceived = new AtomicInteger(1);
        int majority = (peers.size() + 1) / 2 + 1;

        // 4. Reset timer so we can retry if we don't get a majority in time
        resetElectionTimer();

        // 5. Send requests to all peers asynchronously
        for (RaftClient peer : peers.values()) {
            peer.getAsyncStub().requestVote(request, new StreamObserver<RequestVoteResponse>() {
                @Override
                public void onNext(RequestVoteResponse response) {
                    handleVoteResponse(response, votesReceived, majority);
                }
                @Override public void onError(Throwable t) { /* Log error */ }
                @Override public void onCompleted() {}
            });
        }
    }

    public synchronized void applyCommits() {
        long commitIndex = logManager.getCommitIndex();

        while (lastApplied < commitIndex) {
            lastApplied++;
            // Get the command at the specific 1-based Raft index
            RaftCommand cmd = logManager.getCommandAtIndex(lastApplied);

            if (cmd != null) {
                // Apply the actual BOOST or INSERT to the TST
                logManager.applyToStateMachine(cmd);
                System.out.println("[" + nodeId + "] ✅ Applied to TST at index " + lastApplied + ": " + cmd.word());
            }
        }
    }


    public synchronized void handleVoteResponse(RequestVoteResponse response,
                                                AtomicInteger votesReceived,
                                                int majority) {
        // If the response comes from a higher term, we are outdated
        if (response.getTerm() > currentTerm.get()) {
            transitionToFollower(response.getTerm());
            return;
        }

        if (state == RaftState.CANDIDATE && response.getVoteGranted()) {
            int currentVotes = votesReceived.incrementAndGet();
            if (currentVotes >= majority) {
                transitionToLeader();
            }
        }
    }

    private void startGrpcServer(int port) {
        try {
            // Create the server and register your Raft logic
            this.grpcServer = io.grpc.ServerBuilder.forPort(port)
                    .addService(new RaftServiceImpl(this)) // Connects the RPCs to this node
                    .build()
                    .start();

            System.out.println(nodeId + " gRPC server started on port " + port);

            // Optional: Ensure the server shuts down when the JVM exits
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                this.stop();
            }));
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to start gRPC server on port " + port, e);
        }
    }

    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
        scheduler.shutdown();
        peers.values().forEach(RaftClient::shutdown);
    }

    private void sendHeartbeats() {
        // 1. Prepare the standard heartbeat request
        // Note: entries is empty, and for a simple heartbeat,
        // we use our current last log index/term.
        AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                .setTerm(currentTerm.get())
                .setLeaderId(nodeId)
                .setPrevLogIndex(logManager.getLastLogIndex())
                .setPrevLogTerm(logManager.getLastLogTerm())
                .setLeaderCommit(logManager.getCommitIndex()) // Assuming we added a commitIndex tracker
                .build();

        // 2. Broadcast to all peers
        for (RaftClient peer : peers.values()) {
            peer.getAsyncStub().appendEntries(heartbeat, new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(AppendEntriesResponse response) {
                    // 3. If a follower has a higher term, the leader must step down
                    if (response.getTerm() > currentTerm.get()) {
                        System.out.println(nodeId + " saw higher term, stepping down.");
                        transitionToFollower(response.getTerm());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    // Silently ignore: peers might be temporarily down
                }

                @Override
                public void onCompleted() {}
            });
        }
    }

    private synchronized void transitionToLeader() {
        this.state = RaftState.LEADER;
        long lastIndex = logManager.getLastLogIndex();
        for (String peerId : peers.keySet()) {
            nextIndex.put(peerId, lastIndex + 1);
            matchIndex.put(peerId, 0L);
        }
        startHeartbeat();
    }

    private void replicateToPeer(String peerId, RaftClient peer) {
        long prevIndex = nextIndex.get(peerId) - 1;
        long prevTerm = logManager.getTermAtIndex(prevIndex);

        // Get all entries from nextIndex to the end of our log
        List<LogEntry> entriesToSend = logManager.getEntriesSince(nextIndex.get(peerId));

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(currentTerm.get())
                .setLeaderId(nodeId)
                .setPrevLogIndex(prevIndex)
                .setPrevLogTerm(prevTerm)
                .addAllEntries(entriesToSend)
                .setLeaderCommit(logManager.getCommitIndex())
                .build();

        peer.getAsyncStub().appendEntries(request, new StreamObserver<AppendEntriesResponse>() {
            @Override
            public void onNext(AppendEntriesResponse response) {
                handleReplicationResponse(peerId, response, request);
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });
    }

    private synchronized void handleReplicationResponse(String peerId, AppendEntriesResponse response, AppendEntriesRequest request) {
        if (response.getTerm() > currentTerm.get()) {
            transitionToFollower(response.getTerm());
            return;
        }

        if (response.getSuccess()) {
            // Update indices for this follower
            long lastAppended = request.getPrevLogIndex() + request.getEntriesCount();
            nextIndex.put(peerId, lastAppended + 1);
            matchIndex.put(peerId, lastAppended);

            // Check if we can now COMMIT a new index (if a majority matches it)
            tryCommit(lastAppended);
            applyCommits();
        } else {
            // LOG MISMATCH: Decrement nextIndex and the next heartbeat will retry the previous entry
            nextIndex.put(peerId, Math.max(1, nextIndex.get(peerId) - 1));
        }
    }

    private void tryCommit(long index) {
        if (index <= logManager.getCommitIndex()) return;

        // 1. Count how many nodes have replicated up to 'index'
        int count = 1;
        for (long match : matchIndex.values()) {
            if (match >= index) count++;
        }

        int majority = (peers.size() + 1) / 2 + 1;

        // 2. If majority reached, update and PROPAGATE
        if (count >= majority) {
            logManager.setCommitIndex(index);
            System.out.println("[" + nodeId + "] LOG COMMITTED up to index " + index);

            // --- THE MISSING LINKS ---

            // A. Leader must physically update its own TST
            applyCommits();

            // B. Leader must tell followers about the new commitIndex immediately
            // This ensures Node 3 turns "Green" in your test client logs
            sendHeartbeats();
        }
    }


    // Inside RaftNode.java
    public synchronized boolean handleClientRequest(String word) {
        if (state != RaftState.LEADER) return false; // Client must redirect to leader

        // 1. Create and Append locally
        RaftCommand cmd = RaftCommand.boost(word, currentTerm.get());
        logManager.appendAndApply(cmd);

        // 2. Trigger replication immediately (don't wait for next heartbeat)
        for (Map.Entry<String, RaftClient> entry : peers.entrySet()) {
            replicateToPeer(entry.getKey(), entry.getValue());
        }

        return true;
    }

    /**
     * Handles an autocomplete request.
     * @param allowStale If true, any node can answer. If false, only the leader answers.
     */
    public List<SearchResult> handleClientRead(String query, int limit, boolean allowStale) {
        // 1. Policy: Strict Linearizability
        if (!allowStale && state != RaftState.LEADER) {
            return null; // Signal to redirect to leader
        }

        // 2. Policy: Eventual Consistency (Follower Read)
        // Even if we are a follower, we serve the request from our local TST.
        // It might be 50ms behind the leader, but it's O(0.028 microseconds) fast!
        return logManager.getTst().autocomplete(query, limit);
    }

    // In RaftNode.java, update this whenever we get a heartbeat
    public synchronized void transitionToFollower(long term, String leaderId) {
        this.currentTerm.set(term);
        this.state = RaftState.FOLLOWER;
        this.votedFor = null;
        this.leaderId = leaderId; // Now we know who the leader is!
        resetElectionTimer();
    }

    public String getLeaderId() {
        return this.leaderId;
    }

    public synchronized boolean handleClientInsert(String word, int weight, String url) {
        if (state != RaftState.LEADER) return false;

        // Use the term and a new type "INSERT"
        RaftCommand cmd = new RaftCommand("INSERT", word, weight, currentTerm.get(), url);
        // (Ensure your RaftCommand can store the URL or use a structured string)

        logManager.appendAndApply(cmd);

        // Trigger immediate replication
        replicateToAllPeers();
        return true;
    }

    /**
     * Triggers an immediate log replication to all known peers.
     * Called by the Leader as soon as a client 'Insert' or 'Boost' is received.
     */
    private void replicateToAllPeers() {
        if (state != RaftState.LEADER) return;

        for (Map.Entry<String, RaftClient> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            RaftClient client = entry.getValue();

            // Use the existing logic to calculate nextIndex and send AppendEntries
            replicateToPeer(peerId, client);
        }
    }


    // Add this to your scheduler or call it after every successful replicateResponse
//    private synchronized void applyCommits() {
//        long commitIndex = logManager.getCommitIndex();
//
//        while (lastApplied < commitIndex) {
//            lastApplied++;
//            // 1. Get the command from the log at this index
//            RaftCommand cmd = logManager.getCommandAtIndex(lastApplied);
//
//            if (cmd != null) {
//                // 2. PHYSICALLY update the TST
//                logManager.applyToStateMachine(cmd);
//                System.out.println("[" + nodeId + "] Applied to TST: " + cmd.word());
//            }
//        }
//    }

}

