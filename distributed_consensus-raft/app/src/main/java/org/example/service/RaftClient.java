package org.example.service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.example.raft.rpc.RaftServiceGrpc;

public class RaftClient {
    private final String peerId;
    private final ManagedChannel channel;
    // We use the Async stub for non-blocking elections/heartbeats
    private final RaftServiceGrpc.RaftServiceStub asyncStub;

    public RaftClient(String peerId, String host, int port) {
        this.peerId = peerId;
        // Plaintext is fine for local testing; use TLS for production
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.asyncStub = RaftServiceGrpc.newStub(channel);
    }

    public RaftServiceGrpc.RaftServiceStub getAsyncStub() {
        return asyncStub;
    }

    public void shutdown() {
        channel.shutdown();
    }
}
