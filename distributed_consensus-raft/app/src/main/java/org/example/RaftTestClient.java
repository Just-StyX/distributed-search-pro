package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.example.raft.rpc.*;

import java.util.*;

public class RaftTestClient {
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new HashMap<>();
    private String knownLeaderId = "node1";

    public RaftTestClient(Map<String, String> nodeAddresses) {
        for (var entry : nodeAddresses.entrySet()) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost",
                            Integer.parseInt(entry.getValue().split(":")[1]))
                    .usePlaintext().build();
            stubs.put(entry.getKey(), RaftServiceGrpc.newBlockingStub(channel));
        }
    }

    public void sendBoost(String word) {
        System.out.println(">>> Sending boost for: " + word);
        boolean success = false;

        while (!success) {
            try {
                BoostResponse response = stubs.get(knownLeaderId).handleBoost(
                        BoostRequest.newBuilder().setWord(word).build()
                );

                if (response.getSuccess()) {
                    System.out.println("✅ Boost confirmed by Leader: " + knownLeaderId);
                    success = true;
                } else {
                    System.out.println("❌ " + knownLeaderId + " is a Follower. Redirecting to: " + response.getLeaderId());
                    knownLeaderId = response.getLeaderId();
                }
            } catch (Exception e) {
                System.err.println("⚠️ Node " + knownLeaderId + " unreachable. Trying another...");
                knownLeaderId = stubs.keySet().iterator().next(); // Simple failover
                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            }
        }
    }

    public void verifyReplication(String query) {
        System.out.println("\n--- Verifying Replication Across Cluster ---");
        for (String nodeId : stubs.keySet()) {
            try {
                // 1. Explicitly check the search endpoint
                SearchResponse resp = stubs.get(nodeId).handleSearch(
                        SearchRequest.newBuilder()
                                .setQuery(query)
                                .setLimit(5)
                                .build()
                );

                if (resp.getResultsList().isEmpty()) {
                    System.out.println("Node [" + nodeId + "] ⚠️ Log replicated but TST not yet updated.");
                } else {
                    System.out.println("Node [" + nodeId + "] ✅ Results: " + resp.getResultsList());
                }
            } catch (io.grpc.StatusRuntimeException e) {
                // 2. This tells us EXACTLY why it thinks it's offline
                System.out.println("Node [" + nodeId + "] ❌ Error: " + e.getStatus().getCode());
            } catch (Exception e) {
                System.out.println("Node [" + nodeId + "] ❌ Unknown Error: " + e.getMessage());
            }
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Map<String, String> nodes = Map.of(
                "node1", "localhost:50051",
                "node2", "localhost:50052",
                "node3", "localhost:50053"
        );

        RaftTestClient client = new RaftTestClient(nodes);

        client.sendInsert("apple", 10, "/shop/fruits/apple");// Ensure the word is there!
//        Thread.sleep(500);
        // 1. Send the boost
        client.sendBoost("apple");

        // 2. Wait for Raft replication (Quorum + Commit)
        System.out.println("Waiting for log replication...");
        Thread.sleep(1000);

        // Try to verify multiple times in case of network lag
        for(int i = 0; i < 3; i++) {
            client.verifyReplication("app");
            Thread.sleep(1000);
        }

        // 3. Verify
//        client.verifyReplication("app");
    }

    // Inside RaftTestClient.java
    public void sendInsert(String word, int weight, String url) {
        System.out.println(">>> Inserting word: " + word + " with URL: " + url);
        boolean success = false;

        while (!success) {
            try {
                InsertResponse response = stubs.get(knownLeaderId).handleInsert(
                        InsertRequest.newBuilder()
                                .setWord(word)
                                .setWeight(weight)
                                .setUrl(url) // <--- Pass the URL here
                                .build()
                );

                if (response.getSuccess()) {
                    System.out.println("✅ Insert confirmed by Leader: " + knownLeaderId);
                    success = true;
                } else {
                    knownLeaderId = response.getLeaderId();
                }
            } catch (Exception e) {
                // ... retry logic ...
            }
        }
    }

}
