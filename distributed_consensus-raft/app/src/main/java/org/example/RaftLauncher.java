package org.example;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.example.structures.RaftLogManager;
import org.example.structures.RaftNode;
import org.example.structures.ThreadSafeWeightedTST;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RaftLauncher {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: RaftLauncher <nodeId> <port> <peer1Id:addr1,peer2Id:addr2...>");
            return;
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        String peerInfo = args[2];

        // 1. Create a dedicated data directory for this node
        String dataDir = "data/" + nodeId + "/";
        new File(dataDir).mkdirs();

        // 2. Initialize the TST and the Log Manager
        ThreadSafeWeightedTST tst = new ThreadSafeWeightedTST();
        RaftLogManager logManager = new RaftLogManager(dataDir + "raft.log", tst);

        // 3. Create the Raft Node
        RaftNode node = new RaftNode(nodeId, logManager);

        // 4. Parse peer addresses
        Map<String, String> peerAddresses = getStringStringMap(peerInfo);

        // 5. Start the engine
        node.start(peerAddresses, port);

        System.out.println(">>> Raft Node " + nodeId + " is LIVE on port " + port);
    }

    private static @NonNull Map<String, String> getStringStringMap(String peerInfo) {
        Map<String, String> peerAddresses = new HashMap<>();
        for (String p : peerInfo.split(",")) {
            String[] parts = p.split(":");
            if (parts.length == 2) {
                // Format: nodeId:localhost:port
                peerAddresses.put(parts[0], parts[1] + ":" + parts[2]);
            } else if (parts.length == 3) {
                // Format: nodeId:localhost:port (handling the extra colon)
                peerAddresses.put(parts[0], parts[1] + ":" + parts[2]);
            }
        }
        return peerAddresses;
    }
}
