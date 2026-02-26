package org.example.structures;

import io.grpc.stub.StreamObserver;
import org.example.raft.rpc.AppendEntriesRequest;
import org.example.raft.rpc.AppendEntriesResponse;
import org.example.raft.rpc.LogEntry;
import org.example.service.RaftClient;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
* This class handles writing every command to a file before applying it to the TST. This is called a Write-Ahead Log (WAL).
 */
public class RaftLogManager {
    private final String logPath;
    private long commitIndex = 0;
    private long lastIncludedIndex = 0;
    private long lastIncludedTerm = 0;
    private final List<RaftCommand> memoryLog = new ArrayList<>();
    private ThreadSafeWeightedTST tst;

    public RaftLogManager(String logPath, ThreadSafeWeightedTST tst) {
        this.logPath = logPath;
        this.tst = tst;
        // Rebuild the TST on startup
        replayLog();
    }

    public ThreadSafeWeightedTST getTst() {
        return tst;
    }

    /*
    * While ObjectOutputStream is easy for a prototype, in a production Raft system, we would use Protobuf or FlatBuffers for the log to make it cross-platform and faster.
     */

    // 1. APPEND: Save to disk, then apply to TST
    public synchronized void appendAndApply(RaftCommand command) {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(logPath, true))) {

            oos.writeObject(command);
            memoryLog.add(command);
            applyToStateMachine(command);

        } catch (IOException e) {
            throw new RuntimeException("Failed to persist log!", e);
        }
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(long index) {
        this.commitIndex = index;
    }

    public synchronized long getLastLogIndex() {
        if (memoryLog.isEmpty()) {
            return lastIncludedIndex; // The snapshot is the last known point
        }
        return lastIncludedIndex + memoryLog.size();
    }

    public synchronized long getLastLogTerm() {
        if (memoryLog.isEmpty()) {
            return lastIncludedTerm;
        }
        return memoryLog.getLast().term();
    }


    // 2. REPLAY: Rebuild the TST from the file on restart
    private void replayLog() {
        File file = new File(logPath);
        if (!file.exists()) return;

        try (FileInputStream fis = new FileInputStream(file)) {
            while (fis.available() > 0) {
                ObjectInputStream ois = new ObjectInputStream(fis);
                RaftCommand cmd = (RaftCommand) ois.readObject();
                memoryLog.add(cmd);
                applyToStateMachine(cmd);
            }
        } catch (Exception e) {
            System.err.println("Log replay finished or interrupted: " + e.getMessage());
        }
    }

    public synchronized void appendOnly(RaftCommand command) {
        // 1. Write to Disk
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(logPath, true))) {
            oos.writeObject(command);
            // 2. Add to Memory List
            memoryLog.add(command);
        } catch (IOException e) { e.printStackTrace(); }
    }

    /**
     * Converts a string from the gRPC layer back into a RaftCommand.
     * Format: "TYPE|WORD|VALUE|URL"
     */
    public RaftCommand deserializeCommand(String commandStr, long term) {
        // We use "\\|" because "|" is a special character in Regex
        String[] parts = commandStr.split("\\|");

        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid command format: " + commandStr);
        }

        String type = parts[0];
        String word = parts[1];
        int value = Integer.parseInt(parts[2]);
        String url = (parts.length > 3) ? parts[3] : "";

        return new RaftCommand(type, word, value, term, url);
    }

    private String serializeCommand(RaftCommand cmd) {
        // Leader packs the command into a single string for transport
        return String.format("%s|%s|%d|%s",
                cmd.type(),
                cmd.word(),
                cmd.value(),
                cmd.url() != null ? cmd.url() : "");
    }


//    // 3. APPLY: The bridge to your existing TST logic
//    private void applyToStateMachine(RaftCommand cmd) {
//        switch (cmd.type()) {
//            case "BOOST" -> tst.incrementWeight(cmd.word());
//            case "INSERT" -> tst.insert(cmd.word(), cmd.value(), "");
//        }
//    }

    /**
     * Returns the term of the log entry at a specific index.
     * Index 0 is reserved for the 'empty' state.
     */
    public synchronized long getTermAtIndex(long raftIndex) {
        if (raftIndex <= 0) return 0;
        if (raftIndex == lastIncludedIndex) return lastIncludedTerm;

        // Convert Raft Index to internal List Index
        int internalIndex = (int) (raftIndex - lastIncludedIndex - 1);

        if (internalIndex < 0) {
            // This index was already deleted/snapshotted!
            // The leader must send a Snapshot instead of AppendEntries.
            return -2;
        }

        if (internalIndex >= memoryLog.size()) return -1;
        return memoryLog.get(internalIndex).term();
    }

    /**
     * Returns all entries starting from 'nextIndex' to the end of the log.
     * Converts internal RaftCommands into gRPC LogEntry objects.
     */
    public synchronized List<LogEntry> getEntriesSince(long nextRaftIndex) {
        // Calculate where to start in our truncated list
        int startIndex = (int) (nextRaftIndex - lastIncludedIndex - 1);

        // If the required index is already gone, return null to trigger InstallSnapshot
        if (startIndex < 0) return null;

        return memoryLog.stream()
                .skip(startIndex)
                .map(cmd -> LogEntry.newBuilder()
                        .setTerm(cmd.term())
                        .setCommand(serializeCommand(cmd))
                        .build())
                .toList();
    }

//    private String serializeCommand(RaftCommand cmd) {
//        // Simple format: "TYPE:WORD:VALUE"
//        return String.format("%s:%s:%d", cmd.type(), cmd.word(), cmd.value());
//    }

    public synchronized void takeSnapshot() {
        long lastIndex = getLastLogIndex();
        long lastTerm = getLastLogTerm();

        try {
            // 1. Serialize the current TST state
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this.tst); // Your ThreadSafeWeightedTST must be Serializable
            byte[] tstSnapshot = baos.toByteArray();

            // 2. Save Snapshot to disk
            RaftSnapshot snapshot = new RaftSnapshot(lastIndex, lastTerm, tstSnapshot);
            try (ObjectOutputStream snapshotFile = new ObjectOutputStream(
                    new FileOutputStream(logPath + ".snapshot"))) {
                snapshotFile.writeObject(snapshot);
            }

            // 3. COMPACTION: Discard log entries up to lastIndex
            // In a production system, you'd truncate the actual file.
            // Here, we clear the memory log.
            memoryLog.clear();

            System.out.println("Snapshot taken at index " + lastIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadSnapshotAndLog() {
        File snapFile = new File(logPath + ".snapshot");
        if (snapFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(snapFile))) {
                RaftSnapshot snap = (RaftSnapshot) ois.readObject();

                // Reconstruct TST from byte array
                ObjectInputStream tstStream = new ObjectInputStream(new ByteArrayInputStream(snap.tstData()));
                this.tst = (ThreadSafeWeightedTST) tstStream.readObject();

                this.lastIncludedIndex = snap.lastIncludedIndex();
                this.lastIncludedTerm = snap.lastIncludedTerm();
                System.out.println("Loaded snapshot at index " + lastIncludedIndex);
            } catch (Exception e) { e.printStackTrace(); }
        }
        // After loading snapshot, replay any NEW log entries that came after it
        replayLog();
    }

    public synchronized void applySnapshot(long lastIndex, long lastTerm, byte[] data) {
        // 1. Safety Check: If our current snapshot is already newer, ignore this
        if (lastIndex <= this.lastIncludedIndex) {
            return;
        }

        try {
            // 2. Deserialize the TST from the leader's data
            ObjectInputStream tstStream = new ObjectInputStream(new ByteArrayInputStream(data));
            this.tst = (ThreadSafeWeightedTST) tstStream.readObject();

            // 3. Update persistent metadata
            this.lastIncludedIndex = lastIndex;
            this.lastIncludedTerm = lastTerm;

            // 4. Save to local .snapshot file so it survives a restart
            RaftSnapshot snapshot = new RaftSnapshot(lastIndex, lastTerm, data);
            try (ObjectOutputStream snapshotFile = new ObjectOutputStream(
                    new FileOutputStream(logPath + ".snapshot"))) {
                snapshotFile.writeObject(snapshot);
            }

            // 5. COMPACTION: Clear memory log up to this index
            // If we had logs beyond the snapshot, we keep them; otherwise, clear all.
            if (getLastLogIndex() <= lastIndex) {
                memoryLog.clear();
            } else {
                // Keep the "suffix" of the log that came after the snapshot point
                int toRemove = (int) (lastIndex - (lastIncludedIndex - memoryLog.size()));
                if (toRemove > 0 && toRemove <= memoryLog.size()) {
                    memoryLog.subList(0, toRemove).clear();
                }
            }

            System.out.println("Applied leader snapshot at index " + lastIndex);
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply snapshot from leader!", e);
        }
    }

    // Inside RaftLogManager.java
    public void applyToStateMachine(RaftCommand cmd) {
        if ("BOOST".equals(cmd.type())) {
            tst.incrementWeight(cmd.word());
        } else if ("INSERT".equals(cmd.type())) {
            tst.insert(cmd.word(), cmd.value(), "");
        }
    }

    public RaftCommand getCommandAtIndex(long index) {
        int internalIndex = (int) (index - lastIncludedIndex - 1);
        if (internalIndex >= 0 && internalIndex < memoryLog.size()) {
            return memoryLog.get(internalIndex);
        }
        return null;
    }


}

