package org.example.structures;

import java.io.Serializable;

public record RaftCommand(String type, String word, int value, long term, String url) implements Serializable {

    // The Leader calls this when a user clicks "Boost" in the UI
    public static RaftCommand boost(String word, long currentTerm) {
        return new RaftCommand("BOOST", word, 1, currentTerm, "");
    }

    // The Leader calls this when a new word is first added to the TST
    public static RaftCommand insert(String word, int weight, long currentTerm) {
        return new RaftCommand("INSERT", word, weight, currentTerm, "");
    }
}

