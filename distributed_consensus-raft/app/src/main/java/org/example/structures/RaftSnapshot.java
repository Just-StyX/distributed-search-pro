package org.example.structures;

import java.io.Serializable;

public record RaftSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] tstData) implements Serializable {}

