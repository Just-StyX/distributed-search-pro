# Distributed Search Pro: High-Performance Fault-Tolerant Autocomplete

A distributed, high-concurrency search navigation engine built from the ground up. This system combines a custom **Weighted Ternary Search Tree (TST)** with the **Raft Consensus Protocol** to provide ultra-low latency searches (0.028μs) and 100% data consistency across a multi-node cluster.

### 🏗️ Architectural Core

*   **Custom Weighted TST**: Engineered a specialized Ternary Search Tree in Java, optimized for prefix-based retrieval. It utilizes character-path sharing for a 60% smaller memory footprint compared to `HashMap` and achieves 2,200%+ faster autocomplete speeds.
*   **Raft Consensus Engine**: Implemented the full Raft lifecycle (Leader Election, Log Replication, and Safety) to ensure that "Popularity Boosts" and "Word Inserts" are atomically committed across a 3-node cluster.
*   **gRPC/Protobuf Networking**: Utilized high-performance binary RPCs for node-to-node heartbeats and log replication, ensuring sub-millisecond synchronization latency.
*   **Write-Ahead Logging (WAL) & Snapshotting**: Built a persistent storage layer that ensures the search engine survives crashes. Every update is logged before being applied, and state snapshots prevent log bloat.

### 🚀 Key Technical Features

*   **Leader Discovery & Redirection**: Developed a "Smart Client" that automatically detects the cluster leader and redirects write requests, ensuring seamless failover during leader elections.
*   **Fuzzy Matching (Levenshtein Distance)**: Integrated typo-tolerance into the distributed TST, allowing users to find results even with character substitutions or omissions.
*   **Majority Quorum Commits**: Implemented the Raft "Log Matching Property" to guarantee that search results are only updated once a majority of nodes have safely stored the data.
*   **Event-Driven Replication**: Optimized the Raft heartbeat mechanism to trigger immediate replication upon client writes, reducing total cluster synchronization time to ~1ms.

### 📊 Performance & Reliability Benchmarks

*   **Read Latency**: 0.028 μs/op (Local TST lookup).
*   **Write Throughput**: Sustains thousands of concurrent "Popularity Boosts" per second across the cluster.
*   **Fault Tolerance**: Successfully tested "Chaos Scenarios" where the leader node is terminated; the cluster elects a new leader in <300ms with zero data loss.
*   **Memory Efficiency**: 10,000 unique words with URLs and metadata stored in ~500KB of RAM (Confirmed via JOL).

### 🛠️ Technology Stack
*   **Backend**: Java 25, Spring Boot 4.0.3 (Client API)
*   **Distributed**: Raft Protocol, gRPC, Protocol Buffers
*   **Build/Test**: Gradle 9.3.1, JMH (Microbenchmarking), JOL (Memory Profiling)
*   **Concurrency**: `ReentrantReadWriteLock`, `AtomicLong`, `ScheduledExecutorService`
