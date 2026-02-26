package org.example.structures;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThreadSafeWeightedTST implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static class Node implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        char data;
        Node left, mid, right;
        int weight = -1;
        int maxWeight = 0;
        String url; // The navigation link

        Node(char data) { this.data = data; }
    }

    private static class ScoredResult implements Comparable<ScoredResult> {
        String word;
        String url;
        int distance;
        int weight;

        ScoredResult(String word, String url, int distance, int weight) {
            this.word = word;
            this.url = url;
            this.distance = distance;
            this.weight = weight;
        }

        @Override
        public int compareTo(ScoredResult other) {
            if (this.distance != other.distance) {
                return Integer.compare(other.distance, this.distance);
            }
            return Integer.compare(this.weight, other.weight);
        }
    }

    private Node root;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Updated Insert to include URL
    public void insert(String word, int weight, String url) {
        if (word == null || word.isEmpty()) return;
        lock.writeLock().lock();
        try {
            root = insert(root, word, 0, weight, url);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Node insert(Node node, String word, int index, int weight, String url) {
        char c = word.charAt(index);
        if (node == null) node = new Node(c);

        node.maxWeight = Math.max(node.maxWeight, weight);

        if (c < node.data) {
            node.left = insert(node.left, word, index, weight, url);
        } else if (c > node.data) {
            node.right = insert(node.right, word, index, weight, url);
        } else if (index < word.length() - 1) {
            node.mid = insert(node.mid, word, index + 1, weight, url);
        } else {
            node.weight = weight;
            node.url = url; // Save the URL here
        }
        return node;
    }

    // Updated Autocomplete to return SearchResult objects
    public List<SearchResult> autocomplete(String prefix, int limit) {
        lock.readLock().lock();
        try {
            Node prefixNode = find(root, prefix, 0);
            if (prefixNode == null) return Collections.emptyList();

            List<SearchResult> results = new ArrayList<>();
            if (prefixNode.weight != -1) {
                results.add(new SearchResult(prefix, prefixNode.url));
            }

            collectWords(prefixNode.mid, new StringBuilder(prefix), results, limit);
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void collectWords(Node node, StringBuilder path, List<SearchResult> results, int limit) {
        if (node == null || results.size() >= limit) return;
        collectWords(node.left, path, results, limit);
        path.append(node.data);
        if (node.weight != -1) {
            results.add(new SearchResult(path.toString(), node.url));
        }
        if (results.size() < limit) collectWords(node.mid, path, results, limit);
        path.setLength(path.length() - 1);
        if (results.size() < limit) collectWords(node.right, path, results, limit);
    }

    // Boost logic corrected to handle current node weight in maxWeight calc
    public void incrementWeight(String word) {
        lock.writeLock().lock();
        try {
            root = boostRecursive(root, word, 0);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Node boostRecursive(Node node, String word, int index) {
        if (node == null) return null;
        char c = word.charAt(index);
        if (c < node.data) {
            node.left = boostRecursive(node.left, word, index);
        } else if (c > node.data) {
            node.right = boostRecursive(node.right, word, index);
        } else if (index < word.length() - 1) {
            node.mid = boostRecursive(node.mid, word, index + 1);
        } else {
            if (node.weight != -1) node.weight++;
        }

        int currentMax = Math.max(0, node.weight);
        if (node.left != null) currentMax = Math.max(currentMax, node.left.maxWeight);
        if (node.mid != null) currentMax = Math.max(currentMax, node.mid.maxWeight);
        if (node.right != null) currentMax = Math.max(currentMax, node.right.maxWeight);
        node.maxWeight = currentMax;
        return node;
    }

    // Standard recursive find for the prefix
    private Node find(Node node, String word, int index) {
        if (node == null) return null;
        char c = word.charAt(index);
        if (c < node.data) return find(node.left, word, index);
        if (c > node.data) return find(node.right, word, index);
        if (index < word.length() - 1) return find(node.mid, word, index + 1);
        return node;
    }

    // Fuzzy search updated to pass URL into ScoredResult
    public List<SearchResult> fuzzySearch(String query, int maxDistance, int k) {
        lock.readLock().lock();
        try {
            PriorityQueue<ScoredResult> pq = new PriorityQueue<>(k);
            fuzzySearchRecursive(root, query, 0, maxDistance, new StringBuilder(), pq, k);
            LinkedList<SearchResult> results = new LinkedList<>();
            while (!pq.isEmpty()) {
                ScoredResult sr = pq.poll();
                results.addFirst(new SearchResult(sr.word, sr.url));
            }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void fuzzySearchRecursive(Node node, String query, int index, int dist,
                                      StringBuilder path, PriorityQueue<ScoredResult> pq, int k) {
        if (node == null || dist < 0) return;
        fuzzySearchRecursive(node.left, query, index, dist, path, pq, k);
        fuzzySearchRecursive(node.right, query, index, dist, path, pq, k);

        int nextDist = (index < query.length() && query.charAt(index) == node.data) ? dist : dist - 1;
        path.append(node.data);
        if (node.weight != -1) {
            int finalDist = nextDist - (query.length() - 1 - index);
            if (finalDist >= 0) {
                updateTopK(pq, path.toString(), node.url, finalDist, node.weight, k);
            }
        }
        fuzzySearchRecursive(node.mid, query, index + 1, nextDist, path, pq, k);
        fuzzySearchRecursive(node, query, index + 1, dist - 1, path, pq, k);
        path.setLength(path.length() - 1);
    }

    private void updateTopK(PriorityQueue<ScoredResult> pq, String word, String url, int dist, int weight, int k) {
        ScoredResult newResult = new ScoredResult(word, url, dist, weight);
        if (pq.size() < k) {
            pq.offer(newResult);
        } else {
            assert pq.peek() != null;
            if (newResult.compareTo(pq.peek()) > 0) {
                pq.poll();
                pq.offer(newResult);
            }
        }
    }

    public Map<String, TstEntry> getAllWordsWithDetails() {
        lock.readLock().lock();
        try {
            Map<String, TstEntry> wordMap = new HashMap<>();
            collectAllWithDetails(root, new StringBuilder(), wordMap);
            return wordMap;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void collectAllWithDetails(Node node, StringBuilder path, Map<String, TstEntry> results) {
        if (node == null) return;
        collectAllWithDetails(node.left, path, results);
        path.append(node.data);
        if (node.weight != -1) {
            results.put(path.toString(), new TstEntry(node.weight, node.url));
        }
        collectAllWithDetails(node.mid, path, results);
        path.setLength(path.length() - 1);
        collectAllWithDetails(node.right, path, results);
    }
}
