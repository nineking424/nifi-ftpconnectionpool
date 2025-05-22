package org.apache.nifi.controllers.ftp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

/**
 * Collects and manages metrics for the FTP connection pool and operations.
 * This includes connection statistics, performance metrics, and throughput measurements.
 */
public class FTPConnectionMetrics {
    
    // Connection pool metrics
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger idleConnections = new AtomicInteger(0);
    private final AtomicInteger failedConnections = new AtomicInteger(0);
    
    // Connection lifecycle metrics
    private final AtomicLong connectionsCreated = new AtomicLong(0);
    private final AtomicLong connectionsDestroyed = new AtomicLong(0);
    private final AtomicLong connectionsReused = new AtomicLong(0);
    private final AtomicLong connectionValidations = new AtomicLong(0);
    private final AtomicLong connectionInvalidations = new AtomicLong(0);
    private final AtomicLong connectionResets = new AtomicLong(0);
    
    // Performance metrics
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong successfulOperations = new AtomicLong(0);
    private final AtomicLong failedOperations = new AtomicLong(0);
    private final AtomicLong totalOperationTimeNanos = new AtomicLong(0);
    private final AtomicLong maxOperationTimeNanos = new AtomicLong(0);
    private final AtomicLong minOperationTimeNanos = new AtomicLong(Long.MAX_VALUE);
    
    // Throughput metrics
    private final AtomicLong bytesUploaded = new AtomicLong(0);
    private final AtomicLong bytesDownloaded = new AtomicLong(0);
    private final AtomicLong filesUploaded = new AtomicLong(0);
    private final AtomicLong filesDownloaded = new AtomicLong(0);
    private final AtomicLong directoryListings = new AtomicLong(0);
    
    // Queue metrics
    private final AtomicInteger queuedOperations = new AtomicInteger(0);
    private final AtomicLong totalQueueTimeNanos = new AtomicLong(0);
    private final AtomicLong maxQueueTimeNanos = new AtomicLong(0);
    
    // Wait time metrics
    private final AtomicLong totalWaitTimeForConnectionNanos = new AtomicLong(0);
    private final AtomicLong maxWaitTimeForConnectionNanos = new AtomicLong(0);
    private final AtomicLong connectionWaitCount = new AtomicLong(0);
    
    // Memory and resources metrics
    private final AtomicLong totalMemoryUsedBytes = new AtomicLong(0);
    private final AtomicLong peakMemoryUsedBytes = new AtomicLong(0);
    
    // Thread metrics
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger poolThreads = new AtomicInteger(0);
    
    // Health metrics
    private final AtomicLong lastSuccessfulOperationTimeMillis = new AtomicLong(0);
    private final AtomicLong lastFailedOperationTimeMillis = new AtomicLong(0);
    private volatile String lastOperationError = null;
    
    // Rate tracking for throughput
    private final long[] uploadRateWindows = new long[60]; // 1-minute window in seconds
    private final long[] downloadRateWindows = new long[60]; // 1-minute window in seconds
    private int currentRateIndex = 0;
    private long lastRateUpdateTimeMillis = System.currentTimeMillis();
    
    // Metrics by operation type
    private final Map<String, OperationMetrics> operationMetricsMap = new ConcurrentHashMap<>();
    
    // Custom metrics for specific monitoring needs
    private final Map<String, AtomicLong> customCounterMetrics = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> customGaugeMetrics = new ConcurrentHashMap<>();
    
    // Recent operations tracking
    private static final int MAX_RECENT_OPERATIONS = 100;
    private final OperationRecord[] recentOperations = new OperationRecord[MAX_RECENT_OPERATIONS];
    private int recentOperationIndex = 0;
    
    /**
     * Creates a new metrics collection instance.
     */
    public FTPConnectionMetrics() {
        // Initialize rate tracking windows
        long now = System.currentTimeMillis();
        for (int i = 0; i < uploadRateWindows.length; i++) {
            uploadRateWindows[i] = 0;
            downloadRateWindows[i] = 0;
        }
        lastRateUpdateTimeMillis = now;
        
        // Initialize recent operations
        for (int i = 0; i < MAX_RECENT_OPERATIONS; i++) {
            recentOperations[i] = null;
        }
    }
    
    /**
     * Records a new connection being created.
     */
    public void recordConnectionCreated() {
        connectionsCreated.incrementAndGet();
        totalConnections.incrementAndGet();
    }
    
    /**
     * Records a connection being destroyed.
     */
    public void recordConnectionDestroyed() {
        connectionsDestroyed.incrementAndGet();
        totalConnections.decrementAndGet();
    }
    
    /**
     * Updates the count of active connections.
     *
     * @param active The number of active connections
     * @param idle The number of idle connections
     * @param failed The number of failed connections
     */
    public void updateConnectionCounts(int active, int idle, int failed) {
        activeConnections.set(active);
        idleConnections.set(idle);
        failedConnections.set(failed);
    }
    
    /**
     * Records a connection being reused from the pool.
     */
    public void recordConnectionReused() {
        connectionsReused.incrementAndGet();
    }
    
    /**
     * Records a connection being validated.
     *
     * @param valid Whether the connection was valid
     */
    public void recordConnectionValidation(boolean valid) {
        connectionValidations.incrementAndGet();
        if (!valid) {
            connectionInvalidations.incrementAndGet();
        }
    }
    
    /**
     * Records a connection being reset.
     */
    public void recordConnectionReset() {
        connectionResets.incrementAndGet();
    }
    
    /**
     * Records the time spent waiting for a connection.
     *
     * @param waitTimeNanos The wait time in nanoseconds
     */
    public void recordConnectionWaitTime(long waitTimeNanos) {
        connectionWaitCount.incrementAndGet();
        totalWaitTimeForConnectionNanos.addAndGet(waitTimeNanos);
        
        // Update max wait time if this wait was longer
        while (true) {
            long current = maxWaitTimeForConnectionNanos.get();
            if (waitTimeNanos <= current || 
                maxWaitTimeForConnectionNanos.compareAndSet(current, waitTimeNanos)) {
                break;
            }
        }
    }
    
    /**
     * Records an operation being queued.
     */
    public void recordOperationQueued() {
        queuedOperations.incrementAndGet();
    }
    
    /**
     * Records an operation being dequeued.
     *
     * @param queueTimeNanos The time the operation spent in the queue in nanoseconds
     */
    public void recordOperationDequeued(long queueTimeNanos) {
        queuedOperations.decrementAndGet();
        totalQueueTimeNanos.addAndGet(queueTimeNanos);
        
        // Update max queue time if this queue time was longer
        while (true) {
            long current = maxQueueTimeNanos.get();
            if (queueTimeNanos <= current || 
                maxQueueTimeNanos.compareAndSet(current, queueTimeNanos)) {
                break;
            }
        }
    }
    
    /**
     * Starts tracking an operation.
     *
     * @param operationType The type of operation
     * @param operationDetails Additional details about the operation
     * @return An operation tracker object
     */
    public OperationTracker startOperation(String operationType, String operationDetails) {
        return new OperationTracker(operationType, operationDetails);
    }
    
    /**
     * Records bytes being uploaded.
     *
     * @param bytes The number of bytes uploaded
     */
    public void recordBytesUploaded(long bytes) {
        bytesUploaded.addAndGet(bytes);
        
        // Update rate window
        updateRateWindows();
        uploadRateWindows[currentRateIndex] += bytes;
    }
    
    /**
     * Records bytes being downloaded.
     *
     * @param bytes The number of bytes downloaded
     */
    public void recordBytesDownloaded(long bytes) {
        bytesDownloaded.addAndGet(bytes);
        
        // Update rate window
        updateRateWindows();
        downloadRateWindows[currentRateIndex] += bytes;
    }
    
    /**
     * Records a file being uploaded.
     *
     * @param fileSize The size of the file in bytes
     */
    public void recordFileUploaded(long fileSize) {
        filesUploaded.incrementAndGet();
        recordBytesUploaded(fileSize);
    }
    
    /**
     * Records a file being downloaded.
     *
     * @param fileSize The size of the file in bytes
     */
    public void recordFileDownloaded(long fileSize) {
        filesDownloaded.incrementAndGet();
        recordBytesDownloaded(fileSize);
    }
    
    /**
     * Records a directory listing operation.
     */
    public void recordDirectoryListing() {
        directoryListings.incrementAndGet();
    }
    
    /**
     * Updates the current thread counts.
     *
     * @param active The number of active threads
     * @param pool The number of threads in the pool
     */
    public void updateThreadCounts(int active, int pool) {
        activeThreads.set(active);
        poolThreads.set(pool);
    }
    
    /**
     * Updates the memory usage metrics.
     *
     * @param currentUsageBytes The current memory usage in bytes
     */
    public void updateMemoryUsage(long currentUsageBytes) {
        totalMemoryUsedBytes.set(currentUsageBytes);
        
        // Update peak memory if current usage is higher
        while (true) {
            long current = peakMemoryUsedBytes.get();
            if (currentUsageBytes <= current || 
                peakMemoryUsedBytes.compareAndSet(current, currentUsageBytes)) {
                break;
            }
        }
    }
    
    /**
     * Sets or creates a custom counter metric.
     *
     * @param name The name of the metric
     * @param value The value to set
     */
    public void setCustomCounterMetric(String name, long value) {
        customCounterMetrics.computeIfAbsent(name, k -> new AtomicLong(0)).set(value);
    }
    
    /**
     * Increments a custom counter metric.
     *
     * @param name The name of the metric
     * @param delta The amount to increment by
     */
    public void incrementCustomCounterMetric(String name, long delta) {
        customCounterMetrics.computeIfAbsent(name, k -> new AtomicLong(0)).addAndGet(delta);
    }
    
    /**
     * Sets a custom gauge metric.
     *
     * @param name The name of the metric
     * @param value The value to set
     */
    public void setCustomGaugeMetric(String name, long value) {
        customGaugeMetrics.computeIfAbsent(name, k -> new AtomicLong(0)).set(value);
    }
    
    /**
     * Records a completed operation.
     *
     * @param operationType The type of operation
     * @param successful Whether the operation was successful
     * @param durationNanos The duration of the operation in nanoseconds
     * @param error Error message if the operation failed, or null
     */
    private void recordOperation(String operationType, boolean successful, 
            long durationNanos, String error) {
        
        // Update general operation metrics
        totalOperations.incrementAndGet();
        totalOperationTimeNanos.addAndGet(durationNanos);
        
        // Update min/max operation time
        updateMinMaxOperationTime(durationNanos);
        
        // Update success/failure metrics
        if (successful) {
            successfulOperations.incrementAndGet();
            lastSuccessfulOperationTimeMillis.set(System.currentTimeMillis());
        } else {
            failedOperations.incrementAndGet();
            lastFailedOperationTimeMillis.set(System.currentTimeMillis());
            lastOperationError = error;
        }
        
        // Get or create metrics tracker for this operation type
        OperationMetrics opMetrics = operationMetricsMap.computeIfAbsent(
                operationType, k -> new OperationMetrics());
        
        // Update operation type specific metrics
        opMetrics.recordOperation(successful, durationNanos);
        
        // Record in recent operations list
        synchronized (recentOperations) {
            recentOperations[recentOperationIndex] = new OperationRecord(
                    operationType, successful, durationNanos, error);
            recentOperationIndex = (recentOperationIndex + 1) % MAX_RECENT_OPERATIONS;
        }
    }
    
    /**
     * Updates the min and max operation times.
     *
     * @param durationNanos The operation duration in nanoseconds
     */
    private void updateMinMaxOperationTime(long durationNanos) {
        // Update max operation time if this operation took longer
        while (true) {
            long current = maxOperationTimeNanos.get();
            if (durationNanos <= current || 
                maxOperationTimeNanos.compareAndSet(current, durationNanos)) {
                break;
            }
        }
        
        // Update min operation time if this operation was faster
        while (true) {
            long current = minOperationTimeNanos.get();
            if (durationNanos >= current || 
                minOperationTimeNanos.compareAndSet(current, durationNanos)) {
                break;
            }
        }
    }
    
    /**
     * Updates the rate tracking windows based on elapsed time.
     */
    private synchronized void updateRateWindows() {
        long now = System.currentTimeMillis();
        long elapsedSeconds = (now - lastRateUpdateTimeMillis) / 1000;
        
        if (elapsedSeconds > 0) {
            // If more than the window size has elapsed, clear all windows
            if (elapsedSeconds >= uploadRateWindows.length) {
                for (int i = 0; i < uploadRateWindows.length; i++) {
                    uploadRateWindows[i] = 0;
                    downloadRateWindows[i] = 0;
                }
            } else {
                // Otherwise, advance the index and clear the appropriate number of windows
                for (int i = 0; i < elapsedSeconds; i++) {
                    currentRateIndex = (currentRateIndex + 1) % uploadRateWindows.length;
                    uploadRateWindows[currentRateIndex] = 0;
                    downloadRateWindows[currentRateIndex] = 0;
                }
            }
            
            lastRateUpdateTimeMillis = now;
        }
    }
    
    /**
     * Gets the current upload rate in bytes per second.
     *
     * @return The current upload rate
     */
    public long getCurrentUploadRate() {
        updateRateWindows(); // Ensure windows are current
        long totalBytes = 0;
        for (long bytes : uploadRateWindows) {
            totalBytes += bytes;
        }
        return totalBytes / uploadRateWindows.length;
    }
    
    /**
     * Gets the current download rate in bytes per second.
     *
     * @return The current download rate
     */
    public long getCurrentDownloadRate() {
        updateRateWindows(); // Ensure windows are current
        long totalBytes = 0;
        for (long bytes : downloadRateWindows) {
            totalBytes += bytes;
        }
        return totalBytes / downloadRateWindows.length;
    }
    
    /**
     * Gets a snapshot of all metrics.
     *
     * @return A map containing all metrics
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Connection pool metrics
        Map<String, Object> connectionPoolMetrics = new HashMap<>();
        connectionPoolMetrics.put("totalConnections", totalConnections.get());
        connectionPoolMetrics.put("activeConnections", activeConnections.get());
        connectionPoolMetrics.put("idleConnections", idleConnections.get());
        connectionPoolMetrics.put("failedConnections", failedConnections.get());
        connectionPoolMetrics.put("connectionsCreated", connectionsCreated.get());
        connectionPoolMetrics.put("connectionsDestroyed", connectionsDestroyed.get());
        connectionPoolMetrics.put("connectionsReused", connectionsReused.get());
        connectionPoolMetrics.put("connectionValidations", connectionValidations.get());
        connectionPoolMetrics.put("connectionInvalidations", connectionInvalidations.get());
        connectionPoolMetrics.put("connectionResets", connectionResets.get());
        metrics.put("connectionPool", connectionPoolMetrics);
        
        // Performance metrics
        Map<String, Object> performanceMetrics = new HashMap<>();
        performanceMetrics.put("totalOperations", totalOperations.get());
        performanceMetrics.put("successfulOperations", successfulOperations.get());
        performanceMetrics.put("failedOperations", failedOperations.get());
        
        // Calculate average operation time
        long totalOps = totalOperations.get();
        double avgOperationTimeNanos = totalOps > 0 
                ? (double) totalOperationTimeNanos.get() / totalOps 
                : 0.0;
        
        performanceMetrics.put("averageOperationTimeMs", nanosToMillis(avgOperationTimeNanos));
        performanceMetrics.put("maxOperationTimeMs", nanosToMillis(maxOperationTimeNanos.get()));
        performanceMetrics.put("minOperationTimeMs", nanosToMillis(minOperationTimeNanos.get() == Long.MAX_VALUE 
                ? 0 : minOperationTimeNanos.get()));
        metrics.put("performance", performanceMetrics);
        
        // Throughput metrics
        Map<String, Object> throughputMetrics = new HashMap<>();
        throughputMetrics.put("bytesUploaded", bytesUploaded.get());
        throughputMetrics.put("bytesDownloaded", bytesDownloaded.get());
        throughputMetrics.put("filesUploaded", filesUploaded.get());
        throughputMetrics.put("filesDownloaded", filesDownloaded.get());
        throughputMetrics.put("directoryListings", directoryListings.get());
        throughputMetrics.put("uploadRateBytes", getCurrentUploadRate());
        throughputMetrics.put("downloadRateBytes", getCurrentDownloadRate());
        metrics.put("throughput", throughputMetrics);
        
        // Queue metrics
        Map<String, Object> queueMetrics = new HashMap<>();
        queueMetrics.put("queuedOperations", queuedOperations.get());
        
        // Calculate average queue time
        long queueCount = totalOperations.get() - queuedOperations.get();
        double avgQueueTimeNanos = queueCount > 0 
                ? (double) totalQueueTimeNanos.get() / queueCount 
                : 0.0;
        
        queueMetrics.put("averageQueueTimeMs", nanosToMillis(avgQueueTimeNanos));
        queueMetrics.put("maxQueueTimeMs", nanosToMillis(maxQueueTimeNanos.get()));
        metrics.put("queue", queueMetrics);
        
        // Wait time metrics
        Map<String, Object> waitTimeMetrics = new HashMap<>();
        
        // Calculate average wait time
        long waitCount = connectionWaitCount.get();
        double avgWaitTimeNanos = waitCount > 0 
                ? (double) totalWaitTimeForConnectionNanos.get() / waitCount 
                : 0.0;
        
        waitTimeMetrics.put("averageWaitTimeMs", nanosToMillis(avgWaitTimeNanos));
        waitTimeMetrics.put("maxWaitTimeMs", nanosToMillis(maxWaitTimeForConnectionNanos.get()));
        waitTimeMetrics.put("connectionWaitCount", waitCount);
        metrics.put("waitTime", waitTimeMetrics);
        
        // Memory and resource metrics
        Map<String, Object> resourceMetrics = new HashMap<>();
        resourceMetrics.put("currentMemoryUsageBytes", totalMemoryUsedBytes.get());
        resourceMetrics.put("peakMemoryUsageBytes", peakMemoryUsedBytes.get());
        resourceMetrics.put("activeThreads", activeThreads.get());
        resourceMetrics.put("poolThreads", poolThreads.get());
        metrics.put("resources", resourceMetrics);
        
        // Health metrics
        Map<String, Object> healthMetrics = new HashMap<>();
        long lastSuccessTime = lastSuccessfulOperationTimeMillis.get();
        long lastFailTime = lastFailedOperationTimeMillis.get();
        
        healthMetrics.put("lastSuccessfulOperationTime", 
                lastSuccessTime > 0 ? new java.util.Date(lastSuccessTime) : null);
        healthMetrics.put("lastFailedOperationTime", 
                lastFailTime > 0 ? new java.util.Date(lastFailTime) : null);
        healthMetrics.put("lastOperationError", lastOperationError);
        
        // Calculate success rate
        double successRate = totalOps > 0 
                ? (double) successfulOperations.get() / totalOps * 100.0 
                : 0.0;
        healthMetrics.put("successRate", successRate);
        metrics.put("health", healthMetrics);
        
        // Operation type metrics
        Map<String, Object> operationTypeMetrics = new HashMap<>();
        for (Map.Entry<String, OperationMetrics> entry : operationMetricsMap.entrySet()) {
            operationTypeMetrics.put(entry.getKey(), entry.getValue().getMetrics());
        }
        metrics.put("operationTypes", operationTypeMetrics);
        
        // Custom metrics
        Map<String, Object> customMetrics = new HashMap<>();
        for (Map.Entry<String, AtomicLong> entry : customCounterMetrics.entrySet()) {
            customMetrics.put(entry.getKey(), entry.getValue().get());
        }
        for (Map.Entry<String, AtomicLong> entry : customGaugeMetrics.entrySet()) {
            customMetrics.put(entry.getKey(), entry.getValue().get());
        }
        metrics.put("custom", customMetrics);
        
        return metrics;
    }
    
    /**
     * Gets a snapshot of recent operations.
     *
     * @param limit The maximum number of operations to return (up to MAX_RECENT_OPERATIONS)
     * @return An array of recent operations
     */
    public OperationRecord[] getRecentOperations(int limit) {
        int count = Math.min(limit, MAX_RECENT_OPERATIONS);
        OperationRecord[] result = new OperationRecord[count];
        
        synchronized (recentOperations) {
            for (int i = 0; i < count; i++) {
                int index = (recentOperationIndex - i - 1 + MAX_RECENT_OPERATIONS) % MAX_RECENT_OPERATIONS;
                result[i] = recentOperations[index];
                if (result[i] == null) {
                    // We've reached the end of recorded operations
                    OperationRecord[] trimmed = new OperationRecord[i];
                    System.arraycopy(result, 0, trimmed, 0, i);
                    return trimmed;
                }
            }
        }
        
        return result;
    }
    
    /**
     * Resets all metrics to their initial values.
     */
    public void reset() {
        // Reset connection pool metrics
        totalConnections.set(0);
        activeConnections.set(0);
        idleConnections.set(0);
        failedConnections.set(0);
        connectionsCreated.set(0);
        connectionsDestroyed.set(0);
        connectionsReused.set(0);
        connectionValidations.set(0);
        connectionInvalidations.set(0);
        connectionResets.set(0);
        
        // Reset performance metrics
        totalOperations.set(0);
        successfulOperations.set(0);
        failedOperations.set(0);
        totalOperationTimeNanos.set(0);
        maxOperationTimeNanos.set(0);
        minOperationTimeNanos.set(Long.MAX_VALUE);
        
        // Reset throughput metrics
        bytesUploaded.set(0);
        bytesDownloaded.set(0);
        filesUploaded.set(0);
        filesDownloaded.set(0);
        directoryListings.set(0);
        
        // Reset rate windows
        synchronized (this) {
            for (int i = 0; i < uploadRateWindows.length; i++) {
                uploadRateWindows[i] = 0;
                downloadRateWindows[i] = 0;
            }
            currentRateIndex = 0;
            lastRateUpdateTimeMillis = System.currentTimeMillis();
        }
        
        // Reset queue metrics
        queuedOperations.set(0);
        totalQueueTimeNanos.set(0);
        maxQueueTimeNanos.set(0);
        
        // Reset wait time metrics
        totalWaitTimeForConnectionNanos.set(0);
        maxWaitTimeForConnectionNanos.set(0);
        connectionWaitCount.set(0);
        
        // Reset memory and resources metrics
        totalMemoryUsedBytes.set(0);
        peakMemoryUsedBytes.set(0);
        activeThreads.set(0);
        poolThreads.set(0);
        
        // Reset health metrics
        lastSuccessfulOperationTimeMillis.set(0);
        lastFailedOperationTimeMillis.set(0);
        lastOperationError = null;
        
        // Reset operation metrics
        operationMetricsMap.clear();
        
        // Reset custom metrics
        customCounterMetrics.clear();
        customGaugeMetrics.clear();
        
        // Reset recent operations
        synchronized (recentOperations) {
            for (int i = 0; i < MAX_RECENT_OPERATIONS; i++) {
                recentOperations[i] = null;
            }
            recentOperationIndex = 0;
        }
    }
    
    /**
     * Converts nanoseconds to milliseconds.
     *
     * @param nanos The time in nanoseconds
     * @return The time in milliseconds
     */
    private double nanosToMillis(double nanos) {
        return nanos / 1_000_000.0;
    }
    
    /**
     * Converts nanoseconds to milliseconds.
     *
     * @param nanos The time in nanoseconds
     * @return The time in milliseconds
     */
    private double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }
    
    /**
     * Utility class for tracking the metrics of a specific operation type.
     */
    private static class OperationMetrics {
        private final AtomicLong totalOperations = new AtomicLong(0);
        private final AtomicLong successfulOperations = new AtomicLong(0);
        private final AtomicLong failedOperations = new AtomicLong(0);
        private final AtomicLong totalTimeNanos = new AtomicLong(0);
        private final AtomicLong maxTimeNanos = new AtomicLong(0);
        private final AtomicLong minTimeNanos = new AtomicLong(Long.MAX_VALUE);
        
        /**
         * Records an operation.
         *
         * @param successful Whether the operation was successful
         * @param durationNanos The duration of the operation in nanoseconds
         */
        public void recordOperation(boolean successful, long durationNanos) {
            totalOperations.incrementAndGet();
            totalTimeNanos.addAndGet(durationNanos);
            
            if (successful) {
                successfulOperations.incrementAndGet();
            } else {
                failedOperations.incrementAndGet();
            }
            
            // Update max time if this operation took longer
            while (true) {
                long current = maxTimeNanos.get();
                if (durationNanos <= current || 
                    maxTimeNanos.compareAndSet(current, durationNanos)) {
                    break;
                }
            }
            
            // Update min time if this operation was faster
            while (true) {
                long current = minTimeNanos.get();
                if (durationNanos >= current || 
                    minTimeNanos.compareAndSet(current, durationNanos)) {
                    break;
                }
            }
        }
        
        /**
         * Gets a snapshot of the metrics.
         *
         * @return A map containing the metrics
         */
        public Map<String, Object> getMetrics() {
            Map<String, Object> metrics = new HashMap<>();
            
            metrics.put("totalOperations", totalOperations.get());
            metrics.put("successfulOperations", successfulOperations.get());
            metrics.put("failedOperations", failedOperations.get());
            
            // Calculate success rate
            long total = totalOperations.get();
            double successRate = total > 0 
                    ? (double) successfulOperations.get() / total * 100.0 
                    : 0.0;
            metrics.put("successRate", successRate);
            
            // Calculate average time
            double avgTimeNanos = total > 0 
                    ? (double) totalTimeNanos.get() / total 
                    : 0.0;
            metrics.put("averageTimeMs", avgTimeNanos / 1_000_000.0);
            metrics.put("maxTimeMs", maxTimeNanos.get() / 1_000_000.0);
            metrics.put("minTimeMs", minTimeNanos.get() == Long.MAX_VALUE 
                    ? 0 : minTimeNanos.get() / 1_000_000.0);
            
            return metrics;
        }
    }
    
    /**
     * Class for tracking a specific operation's metrics.
     */
    public class OperationTracker implements AutoCloseable {
        private final String operationType;
        private final String operationDetails;
        private final long startTimeNanos;
        private boolean completed = false;
        private boolean successful = false;
        private String error = null;
        
        /**
         * Creates a new operation tracker.
         *
         * @param operationType The type of operation
         * @param operationDetails Additional details about the operation
         */
        public OperationTracker(String operationType, String operationDetails) {
            this.operationType = operationType;
            this.operationDetails = operationDetails;
            this.startTimeNanos = System.nanoTime();
            
            // Record operation as queued
            recordOperationQueued();
        }
        
        /**
         * Marks the operation as successful and completes it.
         */
        public void markSuccess() {
            if (!completed) {
                successful = true;
                complete();
            }
        }
        
        /**
         * Marks the operation as failed and completes it.
         *
         * @param errorMessage The error message
         */
        public void markFailure(String errorMessage) {
            if (!completed) {
                successful = false;
                error = errorMessage;
                complete();
            }
        }
        
        /**
         * Completes the operation and records its metrics.
         */
        private void complete() {
            long endTimeNanos = System.nanoTime();
            long durationNanos = endTimeNanos - startTimeNanos;
            
            // Record operation dequeued
            recordOperationDequeued(durationNanos);
            
            // Record operation completion
            recordOperation(operationType, successful, durationNanos, error);
            
            completed = true;
        }
        
        @Override
        public void close() {
            // Auto-mark as failure if not explicitly completed
            if (!completed) {
                markFailure("Operation not explicitly completed");
            }
        }
    }
    
    /**
     * Record of a recent operation for tracking operational history.
     */
    public static class OperationRecord {
        private final String operationType;
        private final boolean successful;
        private final long durationNanos;
        private final String error;
        private final long timestamp;
        
        /**
         * Creates a new operation record.
         *
         * @param operationType The type of operation
         * @param successful Whether the operation was successful
         * @param durationNanos The duration of the operation in nanoseconds
         * @param error The error message if the operation failed, or null
         */
        public OperationRecord(String operationType, boolean successful, 
                long durationNanos, String error) {
            this.operationType = operationType;
            this.successful = successful;
            this.durationNanos = durationNanos;
            this.error = error;
            this.timestamp = System.currentTimeMillis();
        }
        
        /**
         * Gets the operation type.
         *
         * @return The operation type
         */
        public String getOperationType() {
            return operationType;
        }
        
        /**
         * Checks if the operation was successful.
         *
         * @return true if the operation was successful, false otherwise
         */
        public boolean isSuccessful() {
            return successful;
        }
        
        /**
         * Gets the duration of the operation in nanoseconds.
         *
         * @return The duration in nanoseconds
         */
        public long getDurationNanos() {
            return durationNanos;
        }
        
        /**
         * Gets the duration of the operation in milliseconds.
         *
         * @return The duration in milliseconds
         */
        public double getDurationMillis() {
            return durationNanos / 1_000_000.0;
        }
        
        /**
         * Gets the error message.
         *
         * @return The error message if the operation failed, or null
         */
        public String getError() {
            return error;
        }
        
        /**
         * Gets the timestamp of the operation.
         *
         * @return The timestamp in milliseconds since epoch
         */
        public long getTimestamp() {
            return timestamp;
        }
        
        /**
         * Gets a map representation of the operation record.
         *
         * @return A map containing the operation record data
         */
        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("operationType", operationType);
            map.put("successful", successful);
            map.put("durationMs", getDurationMillis());
            map.put("error", error);
            map.put("timestamp", new java.util.Date(timestamp));
            return map;
        }
    }
}