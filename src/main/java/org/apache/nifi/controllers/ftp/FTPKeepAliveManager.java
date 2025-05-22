package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages keep-alive operations for FTP connections to prevent them from being
 * closed due to inactivity by the server.
 */
public class FTPKeepAliveManager {
    private final FTPConnectionHealthManager healthManager;
    private final ComponentLog logger;
    
    // Keep-alive tracking
    private final ConcurrentHashMap<String, KeepAliveInfo> connectionKeepAliveMap = new ConcurrentHashMap<>();
    
    // Metrics
    private final AtomicLong totalKeepAlivesSent = new AtomicLong(0);
    private final AtomicLong successfulKeepAlivesSent = new AtomicLong(0);
    private final AtomicLong failedKeepAlivesSent = new AtomicLong(0);
    
    // Scheduler for keep-alive tasks
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> keepAliveTask;
    private final AtomicBoolean isRunningKeepAlive = new AtomicBoolean(false);
    private final AtomicLong keepAliveCycleCount = new AtomicLong(0);
    private long keepAliveIntervalMs;
    private volatile boolean shutdown = false;
    
    /**
     * Inner class to track keep-alive information for a connection.
     */
    private static class KeepAliveInfo {
        private final String connectionId;
        private Instant lastActivity;
        private Instant lastKeepAliveSent;
        private final AtomicLong totalKeepAlivesSent = new AtomicLong(0);
        private final AtomicLong successfulKeepAlivesSent = new AtomicLong(0);
        private final AtomicLong failedKeepAlivesSent = new AtomicLong(0);
        
        public KeepAliveInfo(String connectionId) {
            this.connectionId = connectionId;
            this.lastActivity = Instant.now();
        }
        
        public String getConnectionId() {
            return connectionId;
        }
        
        public Instant getLastActivity() {
            return lastActivity;
        }
        
        public void setLastActivity(Instant lastActivity) {
            this.lastActivity = lastActivity;
        }
        
        public void markActivity() {
            this.lastActivity = Instant.now();
        }
        
        public Instant getLastKeepAliveSent() {
            return lastKeepAliveSent;
        }
        
        public void setLastKeepAliveSent(Instant lastKeepAliveSent) {
            this.lastKeepAliveSent = lastKeepAliveSent;
        }
        
        public void incrementTotalKeepAlivesSent() {
            totalKeepAlivesSent.incrementAndGet();
        }
        
        public long getTotalKeepAlivesSent() {
            return totalKeepAlivesSent.get();
        }
        
        public void incrementSuccessfulKeepAlivesSent() {
            successfulKeepAlivesSent.incrementAndGet();
        }
        
        public long getSuccessfulKeepAlivesSent() {
            return successfulKeepAlivesSent.get();
        }
        
        public void incrementFailedKeepAlivesSent() {
            failedKeepAlivesSent.incrementAndGet();
        }
        
        public long getFailedKeepAlivesSent() {
            return failedKeepAlivesSent.get();
        }
        
        public Duration getIdleTime() {
            return Duration.between(lastActivity, Instant.now());
        }
        
        public Map<String, Object> toMap() {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("connectionId", connectionId);
            map.put("lastActivity", lastActivity.toString());
            map.put("idleTimeMs", getIdleTime().toMillis());
            
            if (lastKeepAliveSent != null) {
                map.put("lastKeepAliveSent", lastKeepAliveSent.toString());
                map.put("timeSinceLastKeepAliveMs", 
                        Duration.between(lastKeepAliveSent, Instant.now()).toMillis());
            }
            
            map.put("totalKeepAlivesSent", totalKeepAlivesSent.get());
            map.put("successfulKeepAlivesSent", successfulKeepAlivesSent.get());
            map.put("failedKeepAlivesSent", failedKeepAlivesSent.get());
            
            return map;
        }
    }
    
    /**
     * Creates a new FTPKeepAliveManager with the given health manager, logger, and interval.
     *
     * @param healthManager the health manager to use for connection testing
     * @param logger the logger to use
     * @param keepAliveIntervalMs the interval between keep-alive operations in milliseconds
     */
    public FTPKeepAliveManager(FTPConnectionHealthManager healthManager, ComponentLog logger,
                              long keepAliveIntervalMs) {
        this.healthManager = healthManager;
        this.logger = logger;
        this.keepAliveIntervalMs = keepAliveIntervalMs;
        
        // Create a scheduler for keep-alive tasks
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("FTP-Keep-Alive-Manager");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Starts the keep-alive management.
     */
    public void startKeepAlive() {
        // Only start if not already running
        if (keepAliveTask != null && !keepAliveTask.isDone()) {
            logger.debug("Keep-alive already started, not starting again");
            return;
        }
        
        logger.info("Starting FTP connection keep-alive with interval: {} ms", keepAliveIntervalMs);
        
        // Schedule the keep-alive task
        this.keepAliveTask = scheduler.scheduleAtFixedRate(
            this::performKeepAliveCycle,
            keepAliveIntervalMs,
            keepAliveIntervalMs,
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * Stops the keep-alive management.
     */
    public void stopKeepAlive() {
        if (keepAliveTask != null) {
            keepAliveTask.cancel(false);
            keepAliveTask = null;
            logger.info("Stopped FTP connection keep-alive");
        }
    }
    
    /**
     * Registers a connection for keep-alive management.
     *
     * @param connection the connection to register
     */
    public void registerConnection(FTPConnection connection) {
        if (connection == null) {
            return;
        }
        
        // Create or update keep-alive info
        KeepAliveInfo keepAliveInfo = connectionKeepAliveMap.computeIfAbsent(
                connection.getId(), id -> new KeepAliveInfo(id));
        
        // Mark current activity
        keepAliveInfo.markActivity();
        
        logger.debug("Registered connection for keep-alive: {}", connection.getId());
    }
    
    /**
     * Unregisters a connection from keep-alive management.
     *
     * @param connection the connection to unregister
     */
    public void unregisterConnection(FTPConnection connection) {
        if (connection == null) {
            return;
        }
        
        // Remove from tracking
        connectionKeepAliveMap.remove(connection.getId());
        
        logger.debug("Unregistered connection from keep-alive: {}", connection.getId());
    }
    
    /**
     * Records activity on a connection to reset its idle timer.
     *
     * @param connection the connection that had activity
     */
    public void recordActivity(FTPConnection connection) {
        if (connection == null) {
            return;
        }
        
        // Update last activity time
        KeepAliveInfo keepAliveInfo = connectionKeepAliveMap.get(connection.getId());
        if (keepAliveInfo != null) {
            keepAliveInfo.markActivity();
        }
    }
    
    /**
     * Performs a keep-alive operation on a specific connection if it needs one.
     *
     * @param connection the connection to keep alive
     * @param forceKeepAlive whether to force a keep-alive even if the connection is not idle
     * @return true if keep-alive was successful or not needed, false if it failed
     */
    public boolean performKeepAlive(FTPConnection connection, boolean forceKeepAlive) {
        if (connection == null) {
            return false;
        }
        
        // Get keep-alive info
        KeepAliveInfo keepAliveInfo = connectionKeepAliveMap.get(connection.getId());
        if (keepAliveInfo == null) {
            // Not registered for keep-alive
            return true;
        }
        
        // Check if we need to send a keep-alive
        Duration idleTime = keepAliveInfo.getIdleTime();
        boolean needsKeepAlive = idleTime.toMillis() >= keepAliveIntervalMs / 2;
        
        if (!needsKeepAlive && !forceKeepAlive) {
            // No need for keep-alive
            return true;
        }
        
        // Update metrics
        totalKeepAlivesSent.incrementAndGet();
        keepAliveInfo.incrementTotalKeepAlivesSent();
        keepAliveInfo.setLastKeepAliveSent(Instant.now());
        
        // Use the health manager to send a keep-alive
        try {
            boolean result = healthManager.keepAlive(connection);
            
            if (result) {
                // Success
                successfulKeepAlivesSent.incrementAndGet();
                keepAliveInfo.incrementSuccessfulKeepAlivesSent();
                keepAliveInfo.markActivity(); // Reset idle timer
                
                logger.debug("Keep-alive successful for connection: {}", connection.getId());
                return true;
            } else {
                // Failed
                failedKeepAlivesSent.incrementAndGet();
                keepAliveInfo.incrementFailedKeepAlivesSent();
                
                logger.warn("Keep-alive failed for connection: {}", connection.getId());
                return false;
            }
        } catch (Exception e) {
            // Failed with exception
            failedKeepAlivesSent.incrementAndGet();
            keepAliveInfo.incrementFailedKeepAlivesSent();
            
            logger.warn("Keep-alive failed with exception for connection {}: {}", 
                    connection.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Sends a manual keep-alive command to an FTP client.
     *
     * @param client the FTP client to send a keep-alive to
     * @return true if the keep-alive was successful, false otherwise
     */
    public boolean sendManualKeepAlive(FTPClient client) {
        if (client == null) {
            return false;
        }
        
        totalKeepAlivesSent.incrementAndGet();
        
        try {
            // Send a NOOP command to keep the connection alive
            boolean result = client.sendNoOp();
            
            if (result) {
                // Success
                successfulKeepAlivesSent.incrementAndGet();
                logger.debug("Manual keep-alive successful for FTP client");
                return true;
            } else {
                // Failed
                failedKeepAlivesSent.incrementAndGet();
                logger.warn("Manual keep-alive failed for FTP client with reply code: {}", 
                        client.getReplyCode());
                return false;
            }
        } catch (IOException e) {
            // Failed with exception
            failedKeepAlivesSent.incrementAndGet();
            logger.warn("Manual keep-alive failed with exception: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Performs a keep-alive cycle on all registered connections.
     */
    private void performKeepAliveCycle() {
        if (shutdown) {
            logger.debug("Not performing keep-alive cycle as manager is shut down");
            return;
        }
        
        // Ensure we don't have two cycles running simultaneously
        if (!isRunningKeepAlive.compareAndSet(false, true)) {
            logger.debug("Keep-alive cycle already running, skipping");
            return;
        }
        
        try {
            // Update metrics
            keepAliveCycleCount.incrementAndGet();
            
            logger.debug("Starting connection keep-alive cycle");
            
            // Check all registered connections
            int total = 0;
            int successful = 0;
            int failed = 0;
            int skipped = 0;
            
            for (Map.Entry<String, KeepAliveInfo> entry : connectionKeepAliveMap.entrySet()) {
                String connectionId = entry.getKey();
                KeepAliveInfo keepAliveInfo = entry.getValue();
                
                // Check if connection needs keep-alive
                Duration idleTime = keepAliveInfo.getIdleTime();
                if (idleTime.toMillis() < keepAliveIntervalMs / 2) {
                    // Connection is not idle enough, skip
                    skipped++;
                    continue;
                }
                
                // Find connection object
                FTPConnection connection = findConnection(connectionId);
                if (connection == null) {
                    // Connection not found, skip
                    skipped++;
                    continue;
                }
                
                // Skip connections that are in transitional or failed states
                if (connection.getState().isTransitional() || connection.getState().isFailed()) {
                    logger.debug("Skipping keep-alive for connection {} in state: {}", 
                            connectionId, connection.getState());
                    skipped++;
                    continue;
                }
                
                // Perform keep-alive
                total++;
                
                try {
                    boolean result = performKeepAlive(connection, false);
                    if (result) {
                        successful++;
                    } else {
                        failed++;
                    }
                } catch (Exception e) {
                    failed++;
                    logger.warn("Error during keep-alive for connection {}: {}", 
                            connectionId, e.getMessage());
                }
            }
            
            logger.debug("Completed keep-alive cycle: total={}, successful={}, failed={}, skipped={}",
                    total, successful, failed, skipped);
            
        } catch (Exception e) {
            logger.error("Error during keep-alive cycle: {}", e.getMessage(), e);
        } finally {
            isRunningKeepAlive.set(false);
        }
    }
    
    /**
     * Finds a connection by ID in the connection manager.
     */
    private FTPConnection findConnection(String connectionId) {
        // We don't have direct access to the connection manager's connection map
        // In a real implementation, there would be a method to find a connection by ID
        // For this example, we'll return null
        return null;
    }
    
    /**
     * Gets the keep-alive metrics.
     *
     * @return a map of keep-alive metrics
     */
    public Map<String, Object> getKeepAliveMetrics() {
        Map<String, Object> metrics = new java.util.HashMap<>();
        
        // Global metrics
        metrics.put("totalKeepAlivesSent", totalKeepAlivesSent.get());
        metrics.put("successfulKeepAlivesSent", successfulKeepAlivesSent.get());
        metrics.put("failedKeepAlivesSent", failedKeepAlivesSent.get());
        metrics.put("keepAliveCycleCount", keepAliveCycleCount.get());
        metrics.put("keepAliveIntervalMs", keepAliveIntervalMs);
        metrics.put("isRunningKeepAlive", isRunningKeepAlive.get());
        metrics.put("registeredConnectionCount", connectionKeepAliveMap.size());
        
        // Connection-specific metrics
        Map<String, Object> connectionMetrics = new java.util.HashMap<>();
        for (Map.Entry<String, KeepAliveInfo> entry : connectionKeepAliveMap.entrySet()) {
            connectionMetrics.put(entry.getKey(), entry.getValue().toMap());
        }
        metrics.put("connectionMetrics", connectionMetrics);
        
        return metrics;
    }
    
    /**
     * Sets the keep-alive interval.
     *
     * @param keepAliveIntervalMs the new keep-alive interval in milliseconds
     */
    public void setKeepAliveInterval(long keepAliveIntervalMs) {
        this.keepAliveIntervalMs = keepAliveIntervalMs;
        
        // Restart keep-alive with new interval if already running
        if (keepAliveTask != null && !keepAliveTask.isDone()) {
            stopKeepAlive();
            startKeepAlive();
        }
    }
    
    /**
     * Shuts down the keep-alive manager.
     */
    public void shutdown() {
        // Set shutdown flag
        shutdown = true;
        
        // Stop keep-alive
        stopKeepAlive();
        
        // Shutdown scheduler
        if (scheduler != null) {
            try {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
        
        // Clear tracking maps
        connectionKeepAliveMap.clear();
        
        logger.info("FTP Keep-Alive Manager shut down");
    }
}