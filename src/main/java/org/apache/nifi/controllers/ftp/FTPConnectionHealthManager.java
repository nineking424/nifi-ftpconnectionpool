package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.controllers.ftp.exception.FTPConnectionException;
import org.apache.nifi.controllers.ftp.exception.FTPErrorType;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of FTPConnectionHealth that provides comprehensive connection
 * health monitoring and management.
 */
public class FTPConnectionHealthManager implements FTPConnectionHealth {
    private final FTPConnectionManager connectionManager;
    private final ComponentLog logger;
    
    // Health status tracking for connections
    private final ConcurrentHashMap<String, ConnectionHealthInfo> connectionHealthMap = new ConcurrentHashMap<>();
    
    // Global health status tracking
    private final AtomicLong lastHealthCheckTime = new AtomicLong(0);
    private final AtomicLong totalConnectionTests = new AtomicLong(0);
    private final AtomicLong successfulConnectionTests = new AtomicLong(0);
    private final AtomicLong failedConnectionTests = new AtomicLong(0);
    private final AtomicLong totalConnectionRefreshes = new AtomicLong(0);
    private final AtomicLong successfulConnectionRefreshes = new AtomicLong(0);
    private final AtomicLong failedConnectionRefreshes = new AtomicLong(0);
    private final AtomicLong totalKeepAlivesSent = new AtomicLong(0);
    private final AtomicLong successfulKeepAlivesSent = new AtomicLong(0);
    private final AtomicLong failedKeepAlivesSent = new AtomicLong(0);
    
    // Error history tracking
    private final ConcurrentLinkedQueue<ErrorHistoryEntry> errorHistory = new ConcurrentLinkedQueue<>();
    private final int maxErrorHistorySize;
    
    // Maintenance cycle management
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> maintenanceTask;
    private final AtomicBoolean isRunningMaintenance = new AtomicBoolean(false);
    private final AtomicLong maintenanceCycleCount = new AtomicLong(0);
    private final AtomicLong lastMaintenanceStartTime = new AtomicLong(0);
    private final AtomicLong lastMaintenanceEndTime = new AtomicLong(0);
    private final AtomicLong totalMaintenanceTime = new AtomicLong(0);
    private volatile boolean shutdown = false;
    
    // Configuration parameters
    private final long connectionHealthThresholdMs;
    private final long connectionWarningThresholdMs;
    private final long healthCheckIntervalMs;
    private final long repairRetryIntervalMs;
    private final int maxRepairAttempts;
    
    /**
     * Creates a new FTPConnectionHealthManager with the given connection manager and logger.
     *
     * @param connectionManager the FTP connection manager to monitor
     * @param logger the logger to use
     */
    public FTPConnectionHealthManager(FTPConnectionManager connectionManager, ComponentLog logger) {
        this(connectionManager, logger, 300000, 60000, 60000, 30000, 3, 100);
    }
    
    /**
     * Creates a new FTPConnectionHealthManager with the given connection manager, logger, and configuration.
     *
     * @param connectionManager the FTP connection manager to monitor
     * @param logger the logger to use
     * @param connectionHealthThresholdMs the threshold in milliseconds after which a connection is considered unhealthy
     * @param connectionWarningThresholdMs the threshold in milliseconds after which a connection is considered degraded
     * @param healthCheckIntervalMs the interval in milliseconds between health checks
     * @param repairRetryIntervalMs the interval in milliseconds between repair attempts
     * @param maxRepairAttempts the maximum number of repair attempts
     * @param maxErrorHistorySize the maximum number of error entries to keep in history
     */
    public FTPConnectionHealthManager(FTPConnectionManager connectionManager, ComponentLog logger,
                                      long connectionHealthThresholdMs, long connectionWarningThresholdMs,
                                      long healthCheckIntervalMs, long repairRetryIntervalMs,
                                      int maxRepairAttempts, int maxErrorHistorySize) {
        this.connectionManager = connectionManager;
        this.logger = logger;
        this.connectionHealthThresholdMs = connectionHealthThresholdMs;
        this.connectionWarningThresholdMs = connectionWarningThresholdMs;
        this.healthCheckIntervalMs = healthCheckIntervalMs;
        this.repairRetryIntervalMs = repairRetryIntervalMs;
        this.maxRepairAttempts = maxRepairAttempts;
        this.maxErrorHistorySize = maxErrorHistorySize;
        
        // Create a scheduler for health check tasks
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("FTP-Connection-Health-Manager");
            t.setDaemon(true);
            return t;
        });
        
        // Schedule the maintenance task
        if (healthCheckIntervalMs > 0) {
            this.maintenanceTask = scheduler.scheduleAtFixedRate(
                this::performMaintenanceCycle,
                healthCheckIntervalMs,
                healthCheckIntervalMs,
                TimeUnit.MILLISECONDS
            );
            
            logger.debug("Scheduled connection health maintenance task with interval: {} ms", healthCheckIntervalMs);
        }
    }
    
    /**
     * Inner class to track health information for a specific connection.
     */
    private static class ConnectionHealthInfo {
        private final String connectionId;
        private final AtomicLong lastSuccessfulTestTime = new AtomicLong(0);
        private final AtomicLong lastFailedTestTime = new AtomicLong(0);
        private final AtomicLong totalTests = new AtomicLong(0);
        private final AtomicLong successfulTests = new AtomicLong(0);
        private final AtomicLong failedTests = new AtomicLong(0);
        private final AtomicLong totalRefreshes = new AtomicLong(0);
        private final AtomicLong successfulRefreshes = new AtomicLong(0);
        private final AtomicLong failedRefreshes = new AtomicLong(0);
        private final AtomicLong repairAttempts = new AtomicLong(0);
        private final ConcurrentLinkedQueue<String> connectionErrorMessages = new ConcurrentLinkedQueue<>();
        private volatile String lastErrorMessage;
        private volatile HealthStatus currentStatus = HealthStatus.HEALTHY;
        private volatile Instant statusChangeTime = Instant.now();
        
        public ConnectionHealthInfo(String connectionId) {
            this.connectionId = connectionId;
        }
        
        public String getConnectionId() {
            return connectionId;
        }
        
        public long getLastSuccessfulTestTime() {
            return lastSuccessfulTestTime.get();
        }
        
        public void setLastSuccessfulTestTime(long timestamp) {
            lastSuccessfulTestTime.set(timestamp);
        }
        
        public long getLastFailedTestTime() {
            return lastFailedTestTime.get();
        }
        
        public void setLastFailedTestTime(long timestamp) {
            lastFailedTestTime.set(timestamp);
        }
        
        public void incrementTotalTests() {
            totalTests.incrementAndGet();
        }
        
        public long getTotalTests() {
            return totalTests.get();
        }
        
        public void incrementSuccessfulTests() {
            successfulTests.incrementAndGet();
        }
        
        public long getSuccessfulTests() {
            return successfulTests.get();
        }
        
        public void incrementFailedTests() {
            failedTests.incrementAndGet();
        }
        
        public long getFailedTests() {
            return failedTests.get();
        }
        
        public void incrementTotalRefreshes() {
            totalRefreshes.incrementAndGet();
        }
        
        public long getTotalRefreshes() {
            return totalRefreshes.get();
        }
        
        public void incrementSuccessfulRefreshes() {
            successfulRefreshes.incrementAndGet();
        }
        
        public long getSuccessfulRefreshes() {
            return successfulRefreshes.get();
        }
        
        public void incrementFailedRefreshes() {
            failedRefreshes.incrementAndGet();
        }
        
        public long getFailedRefreshes() {
            return failedRefreshes.get();
        }
        
        public long incrementRepairAttempts() {
            return repairAttempts.incrementAndGet();
        }
        
        public void resetRepairAttempts() {
            repairAttempts.set(0);
        }
        
        public long getRepairAttempts() {
            return repairAttempts.get();
        }
        
        public void addErrorMessage(String errorMessage) {
            if (errorMessage != null) {
                lastErrorMessage = errorMessage;
                connectionErrorMessages.add(errorMessage);
                // Keep the queue size manageable
                while (connectionErrorMessages.size() > 10) {
                    connectionErrorMessages.poll();
                }
            }
        }
        
        public List<String> getErrorMessages() {
            return new ArrayList<>(connectionErrorMessages);
        }
        
        public String getLastErrorMessage() {
            return lastErrorMessage;
        }
        
        public HealthStatus getCurrentStatus() {
            return currentStatus;
        }
        
        public void setCurrentStatus(HealthStatus newStatus) {
            if (newStatus != currentStatus) {
                this.currentStatus = newStatus;
                this.statusChangeTime = Instant.now();
            }
        }
        
        public Instant getStatusChangeTime() {
            return statusChangeTime;
        }
        
        public Duration getStatusDuration() {
            return Duration.between(statusChangeTime, Instant.now());
        }
        
        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("connectionId", connectionId);
            map.put("lastSuccessfulTestTime", lastSuccessfulTestTime.get());
            map.put("lastFailedTestTime", lastFailedTestTime.get());
            map.put("totalTests", totalTests.get());
            map.put("successfulTests", successfulTests.get());
            map.put("failedTests", failedTests.get());
            map.put("totalRefreshes", totalRefreshes.get());
            map.put("successfulRefreshes", successfulRefreshes.get());
            map.put("failedRefreshes", failedRefreshes.get());
            map.put("repairAttempts", repairAttempts.get());
            map.put("currentStatus", currentStatus.name());
            map.put("statusChangeTime", statusChangeTime.toString());
            map.put("statusDurationMs", getStatusDuration().toMillis());
            map.put("lastErrorMessage", lastErrorMessage);
            return map;
        }
    }
    
    /**
     * Inner class to track error history with timestamps.
     */
    private static class ErrorHistoryEntry {
        private final String connectionId;
        private final Instant timestamp;
        private final String errorMessage;
        private final FTPErrorType errorType;
        
        public ErrorHistoryEntry(String connectionId, String errorMessage, FTPErrorType errorType) {
            this.connectionId = connectionId;
            this.timestamp = Instant.now();
            this.errorMessage = errorMessage;
            this.errorType = errorType;
        }
        
        public String getConnectionId() {
            return connectionId;
        }
        
        public Instant getTimestamp() {
            return timestamp;
        }
        
        public String getErrorMessage() {
            return errorMessage;
        }
        
        public FTPErrorType getErrorType() {
            return errorType;
        }
        
        @Override
        public String toString() {
            return String.format("[%s] Connection %s: %s (%s)",
                    timestamp, connectionId, errorMessage, 
                    errorType != null ? errorType.name() : "UNKNOWN");
        }
        
        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("connectionId", connectionId);
            map.put("timestamp", timestamp.toString());
            map.put("errorMessage", errorMessage);
            map.put("errorType", errorType != null ? errorType.name() : "UNKNOWN");
            return map;
        }
    }

    @Override
    public boolean testConnection(FTPClient client) {
        if (client == null) {
            logger.warn("Cannot test null FTP client");
            return false;
        }
        
        // Try to locate a connection ID for this client
        String connectionId = getConnectionIdForClient(client);
        
        try {
            // Track the test attempt
            totalConnectionTests.incrementAndGet();
            if (connectionId != null) {
                ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
                healthInfo.incrementTotalTests();
            }
            
            // Check if the client is connected
            if (!client.isConnected()) {
                recordFailedTest(connectionId, "Client is not connected");
                return false;
            }
            
            // Send a NOOP command to test the connection
            boolean result = client.sendNoOp();
            int replyCode = client.getReplyCode();
            
            if (result) {
                // Success - connection is working
                long now = System.currentTimeMillis();
                recordSuccessfulTest(connectionId, now);
                logger.debug("Connection test successful with reply code: {}", replyCode);
                return true;
            } else {
                // Failed - connection is not working
                String message = "Connection test failed with reply code: " + replyCode;
                recordFailedTest(connectionId, message);
                logger.warn(message);
                return false;
            }
        } catch (IOException e) {
            // Exception indicates a connection problem
            String message = "Connection test failed with exception: " + e.getMessage();
            recordFailedTest(connectionId, message);
            logger.warn("Error testing FTP connection: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean testConnection(FTPConnection connection) {
        if (connection == null) {
            logger.warn("Cannot test null FTP connection");
            return false;
        }
        
        try {
            // Track the test attempt
            totalConnectionTests.incrementAndGet();
            ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
            healthInfo.incrementTotalTests();
            
            // Get the FTP client from the connection
            FTPClient client = connection.getClient();
            if (client == null) {
                recordFailedTest(connection.getId(), "Connection has no client");
                return false;
            }
            
            // Check if the client is connected
            if (!client.isConnected()) {
                recordFailedTest(connection.getId(), "Client is not connected");
                return false;
            }
            
            // Send a NOOP command to test the connection
            boolean result = client.sendNoOp();
            int replyCode = client.getReplyCode();
            
            if (result) {
                // Success - connection is working
                long now = System.currentTimeMillis();
                recordSuccessfulTest(connection.getId(), now);
                logger.debug("Connection test successful for {}", connection.getId());
                
                // Update connection state if needed
                if (connection.getState() == FTPConnectionState.FAILED) {
                    connection.setState(FTPConnectionState.CONNECTED);
                }
                
                // Update connection's last tested timestamp
                connection.markAsTested();
                
                return true;
            } else {
                // Failed - connection is not working
                String message = "Connection test failed with reply code: " + replyCode;
                recordFailedTest(connection.getId(), message);
                
                // Update connection state
                connection.setState(FTPConnectionState.FAILED);
                connection.setLastErrorMessage(message);
                
                logger.warn("Connection test failed for {} with reply code: {}", 
                        connection.getId(), replyCode);
                return false;
            }
        } catch (IOException e) {
            // Exception indicates a connection problem
            String message = "Connection test failed with exception: " + e.getMessage();
            recordFailedTest(connection.getId(), message);
            
            // Update connection state
            connection.setState(FTPConnectionState.FAILED);
            connection.setLastErrorMessage(message);
            
            logger.warn("Error testing FTP connection {}: {}", 
                    connection.getId(), e.getMessage());
            return false;
        }
    }

    @Override
    public boolean refreshConnection(FTPClient client) {
        if (client == null) {
            logger.warn("Cannot refresh null FTP client");
            return false;
        }
        
        // Try to locate a connection ID for this client
        String connectionId = getConnectionIdForClient(client);
        
        try {
            // Track the refresh attempt
            totalConnectionRefreshes.incrementAndGet();
            if (connectionId != null) {
                ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
                healthInfo.incrementTotalRefreshes();
            }
            
            // First, check if the connection is still valid
            try {
                boolean valid = client.sendNoOp();
                if (valid) {
                    if (connectionId != null) {
                        // The connection is already good, no need to refresh
                        logger.debug("Connection is already valid, no refresh needed");
                        recordSuccessfulRefresh(connectionId);
                    }
                    return true;
                }
            } catch (IOException e) {
                // Ignore exception, we'll refresh anyway
                logger.debug("Error checking connection before refresh: {}", e.getMessage());
            }
            
            // Attempt to close the client
            try {
                if (client.isConnected()) {
                    client.disconnect();
                }
            } catch (IOException e) {
                logger.debug("Error disconnecting FTP client before refresh: {}", e.getMessage());
                // Continue with refresh attempt
            }
            
            // Create a new FTP connection
            FTPClient newClient = connectionManager.createConnection();
            
            // If we were successful, update successful metrics
            if (newClient != null && newClient.isConnected()) {
                if (connectionId != null) {
                    recordSuccessfulRefresh(connectionId);
                } else {
                    successfulConnectionRefreshes.incrementAndGet();
                }
                
                logger.debug("Successfully refreshed FTP connection");
                return true;
            } else {
                // Failed to create a new connection
                if (connectionId != null) {
                    recordFailedRefresh(connectionId, "Failed to create new connection");
                } else {
                    failedConnectionRefreshes.incrementAndGet();
                    addErrorToHistory(null, "Failed to create new connection", FTPErrorType.CONNECTION_ERROR);
                }
                
                logger.warn("Failed to refresh FTP connection");
                return false;
            }
        } catch (Exception e) {
            // Record the failure
            if (connectionId != null) {
                recordFailedRefresh(connectionId, "Refresh failed: " + e.getMessage());
            } else {
                failedConnectionRefreshes.incrementAndGet();
                addErrorToHistory(null, "Refresh failed: " + e.getMessage(), FTPErrorType.CONNECTION_ERROR);
            }
            
            logger.warn("Error refreshing FTP connection: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean refreshConnection(FTPConnection connection) {
        if (connection == null) {
            logger.warn("Cannot refresh null FTP connection");
            return false;
        }
        
        // Track the refresh attempt
        totalConnectionRefreshes.incrementAndGet();
        ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
        healthInfo.incrementTotalRefreshes();
        
        // Try reconnecting through the connection manager
        try {
            boolean reconnected = connectionManager.reconnect(connection);
            if (reconnected) {
                // Success
                recordSuccessfulRefresh(connection.getId());
                logger.debug("Successfully refreshed FTP connection: {}", connection.getId());
                return true;
            } else {
                // Failed
                recordFailedRefresh(connection.getId(), "Connection refresh failed");
                logger.warn("Failed to refresh FTP connection: {}", connection.getId());
                return false;
            }
        } catch (Exception e) {
            // Record the failure
            recordFailedRefresh(connection.getId(), "Refresh failed: " + e.getMessage());
            logger.warn("Error refreshing FTP connection {}: {}", 
                    connection.getId(), e.getMessage());
            return false;
        }
    }

    @Override
    public boolean keepAlive(FTPClient client) {
        if (client == null) {
            logger.warn("Cannot keep alive null FTP client");
            return false;
        }
        
        // Try to locate a connection ID for this client
        String connectionId = getConnectionIdForClient(client);
        
        try {
            // Track the keep-alive attempt
            totalKeepAlivesSent.incrementAndGet();
            
            // Send a NOOP command to keep the connection alive
            boolean result = client.sendNoOp();
            
            if (result) {
                // Success
                successfulKeepAlivesSent.incrementAndGet();
                if (connectionId != null) {
                    long now = System.currentTimeMillis();
                    ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
                    healthInfo.setLastSuccessfulTestTime(now);
                }
                
                logger.debug("Keep-alive successful for FTP connection");
                return true;
            } else {
                // Failed
                failedKeepAlivesSent.incrementAndGet();
                if (connectionId != null) {
                    ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
                    healthInfo.addErrorMessage("Keep-alive failed with reply code: " + client.getReplyCode());
                }
                
                logger.warn("Keep-alive failed for FTP connection with reply code: {}", 
                        client.getReplyCode());
                return false;
            }
        } catch (IOException e) {
            // Exception indicates a connection problem
            failedKeepAlivesSent.incrementAndGet();
            if (connectionId != null) {
                ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
                healthInfo.addErrorMessage("Keep-alive failed: " + e.getMessage());
            }
            
            logger.warn("Error sending keep-alive to FTP connection: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean keepAlive(FTPConnection connection) {
        if (connection == null) {
            logger.warn("Cannot keep alive null FTP connection");
            return false;
        }
        
        try {
            // Track the keep-alive attempt
            totalKeepAlivesSent.incrementAndGet();
            
            // Get the FTP client from the connection
            FTPClient client = connection.getClient();
            if (client == null) {
                logger.warn("Cannot keep alive connection {} with null client", connection.getId());
                failedKeepAlivesSent.incrementAndGet();
                return false;
            }
            
            // Send a NOOP command to keep the connection alive
            boolean result = client.sendNoOp();
            
            if (result) {
                // Success
                successfulKeepAlivesSent.incrementAndGet();
                long now = System.currentTimeMillis();
                ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
                healthInfo.setLastSuccessfulTestTime(now);
                
                // Update connection's last tested timestamp
                connection.markAsTested();
                
                logger.debug("Keep-alive successful for FTP connection {}", connection.getId());
                return true;
            } else {
                // Failed
                failedKeepAlivesSent.incrementAndGet();
                ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
                healthInfo.addErrorMessage("Keep-alive failed with reply code: " + client.getReplyCode());
                
                logger.warn("Keep-alive failed for FTP connection {} with reply code: {}", 
                        connection.getId(), client.getReplyCode());
                return false;
            }
        } catch (IOException e) {
            // Exception indicates a connection problem
            failedKeepAlivesSent.incrementAndGet();
            ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
            healthInfo.addErrorMessage("Keep-alive failed: " + e.getMessage());
            
            logger.warn("Error sending keep-alive to FTP connection {}: {}", 
                    connection.getId(), e.getMessage());
            return false;
        }
    }

    @Override
    public HealthStatus getConnectionStatus(FTPClient client) {
        if (client == null) {
            return HealthStatus.FAILED;
        }
        
        // Try to locate a connection ID for this client
        String connectionId = getConnectionIdForClient(client);
        if (connectionId != null) {
            ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
            return determineHealthStatus(healthInfo);
        }
        
        // If we can't find the connection in our tracking, we'll have to test it directly
        try {
            // Check if the client is connected
            if (!client.isConnected()) {
                return HealthStatus.FAILED;
            }
            
            // Try a NOOP command
            boolean result = client.sendNoOp();
            if (result) {
                return HealthStatus.HEALTHY;
            } else {
                int replyCode = client.getReplyCode();
                // Temporary failures might be recoverable
                if (FTPReply.isPositivePreliminary(replyCode) || 
                    (replyCode >= 400 && replyCode < 500)) { // 4xx codes are transient errors
                    return HealthStatus.DEGRADED;
                } else {
                    return HealthStatus.FAILED;
                }
            }
        } catch (Exception e) {
            return HealthStatus.FAILED;
        }
    }

    @Override
    public HealthStatus getConnectionStatus(FTPConnection connection) {
        if (connection == null) {
            return HealthStatus.FAILED;
        }
        
        // Check if we have health info for this connection
        ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
        
        // If the connection is in a transitional state, return REPAIRING
        if (connection.getState().isTransitional()) {
            healthInfo.setCurrentStatus(HealthStatus.REPAIRING);
            return HealthStatus.REPAIRING;
        }
        
        // If the connection is failed or disconnected, return FAILED
        if (connection.getState() == FTPConnectionState.FAILED || 
            connection.getState() == FTPConnectionState.DISCONNECTED) {
            healthInfo.setCurrentStatus(HealthStatus.FAILED);
            return HealthStatus.FAILED;
        }
        
        // Determine status based on health metrics
        return determineHealthStatus(healthInfo);
    }

    @Override
    public long getLastSuccessfulTestTime(FTPClient client) {
        if (client == null) {
            return 0;
        }
        
        // Try to locate a connection ID for this client
        String connectionId = getConnectionIdForClient(client);
        if (connectionId != null) {
            ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
            return healthInfo.getLastSuccessfulTestTime();
        }
        
        return 0;
    }

    @Override
    public long getLastSuccessfulTestTime(FTPConnection connection) {
        if (connection == null) {
            return 0;
        }
        
        ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
        return healthInfo.getLastSuccessfulTestTime();
    }

    @Override
    public List<String> getConnectionErrorHistory() {
        List<String> result = new ArrayList<>(errorHistory.size());
        for (ErrorHistoryEntry entry : errorHistory) {
            result.add(entry.toString());
        }
        return result;
    }

    @Override
    public Map<String, Object> getHealthMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Add global health metrics
        metrics.put("lastHealthCheckTime", lastHealthCheckTime.get());
        metrics.put("totalConnectionTests", totalConnectionTests.get());
        metrics.put("successfulConnectionTests", successfulConnectionTests.get());
        metrics.put("failedConnectionTests", failedConnectionTests.get());
        metrics.put("totalConnectionRefreshes", totalConnectionRefreshes.get());
        metrics.put("successfulConnectionRefreshes", successfulConnectionRefreshes.get());
        metrics.put("failedConnectionRefreshes", failedConnectionRefreshes.get());
        metrics.put("totalKeepAlivesSent", totalKeepAlivesSent.get());
        metrics.put("successfulKeepAlivesSent", successfulKeepAlivesSent.get());
        metrics.put("failedKeepAlivesSent", failedKeepAlivesSent.get());
        
        // Add maintenance metrics
        metrics.put("maintenanceCycleCount", maintenanceCycleCount.get());
        metrics.put("lastMaintenanceStartTime", lastMaintenanceStartTime.get());
        metrics.put("lastMaintenanceEndTime", lastMaintenanceEndTime.get());
        metrics.put("totalMaintenanceTime", totalMaintenanceTime.get());
        metrics.put("isRunningMaintenance", isRunningMaintenance.get());
        
        // Add connection status counts
        int healthyCount = 0;
        int degradedCount = 0;
        int failedCount = 0;
        int repairingCount = 0;
        
        for (ConnectionHealthInfo healthInfo : connectionHealthMap.values()) {
            switch (healthInfo.getCurrentStatus()) {
                case HEALTHY:
                    healthyCount++;
                    break;
                case DEGRADED:
                    degradedCount++;
                    break;
                case FAILED:
                    failedCount++;
                    break;
                case REPAIRING:
                    repairingCount++;
                    break;
            }
        }
        
        metrics.put("healthyConnectionCount", healthyCount);
        metrics.put("degradedConnectionCount", degradedCount);
        metrics.put("failedConnectionCount", failedCount);
        metrics.put("repairingConnectionCount", repairingCount);
        metrics.put("totalTrackedConnections", connectionHealthMap.size());
        
        // Add error history size
        metrics.put("errorHistorySize", errorHistory.size());
        
        return metrics;
    }

    @Override
    public Map<String, Object> analyzeConnection(FTPConnection connection) {
        if (connection == null) {
            return Collections.emptyMap();
        }
        
        Map<String, Object> analysis = new HashMap<>();
        
        // Get health info and add to analysis
        ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
        analysis.putAll(healthInfo.toMap());
        
        // Add connection state information
        analysis.put("state", connection.getState().name());
        analysis.put("createdAt", connection.getCreatedAt().toString());
        analysis.put("lastUsedAt", connection.getLastUsedAt() != null ? 
                connection.getLastUsedAt().toString() : null);
        analysis.put("lastTestedAt", connection.getLastTestedAt() != null ? 
                connection.getLastTestedAt().toString() : null);
        analysis.put("reconnectAttempts", connection.getReconnectAttempts());
        analysis.put("connectionErrorMessage", connection.getLastErrorMessage());
        
        // Add health status and reason
        HealthStatus status = getConnectionStatus(connection);
        analysis.put("healthStatus", status.name());
        
        // Add reason for current health status
        String statusReason = determineHealthStatusReason(connection, healthInfo);
        analysis.put("healthStatusReason", statusReason);
        
        // Add recommendation based on status
        String recommendation = generateRecommendation(connection, healthInfo, status);
        analysis.put("recommendation", recommendation);
        
        return analysis;
    }

    @Override
    public int performMaintenanceCycle() {
        if (shutdown) {
            logger.debug("Not performing maintenance cycle as health manager is shut down");
            return 0;
        }
        
        // Ensure we don't have two maintenance cycles running simultaneously
        if (!isRunningMaintenance.compareAndSet(false, true)) {
            logger.debug("Maintenance cycle already running, skipping");
            return 0;
        }
        
        try {
            // Update metrics
            lastMaintenanceStartTime.set(System.currentTimeMillis());
            maintenanceCycleCount.incrementAndGet();
            
            logger.debug("Starting connection health maintenance cycle");
            
            // Get all connections from the connection manager
            Map<String, FTPConnection> connections = getConnections();
            if (connections.isEmpty()) {
                logger.debug("No connections to maintain");
                return 0;
            }
            
            // Track how many connections were repaired
            int repairedCount = 0;
            
            // Check and repair each connection as needed
            for (FTPConnection connection : connections.values()) {
                // Skip connections that are in transitional states
                if (connection.getState().isTransitional()) {
                    logger.debug("Skipping connection {} in transitional state: {}", 
                            connection.getId(), connection.getState());
                    continue;
                }
                
                // Update health status
                ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connection.getId());
                HealthStatus status = determineHealthStatus(healthInfo);
                healthInfo.setCurrentStatus(status);
                
                // If the connection is healthy, we can skip it
                if (status == HealthStatus.HEALTHY && connection.getState().isUsable()) {
                    continue;
                }
                
                // If the connection is degraded, test it to make sure
                if (status == HealthStatus.DEGRADED) {
                    logger.debug("Testing degraded connection: {}", connection.getId());
                    boolean testResult = testConnection(connection);
                    if (testResult) {
                        // Connection is actually fine, update status
                        healthInfo.setCurrentStatus(HealthStatus.HEALTHY);
                        continue;
                    }
                }
                
                // If the connection is failed, try to repair it
                if (status == HealthStatus.FAILED || !connection.getState().isUsable()) {
                    // Check if we've exceeded the maximum repair attempts
                    long repairAttempts = healthInfo.getRepairAttempts();
                    if (repairAttempts >= maxRepairAttempts) {
                        logger.warn("Maximum repair attempts ({}) reached for connection {}",
                                maxRepairAttempts, connection.getId());
                        continue;
                    }
                    
                    // Increment repair attempts
                    healthInfo.incrementRepairAttempts();
                    
                    // Set status to repairing
                    healthInfo.setCurrentStatus(HealthStatus.REPAIRING);
                    
                    // Attempt to repair the connection
                    logger.info("Attempting to repair connection {}, attempt {}/{}",
                            connection.getId(), repairAttempts + 1, maxRepairAttempts);
                    
                    boolean repaired = refreshConnection(connection);
                    if (repaired) {
                        // Reset repair attempts on success
                        healthInfo.resetRepairAttempts();
                        
                        // Update status
                        healthInfo.setCurrentStatus(HealthStatus.HEALTHY);
                        
                        // Record success
                        logger.info("Successfully repaired connection {}", connection.getId());
                        repairedCount++;
                    } else {
                        // Update status
                        healthInfo.setCurrentStatus(HealthStatus.FAILED);
                        
                        logger.warn("Failed to repair connection {}", connection.getId());
                    }
                }
            }
            
            // Update metrics
            lastHealthCheckTime.set(System.currentTimeMillis());
            long duration = lastHealthCheckTime.get() - lastMaintenanceStartTime.get();
            lastMaintenanceEndTime.set(lastHealthCheckTime.get());
            totalMaintenanceTime.addAndGet(duration);
            
            logger.debug("Completed connection health maintenance cycle in {}ms. Repaired {} connections",
                    duration, repairedCount);
            
            return repairedCount;
        } catch (Exception e) {
            logger.error("Error during connection health maintenance cycle: {}", e.getMessage(), e);
            return 0;
        } finally {
            isRunningMaintenance.set(false);
        }
    }
    
    /**
     * Shuts down the health manager, releasing any resources.
     */
    public void shutdown() {
        if (shutdown) {
            return;
        }
        
        shutdown = true;
        
        // Cancel scheduled tasks
        if (maintenanceTask != null) {
            maintenanceTask.cancel(true);
            maintenanceTask = null;
        }
        
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
        connectionHealthMap.clear();
        errorHistory.clear();
        
        logger.info("FTP Connection Health Manager shut down");
    }

    /* Private helper methods */
    
    /**
     * Gets or creates a ConnectionHealthInfo object for a connection ID.
     */
    private ConnectionHealthInfo getConnectionHealthInfo(String connectionId) {
        return connectionHealthMap.computeIfAbsent(connectionId, id -> new ConnectionHealthInfo(id));
    }
    
    /**
     * Records a successful connection test.
     */
    private void recordSuccessfulTest(String connectionId, long timestamp) {
        // Update global metrics
        successfulConnectionTests.incrementAndGet();
        
        // Update connection-specific metrics if we have a connection ID
        if (connectionId != null) {
            ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
            healthInfo.setLastSuccessfulTestTime(timestamp);
            healthInfo.incrementSuccessfulTests();
            healthInfo.setCurrentStatus(HealthStatus.HEALTHY);
        }
    }
    
    /**
     * Records a failed connection test.
     */
    private void recordFailedTest(String connectionId, String errorMessage) {
        // Update global metrics
        failedConnectionTests.incrementAndGet();
        
        // Update connection-specific metrics if we have a connection ID
        if (connectionId != null) {
            ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
            healthInfo.setLastFailedTestTime(System.currentTimeMillis());
            healthInfo.incrementFailedTests();
            healthInfo.addErrorMessage(errorMessage);
            
            // Update status based on error pattern
            if (errorMessage != null && (
                    errorMessage.contains("not connected") ||
                    errorMessage.contains("reply code: 421") ||
                    errorMessage.contains("connection closed"))) {
                healthInfo.setCurrentStatus(HealthStatus.FAILED);
            } else {
                healthInfo.setCurrentStatus(HealthStatus.DEGRADED);
            }
            
            // Add to global error history
            addErrorToHistory(connectionId, errorMessage, deriveErrorType(errorMessage));
        }
    }
    
    /**
     * Records a successful connection refresh.
     */
    private void recordSuccessfulRefresh(String connectionId) {
        // Update global metrics
        successfulConnectionRefreshes.incrementAndGet();
        
        // Update connection-specific metrics
        ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
        healthInfo.incrementSuccessfulRefreshes();
        healthInfo.setLastSuccessfulTestTime(System.currentTimeMillis());
        healthInfo.setCurrentStatus(HealthStatus.HEALTHY);
    }
    
    /**
     * Records a failed connection refresh.
     */
    private void recordFailedRefresh(String connectionId, String errorMessage) {
        // Update global metrics
        failedConnectionRefreshes.incrementAndGet();
        
        // Update connection-specific metrics
        ConnectionHealthInfo healthInfo = getConnectionHealthInfo(connectionId);
        healthInfo.incrementFailedRefreshes();
        healthInfo.addErrorMessage(errorMessage);
        healthInfo.setCurrentStatus(HealthStatus.FAILED);
        
        // Add to global error history
        addErrorToHistory(connectionId, errorMessage, deriveErrorType(errorMessage));
    }
    
    /**
     * Adds an error to the global error history.
     */
    private void addErrorToHistory(String connectionId, String errorMessage, FTPErrorType errorType) {
        ErrorHistoryEntry entry = new ErrorHistoryEntry(connectionId != null ? connectionId : "unknown", 
                errorMessage, errorType);
        errorHistory.add(entry);
        
        // Trim history if needed
        while (errorHistory.size() > maxErrorHistorySize) {
            errorHistory.poll();
        }
    }
    
    /**
     * Attempts to derive an error type from an error message.
     */
    private FTPErrorType deriveErrorType(String errorMessage) {
        if (errorMessage == null) {
            return FTPErrorType.UNEXPECTED_ERROR;
        }
        
        // Check for specific error patterns
        if (errorMessage.contains("timeout") || errorMessage.contains("timed out")) {
            return FTPErrorType.CONNECTION_TIMEOUT;
        } else if (errorMessage.contains("refused")) {
            return FTPErrorType.CONNECTION_REFUSED;
        } else if (errorMessage.contains("reset") || errorMessage.contains("closed") || 
                   errorMessage.contains("not connected")) {
            return FTPErrorType.CONNECTION_CLOSED;
        } else if (errorMessage.contains("login") || errorMessage.contains("authentication") ||
                   errorMessage.contains("password") || errorMessage.contains("username")) {
            return FTPErrorType.AUTHENTICATION_ERROR;
        } else if (errorMessage.contains("reply code: 5")) {
            return FTPErrorType.SERVER_ERROR;
        } else if (errorMessage.contains("reply code: 4")) {
            return FTPErrorType.TEMPORARY_ERROR;
        }
        
        // Default case
        return FTPErrorType.CONNECTION_ERROR;
    }
    
    /**
     * Determines the health status of a connection based on its health metrics.
     */
    private HealthStatus determineHealthStatus(ConnectionHealthInfo healthInfo) {
        // If the connection is currently being repaired, return REPAIRING
        if (healthInfo.getCurrentStatus() == HealthStatus.REPAIRING) {
            return HealthStatus.REPAIRING;
        }
        
        // Check the time since the last successful test
        long lastSuccessTime = healthInfo.getLastSuccessfulTestTime();
        if (lastSuccessTime == 0) {
            // Never been successfully tested
            return HealthStatus.FAILED;
        }
        
        long now = System.currentTimeMillis();
        long timeSinceLastSuccess = now - lastSuccessTime;
        
        // If it's been too long since the last successful test, consider it failed
        if (timeSinceLastSuccess > connectionHealthThresholdMs) {
            return HealthStatus.FAILED;
        }
        
        // If it's been a while, but not too long, consider it degraded
        if (timeSinceLastSuccess > connectionWarningThresholdMs) {
            return HealthStatus.DEGRADED;
        }
        
        // Check if there have been recent failures
        long lastFailedTime = healthInfo.getLastFailedTestTime();
        if (lastFailedTime > 0) {
            long timeSinceLastFailure = now - lastFailedTime;
            
            // If there was a recent failure but also a more recent success
            if (timeSinceLastFailure < connectionWarningThresholdMs && lastSuccessTime > lastFailedTime) {
                return HealthStatus.DEGRADED;
            }
            
            // If the last test was a failure, consider it failed
            if (lastFailedTime > lastSuccessTime) {
                return HealthStatus.FAILED;
            }
        }
        
        // If we got here, the connection is healthy
        return HealthStatus.HEALTHY;
    }
    
    /**
     * Provides a human-readable reason for the current health status of a connection.
     */
    private String determineHealthStatusReason(FTPConnection connection, ConnectionHealthInfo healthInfo) {
        HealthStatus status = healthInfo.getCurrentStatus();
        
        switch (status) {
            case HEALTHY:
                long timeSinceTest = System.currentTimeMillis() - healthInfo.getLastSuccessfulTestTime();
                return String.format("Connection tested successfully %d ms ago", timeSinceTest);
                
            case DEGRADED:
                if (connection.getState() != FTPConnectionState.CONNECTED && 
                    connection.getState() != FTPConnectionState.IDLE) {
                    return String.format("Connection in non-optimal state: %s", connection.getState());
                }
                
                long timeSinceLastSuccess = System.currentTimeMillis() - healthInfo.getLastSuccessfulTestTime();
                if (timeSinceLastSuccess > connectionWarningThresholdMs) {
                    return String.format("Connection has not been tested successfully in %d ms", 
                            timeSinceLastSuccess);
                }
                
                if (healthInfo.getLastFailedTestTime() > 0) {
                    return String.format("Connection has recent test failures: %s", 
                            healthInfo.getLastErrorMessage());
                }
                
                return "Connection is experiencing intermittent issues";
                
            case FAILED:
                if (connection.getState() == FTPConnectionState.FAILED) {
                    return String.format("Connection is in FAILED state: %s", 
                            connection.getLastErrorMessage());
                }
                
                if (connection.getState() == FTPConnectionState.DISCONNECTED) {
                    return "Connection is disconnected";
                }
                
                if (healthInfo.getLastSuccessfulTestTime() == 0) {
                    return "Connection has never been successfully tested";
                }
                
                timeSinceLastSuccess = System.currentTimeMillis() - healthInfo.getLastSuccessfulTestTime();
                return String.format("Connection has not been successfully tested in %d ms", 
                        timeSinceLastSuccess);
                
            case REPAIRING:
                return String.format("Connection is being repaired (attempt %d of %d)", 
                        healthInfo.getRepairAttempts(), maxRepairAttempts);
                
            default:
                return "Unknown status reason";
        }
    }
    
    /**
     * Generates a recommendation for handling a connection based on its health status.
     */
    private String generateRecommendation(FTPConnection connection, ConnectionHealthInfo healthInfo, 
                                         HealthStatus status) {
        switch (status) {
            case HEALTHY:
                return "No action needed";
                
            case DEGRADED:
                return "Monitor connection and test more frequently";
                
            case FAILED:
                if (healthInfo.getRepairAttempts() >= maxRepairAttempts) {
                    return "Maximum repair attempts reached, manual intervention required";
                }
                return "Attempt to repair connection";
                
            case REPAIRING:
                return "Wait for repair to complete";
                
            default:
                return "Unknown recommendation";
        }
    }
    
    /**
     * Attempts to find a connection ID for an FTP client by checking all connections.
     */
    private String getConnectionIdForClient(FTPClient client) {
        if (client == null) {
            return null;
        }
        
        // Check all connections in the manager
        Map<String, FTPConnection> connections = getConnections();
        for (FTPConnection connection : connections.values()) {
            if (connection.getClient() == client) {
                return connection.getId();
            }
        }
        
        // If not found, we generate a temporary ID
        String tempId = "tmp-" + UUID.randomUUID().toString();
        logger.debug("No matching connection found for FTP client, assigning temporary ID: {}", tempId);
        return tempId;
    }
    
    /**
     * Gets all connections from the connection manager.
     * This method is mainly for testing and internal use.
     */
    protected Map<String, FTPConnection> getConnections() {
        // We don't have direct access to the connections map in the manager
        // In a real implementation, there might be a method to get all connections
        // For now, we'll return an empty map to satisfy the interface
        return new HashMap<>();
    }
}