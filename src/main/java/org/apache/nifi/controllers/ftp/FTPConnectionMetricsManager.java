package org.apache.nifi.controllers.ftp;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessorNode;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages all metrics collection and reporting components for FTP connections.
 * This class serves as a central coordination point for metrics collection, JMX exposure,
 * bulletin reporting, and regular health monitoring.
 */
public class FTPConnectionMetricsManager {
    
    private final ComponentLog logger;
    private final FTPConnectionMetrics metrics;
    private final FTPConnectionJMXMonitor jmxMonitor;
    private final FTPConnectionBulletinReporter bulletinReporter;
    private final FTPConnectionManager connectionManager;
    private final ProcessorNode processorNode;
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> healthCheckTask;
    private FTPConnectionPoolHealth poolHealth;
    
    // Default settings
    private long healthCheckIntervalMs = 30000; // Default to 30 seconds
    
    /**
     * Creates a new metrics manager with all components.
     *
     * @param connectionManager The connection manager to monitor
     * @param serviceName The name of the FTP service (for JMX and bulletins)
     * @param processorNode The processor node for bulletins
     * @param logger The logger to use
     */
    public FTPConnectionMetricsManager(FTPConnectionManager connectionManager, String serviceName,
            ProcessorNode processorNode, ComponentLog logger) {
        this.logger = logger;
        this.connectionManager = connectionManager;
        this.processorNode = processorNode;
        
        // Create the metrics components
        this.metrics = new FTPConnectionMetrics();
        this.jmxMonitor = new FTPConnectionJMXMonitor(metrics, serviceName, logger);
        this.bulletinReporter = processorNode != null 
                ? new FTPConnectionBulletinReporter(metrics, processorNode, logger)
                : null;
                
        this.poolHealth = new FTPConnectionPoolHealth(metrics, logger);
    }
    
    /**
     * Enables all metrics collection and reporting.
     */
    public void enable() {
        if (enabled.compareAndSet(false, true)) {
            // Register JMX MBean
            jmxMonitor.register();
            
            // Start bulletin reporter if available
            if (bulletinReporter != null) {
                bulletinReporter.start(5000, 60000); // 5 second initial delay, 1 minute interval
            }
            
            // Start health check scheduler
            startHealthChecks();
            
            logger.info("Enabled FTP Connection Metrics Manager");
        }
    }
    
    /**
     * Disables all metrics collection and reporting.
     */
    public void disable() {
        if (enabled.compareAndSet(true, false)) {
            // Unregister JMX MBean
            jmxMonitor.unregister();
            
            // Stop bulletin reporter if available
            if (bulletinReporter != null) {
                bulletinReporter.stop();
            }
            
            // Stop health check scheduler
            stopHealthChecks();
            
            logger.info("Disabled FTP Connection Metrics Manager");
        }
    }
    
    /**
     * Starts the health check scheduler.
     */
    private void startHealthChecks() {
        // Create a scheduler for health checks
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("FTP-HealthCheck");
            t.setDaemon(true);
            return t;
        });
        
        // Schedule the health check task
        this.healthCheckTask = scheduler.scheduleAtFixedRate(
                this::performHealthCheck,
                5000, // 5 second initial delay
                healthCheckIntervalMs,
                TimeUnit.MILLISECONDS);
        
        logger.debug("Started FTP Connection health checks with interval: {} ms", healthCheckIntervalMs);
    }
    
    /**
     * Stops the health check scheduler.
     */
    private void stopHealthChecks() {
        // Cancel the health check task
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
            healthCheckTask = null;
        }
        
        // Shutdown the scheduler
        if (scheduler != null) {
            try {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            } finally {
                scheduler = null;
            }
        }
    }
    
    /**
     * Performs a health check of the connection pool and updates metrics.
     */
    private void performHealthCheck() {
        try {
            // Get connection pool statistics
            int totalConnections = connectionManager.getConnectionCount();
            int activeConnections = connectionManager.getConnectionCount(FTPConnectionState.CONNECTED);
            int failedConnections = connectionManager.getConnectionCount(FTPConnectionState.FAILED);
            int idleConnections = totalConnections - activeConnections - failedConnections;
            
            // Update connection counts in metrics
            metrics.updateConnectionCounts(activeConnections, idleConnections, failedConnections);
            
            // Get memory usage
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            metrics.updateMemoryUsage(usedMemory);
            
            // Get thread metrics
            int activeThreads = Thread.activeCount();
            metrics.updateThreadCounts(activeThreads, 0); // We don't track pool threads currently
            
            // Update pool health status
            poolHealth.checkHealth();
            
        } catch (Exception e) {
            logger.error("Error during FTP connection health check: {}", new Object[] { e.getMessage() }, e);
        }
    }
    
    /**
     * Gets the metrics collection component.
     *
     * @return The metrics component
     */
    public FTPConnectionMetrics getMetrics() {
        return metrics;
    }
    
    /**
     * Gets the JMX monitor component.
     *
     * @return The JMX monitor
     */
    public FTPConnectionJMXMonitor getJmxMonitor() {
        return jmxMonitor;
    }
    
    /**
     * Gets the bulletin reporter component.
     *
     * @return The bulletin reporter
     */
    public FTPConnectionBulletinReporter getBulletinReporter() {
        return bulletinReporter;
    }
    
    /**
     * Gets the pool health component.
     *
     * @return The pool health component
     */
    public FTPConnectionPoolHealth getPoolHealth() {
        return poolHealth;
    }
    
    /**
     * Gets a snapshot of all metrics.
     *
     * @return A map containing all metrics
     */
    public Map<String, Object> getMetricsSnapshot() {
        return metrics.getMetrics();
    }
    
    /**
     * Sets the health check interval.
     *
     * @param intervalMs The interval in milliseconds
     */
    public void setHealthCheckInterval(long intervalMs) {
        this.healthCheckIntervalMs = intervalMs;
        
        // Restart health checks with the new interval if enabled
        if (enabled.get()) {
            stopHealthChecks();
            startHealthChecks();
        }
    }
    
    /**
     * Records a connection being created.
     */
    public void recordConnectionCreated() {
        metrics.recordConnectionCreated();
    }
    
    /**
     * Records a connection being destroyed.
     */
    public void recordConnectionDestroyed() {
        metrics.recordConnectionDestroyed();
    }
    
    /**
     * Records a connection being validated.
     *
     * @param valid Whether the connection was valid
     */
    public void recordConnectionValidation(boolean valid) {
        metrics.recordConnectionValidation(valid);
    }
    
    /**
     * Records a file being uploaded.
     *
     * @param fileSize The size of the file in bytes
     */
    public void recordFileUploaded(long fileSize) {
        metrics.recordFileUploaded(fileSize);
    }
    
    /**
     * Records a file being downloaded.
     *
     * @param fileSize The size of the file in bytes
     */
    public void recordFileDownloaded(long fileSize) {
        metrics.recordFileDownloaded(fileSize);
    }
    
    /**
     * Records a directory listing operation.
     */
    public void recordDirectoryListing() {
        metrics.recordDirectoryListing();
    }
    
    /**
     * Creates an operation tracker for measuring operation metrics.
     *
     * @param operationType The type of operation
     * @param operationDetails Additional details about the operation
     * @return An operation tracker
     */
    public FTPConnectionMetrics.OperationTracker startOperation(String operationType, String operationDetails) {
        return metrics.startOperation(operationType, operationDetails);
    }
    
    /**
     * Pool health monitoring class.
     */
    public static class FTPConnectionPoolHealth {
        private final FTPConnectionMetrics metrics;
        private final ComponentLog logger;
        private String currentHealthStatus = "UNKNOWN";
        private long lastHealthCheckTime = 0;
        
        /**
         * Creates a new pool health monitor.
         *
         * @param metrics The metrics to monitor
         * @param logger The logger to use
         */
        public FTPConnectionPoolHealth(FTPConnectionMetrics metrics, ComponentLog logger) {
            this.metrics = metrics;
            this.logger = logger;
        }
        
        /**
         * Checks the health of the connection pool.
         *
         * @return The current health status
         */
        public String checkHealth() {
            try {
                // Get current metrics
                Map<String, Object> metricsSnapshot = metrics.getMetrics();
                Map<String, Object> connectionPoolMetrics = getNestedMap(metricsSnapshot, "connectionPool");
                Map<String, Object> performanceMetrics = getNestedMap(metricsSnapshot, "performance");
                Map<String, Object> healthMetrics = getNestedMap(metricsSnapshot, "health");
                
                if (connectionPoolMetrics != null && performanceMetrics != null && healthMetrics != null) {
                    // Extract key metrics
                    int totalConnections = (int) connectionPoolMetrics.get("totalConnections");
                    int failedConnections = (int) connectionPoolMetrics.get("failedConnections");
                    double successRate = (double) healthMetrics.get("successRate");
                    
                    // Determine health status
                    String newStatus;
                    if (failedConnections == 0 && successRate >= 99.0) {
                        newStatus = "EXCELLENT";
                    } else if (failedConnections <= 1 && successRate >= 95.0) {
                        newStatus = "GOOD";
                    } else if (failedConnections <= 2 && successRate >= 90.0) {
                        newStatus = "FAIR";
                    } else if (successRate >= 80.0) {
                        newStatus = "DEGRADED";
                    } else {
                        newStatus = "POOR";
                    }
                    
                    // Log if health status changed
                    if (!newStatus.equals(currentHealthStatus)) {
                        logger.info("FTP Connection Pool health changed from {} to {}", 
                                new Object[] { currentHealthStatus, newStatus });
                        currentHealthStatus = newStatus;
                    }
                    
                    // Update last check time
                    lastHealthCheckTime = System.currentTimeMillis();
                    
                    return currentHealthStatus;
                }
            } catch (Exception e) {
                logger.error("Error checking pool health: {}", new Object[] { e.getMessage() }, e);
            }
            
            return currentHealthStatus;
        }
        
        /**
         * Gets the current health status.
         *
         * @return The current health status
         */
        public String getCurrentHealthStatus() {
            return currentHealthStatus;
        }
        
        /**
         * Gets the time of the last health check.
         *
         * @return The time of the last health check in milliseconds since epoch
         */
        public long getLastHealthCheckTime() {
            return lastHealthCheckTime;
        }
        
        /**
         * Gets a nested map from a metrics map.
         *
         * @param metrics The metrics map
         * @param key The key for the nested map
         * @return The nested map, or null if not found
         */
        @SuppressWarnings("unchecked")
        private Map<String, Object> getNestedMap(Map<String, Object> metrics, String key) {
            Object value = metrics.get(key);
            if (value instanceof Map) {
                return (Map<String, Object>) value;
            }
            return null;
        }
    }
}