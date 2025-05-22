package org.apache.nifi.controllers.ftp;

import org.apache.nifi.controllers.ftp.FTPConnectionHealth.HealthStatus;
import org.apache.nifi.logging.ComponentLog;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Utility class for monitoring FTP connection health and reporting metrics.
 * This class can be used to integrate with JMX or other monitoring systems.
 */
public class FTPConnectionMonitor {
    private final FTPConnectionHealthManager healthManager;
    private final FTPConnectionManager connectionManager;
    private final ComponentLog logger;
    
    // Monitoring statistics
    private final AtomicLong totalHealthyConnections = new AtomicLong(0);
    private final AtomicLong totalDegradedConnections = new AtomicLong(0);
    private final AtomicLong totalFailedConnections = new AtomicLong(0);
    private final AtomicLong totalConnectionIssues = new AtomicLong(0);
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private final AtomicLong lastIssueTimestamp = new AtomicLong(0);
    private final AtomicLong totalRepairedConnections = new AtomicLong(0);
    private final AtomicLong totalChecksDone = new AtomicLong(0);
    
    // Alert thresholds
    private long warningThresholdMs;
    private int warningFailureCount;
    private long criticalThresholdMs;
    private int criticalFailureCount;
    
    // Monitoring task
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> monitoringTask;
    private final AtomicBoolean isRunningMonitoring = new AtomicBoolean(false);
    private long monitoringIntervalMs;
    
    // Alert callback
    private Consumer<MonitoringAlert> alertCallback;
    
    /**
     * Represents an alert from the connection monitor.
     */
    public static class MonitoringAlert {
        public enum AlertLevel {
            INFO, WARNING, CRITICAL
        }
        
        private final AlertLevel level;
        private final String message;
        private final long timestamp;
        private final Map<String, Object> details;
        
        /**
         * Creates a new monitoring alert.
         *
         * @param level the alert level
         * @param message the alert message
         * @param details additional details about the alert
         */
        public MonitoringAlert(AlertLevel level, String message, Map<String, Object> details) {
            this.level = level;
            this.message = message;
            this.timestamp = System.currentTimeMillis();
            this.details = details;
        }
        
        public AlertLevel getLevel() {
            return level;
        }
        
        public String getMessage() {
            return message;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public Map<String, Object> getDetails() {
            return details;
        }
        
        @Override
        public String toString() {
            return String.format("[%s] %s: %s", level, formatTimestamp(timestamp), message);
        }
        
        private String formatTimestamp(long timestamp) {
            return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(timestamp));
        }
    }
    
    /**
     * Creates a new FTPConnectionMonitor with default settings.
     *
     * @param healthManager the health manager to use
     * @param connectionManager the connection manager to monitor
     * @param logger the logger to use
     */
    public FTPConnectionMonitor(FTPConnectionHealthManager healthManager, 
                               FTPConnectionManager connectionManager,
                               ComponentLog logger) {
        this(healthManager, connectionManager, logger, 60000, 300000, 3, 600000, 5);
    }
    
    /**
     * Creates a new FTPConnectionMonitor with custom settings.
     *
     * @param healthManager the health manager to use
     * @param connectionManager the connection manager to monitor
     * @param logger the logger to use
     * @param monitoringIntervalMs the interval between monitoring checks in milliseconds
     * @param warningThresholdMs the time threshold for warning alerts in milliseconds
     * @param warningFailureCount the failure count threshold for warning alerts
     * @param criticalThresholdMs the time threshold for critical alerts in milliseconds
     * @param criticalFailureCount the failure count threshold for critical alerts
     */
    public FTPConnectionMonitor(FTPConnectionHealthManager healthManager, 
                               FTPConnectionManager connectionManager,
                               ComponentLog logger,
                               long monitoringIntervalMs,
                               long warningThresholdMs,
                               int warningFailureCount,
                               long criticalThresholdMs,
                               int criticalFailureCount) {
        this.healthManager = healthManager;
        this.connectionManager = connectionManager;
        this.logger = logger;
        this.monitoringIntervalMs = monitoringIntervalMs;
        this.warningThresholdMs = warningThresholdMs;
        this.warningFailureCount = warningFailureCount;
        this.criticalThresholdMs = criticalThresholdMs;
        this.criticalFailureCount = criticalFailureCount;
        
        // Create a scheduler for monitoring tasks
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("FTP-Connection-Monitor");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Starts the connection monitoring.
     */
    public void startMonitoring() {
        // Only start if not already running
        if (monitoringTask != null && !monitoringTask.isDone()) {
            logger.debug("Monitoring already started, not starting again");
            return;
        }
        
        logger.info("Starting FTP connection monitoring with interval: {} ms", monitoringIntervalMs);
        
        // Schedule the monitoring task
        this.monitoringTask = scheduler.scheduleAtFixedRate(
            this::checkConnectionHealth,
            0,
            monitoringIntervalMs,
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * Stops the connection monitoring.
     */
    public void stopMonitoring() {
        if (monitoringTask != null) {
            monitoringTask.cancel(false);
            monitoringTask = null;
            logger.info("Stopped FTP connection monitoring");
        }
    }
    
    /**
     * Sets a callback to be notified of monitoring alerts.
     *
     * @param alertCallback the callback to receive alerts
     */
    public void setAlertCallback(Consumer<MonitoringAlert> alertCallback) {
        this.alertCallback = alertCallback;
    }
    
    /**
     * Performs a connection health check and updates metrics.
     */
    private void checkConnectionHealth() {
        // Ensure we don't have two checks running simultaneously
        if (!isRunningMonitoring.compareAndSet(false, true)) {
            logger.debug("Health check already running, skipping");
            return;
        }
        
        try {
            logger.debug("Running connection health check");
            
            // Reset counts for this check
            totalHealthyConnections.set(0);
            totalDegradedConnections.set(0);
            totalFailedConnections.set(0);
            
            // Get health metrics
            Map<String, Object> healthMetrics = healthManager.getHealthMetrics();
            
            // Extract status counts
            long healthyCount = (long) healthMetrics.getOrDefault("healthyConnectionCount", 0L);
            long degradedCount = (long) healthMetrics.getOrDefault("degradedConnectionCount", 0L);
            long failedCount = (long) healthMetrics.getOrDefault("failedConnectionCount", 0L);
            long repairingCount = (long) healthMetrics.getOrDefault("repairingConnectionCount", 0L);
            
            // Update totals
            totalHealthyConnections.set(healthyCount);
            totalDegradedConnections.set(degradedCount);
            totalFailedConnections.set(failedCount + repairingCount); // For monitoring purposes, we count repairing as failed
            
            // Update check count
            totalChecksDone.incrementAndGet();
            
            // Perform maintenance if needed
            long healthCheckTime = (long) healthMetrics.getOrDefault("lastHealthCheckTime", 0L);
            long now = System.currentTimeMillis();
            if (now - healthCheckTime > monitoringIntervalMs * 2) {
                // It's been a while since the last health check, force one
                logger.debug("Last health check was {} ms ago, forcing maintenance", now - healthCheckTime);
                int repairedCount = healthManager.performMaintenanceCycle();
                totalRepairedConnections.addAndGet(repairedCount);
            }
            
            // Determine if we have connection issues
            if (failedCount > 0 || degradedCount > 0) {
                // We have some connection issues
                totalConnectionIssues.incrementAndGet();
                consecutiveFailures.incrementAndGet();
                lastIssueTimestamp.set(now);
                
                // Check if we need to send alerts
                checkAlertConditions(healthyCount, degradedCount, failedCount, healthMetrics);
            } else {
                // All connections are healthy
                consecutiveFailures.set(0);
                
                // If we had issues before, send a recovery alert
                if (totalConnectionIssues.get() > 0) {
                    sendAlert(MonitoringAlert.AlertLevel.INFO,
                              "All FTP connections are now healthy",
                              healthMetrics);
                }
            }
            
            logger.debug("Connection health check completed: healthy={}, degraded={}, failed={}",
                    healthyCount, degradedCount, failedCount);
            
        } catch (Exception e) {
            logger.error("Error during connection health check: {}", e.getMessage(), e);
        } finally {
            isRunningMonitoring.set(false);
        }
    }
    
    /**
     * Checks if alert conditions are met and sends alerts if needed.
     */
    private void checkAlertConditions(long healthyCount, long degradedCount, long failedCount, 
                                    Map<String, Object> healthMetrics) {
        // Calculate total connections
        long totalConnections = healthyCount + degradedCount + failedCount;
        if (totalConnections == 0) {
            return;
        }
        
        // Calculate failure percentage
        double failurePercentage = (double) (degradedCount + failedCount) / totalConnections * 100;
        
        // Check for critical conditions
        if (consecutiveFailures.get() >= criticalFailureCount || 
            (failedCount > 0 && failedCount >= totalConnections * 0.5) ||
            (System.currentTimeMillis() - lastIssueTimestamp.get() > criticalThresholdMs && 
             failedCount > 0)) {
            
            sendAlert(MonitoringAlert.AlertLevel.CRITICAL,
                      String.format("Critical FTP connection issues: %.1f%% of connections are not healthy " +
                                    "(%d healthy, %d degraded, %d failed)", 
                                    failurePercentage, healthyCount, degradedCount, failedCount),
                      healthMetrics);
            return;
        }
        
        // Check for warning conditions
        if (consecutiveFailures.get() >= warningFailureCount || 
            (degradedCount > 0 && degradedCount >= totalConnections * 0.25) ||
            (System.currentTimeMillis() - lastIssueTimestamp.get() > warningThresholdMs && 
             (degradedCount > 0 || failedCount > 0))) {
            
            sendAlert(MonitoringAlert.AlertLevel.WARNING,
                      String.format("Warning: FTP connection issues detected: %.1f%% of connections are not healthy " +
                                    "(%d healthy, %d degraded, %d failed)", 
                                    failurePercentage, healthyCount, degradedCount, failedCount),
                      healthMetrics);
            return;
        }
        
        // Send info alert for minor issues
        if (degradedCount > 0 || failedCount > 0) {
            sendAlert(MonitoringAlert.AlertLevel.INFO,
                      String.format("Info: Some FTP connections have issues: %.1f%% of connections are not healthy " +
                                    "(%d healthy, %d degraded, %d failed)", 
                                    failurePercentage, healthyCount, degradedCount, failedCount),
                      healthMetrics);
        }
    }
    
    /**
     * Sends an alert through the registered callback.
     */
    private void sendAlert(MonitoringAlert.AlertLevel level, String message, Map<String, Object> details) {
        MonitoringAlert alert = new MonitoringAlert(level, message, details);
        
        // Log the alert
        switch (level) {
            case CRITICAL:
                logger.error(alert.toString());
                break;
            case WARNING:
                logger.warn(alert.toString());
                break;
            case INFO:
                logger.info(alert.toString());
                break;
        }
        
        // Send to callback if registered
        if (alertCallback != null) {
            try {
                alertCallback.accept(alert);
            } catch (Exception e) {
                logger.error("Error in alert callback: {}", e.getMessage(), e);
            }
        }
    }
    
    /**
     * Gets the current monitoring metrics.
     *
     * @return a map of monitoring metrics
     */
    public Map<String, Object> getMonitoringMetrics() {
        Map<String, Object> metrics = new java.util.HashMap<>();
        
        metrics.put("totalHealthyConnections", totalHealthyConnections.get());
        metrics.put("totalDegradedConnections", totalDegradedConnections.get());
        metrics.put("totalFailedConnections", totalFailedConnections.get());
        metrics.put("totalConnectionIssues", totalConnectionIssues.get());
        metrics.put("consecutiveFailures", consecutiveFailures.get());
        metrics.put("lastIssueTimestamp", lastIssueTimestamp.get());
        metrics.put("totalRepairedConnections", totalRepairedConnections.get());
        metrics.put("totalChecksDone", totalChecksDone.get());
        metrics.put("monitoringIntervalMs", monitoringIntervalMs);
        metrics.put("warningThresholdMs", warningThresholdMs);
        metrics.put("warningFailureCount", warningFailureCount);
        metrics.put("criticalThresholdMs", criticalThresholdMs);
        metrics.put("criticalFailureCount", criticalFailureCount);
        metrics.put("isRunningMonitoring", isRunningMonitoring.get());
        
        return metrics;
    }
    
    /**
     * Sets the monitoring interval.
     *
     * @param monitoringIntervalMs the new monitoring interval in milliseconds
     */
    public void setMonitoringInterval(long monitoringIntervalMs) {
        this.monitoringIntervalMs = monitoringIntervalMs;
        
        // Restart monitoring with new interval if already running
        if (monitoringTask != null && !monitoringTask.isDone()) {
            stopMonitoring();
            startMonitoring();
        }
    }
    
    /**
     * Sets the threshold values for alerts.
     *
     * @param warningThresholdMs the new warning threshold in milliseconds
     * @param warningFailureCount the new warning failure count
     * @param criticalThresholdMs the new critical threshold in milliseconds
     * @param criticalFailureCount the new critical failure count
     */
    public void setAlertThresholds(long warningThresholdMs, int warningFailureCount, 
                                  long criticalThresholdMs, int criticalFailureCount) {
        this.warningThresholdMs = warningThresholdMs;
        this.warningFailureCount = warningFailureCount;
        this.criticalThresholdMs = criticalThresholdMs;
        this.criticalFailureCount = criticalFailureCount;
    }
    
    /**
     * Shuts down the connection monitor.
     */
    public void shutdown() {
        // Stop monitoring
        stopMonitoring();
        
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
        
        logger.info("FTP Connection Monitor shut down");
    }
}