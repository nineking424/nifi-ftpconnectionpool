package org.apache.nifi.controllers.ftp;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessorNode;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.BulletinFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Monitors FTP connection metrics and reports issues to NiFi bulletins.
 * This helps operators identify and diagnose problems with the FTP connection pool.
 */
public class FTPConnectionBulletinReporter {
    
    private final ComponentLog logger;
    private final FTPConnectionMetrics metrics;
    private final ProcessorNode processorNode;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, ThresholdRule> thresholdRules = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReportTimeMap = new ConcurrentHashMap<>();
    
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> monitorTask;
    private long monitoringIntervalMs = 60000; // Default to 1 minute
    private long throttleIntervalMs = 300000; // Default to 5 minutes
    
    /**
     * Creates a new bulletin reporter for FTP connection metrics.
     *
     * @param metrics The metrics to monitor
     * @param processorNode The processor node to report bulletins to
     * @param logger The logger to use
     */
    public FTPConnectionBulletinReporter(FTPConnectionMetrics metrics, ProcessorNode processorNode, ComponentLog logger) {
        this.metrics = metrics;
        this.processorNode = processorNode;
        this.logger = logger;
        
        // Add default threshold rules
        addDefaultThresholdRules();
    }
    
    /**
     * Adds default threshold rules for common metrics.
     */
    private void addDefaultThresholdRules() {
        // Error rate threshold
        addThresholdRule("errorRate", metrics -> {
            Map<String, Object> healthMetrics = getNestedMap(metrics.getMetrics(), "health");
            if (healthMetrics != null) {
                double errorRate = 100.0 - (double) healthMetrics.get("successRate");
                return errorRate;
            }
            return 0.0;
        }, 5.0, 10.0, 20.0, "Error rate is high: %.1f%%");
        
        // Failed connections threshold
        addThresholdRule("failedConnections", metrics -> {
            Map<String, Object> poolMetrics = getNestedMap(metrics.getMetrics(), "connectionPool");
            if (poolMetrics != null) {
                return (int) poolMetrics.get("failedConnections");
            }
            return 0;
        }, 1, 3, 5, "Failed connection count is high: %d");
        
        // Connection wait time threshold
        addThresholdRule("waitTime", metrics -> {
            Map<String, Object> waitMetrics = getNestedMap(metrics.getMetrics(), "waitTime");
            if (waitMetrics != null) {
                return (double) waitMetrics.get("averageWaitTimeMs");
            }
            return 0.0;
        }, 500.0, 1000.0, 5000.0, "Connection wait time is high: %.1f ms");
        
        // Operation time threshold
        addThresholdRule("operationTime", metrics -> {
            Map<String, Object> perfMetrics = getNestedMap(metrics.getMetrics(), "performance");
            if (perfMetrics != null) {
                return (double) perfMetrics.get("averageOperationTimeMs");
            }
            return 0.0;
        }, 1000.0, 5000.0, 10000.0, "Operation time is high: %.1f ms");
        
        // Memory usage threshold
        addThresholdRule("memoryUsage", metrics -> {
            Map<String, Object> resourceMetrics = getNestedMap(metrics.getMetrics(), "resources");
            if (resourceMetrics != null) {
                long currentMemory = (long) resourceMetrics.get("currentMemoryUsageBytes");
                long peakMemory = (long) resourceMetrics.get("peakMemoryUsageBytes");
                // Return the current usage as a percentage of the peak
                return peakMemory > 0 ? ((double) currentMemory / peakMemory) * 100.0 : 0.0;
            }
            return 0.0;
        }, 70.0, 85.0, 95.0, "Memory usage is high: %.1f%% of peak");
    }
    
    /**
     * Adds a threshold rule for a metric.
     *
     * @param <T> The type of the metric value
     * @param name The name of the rule
     * @param metricExtractor A function that extracts the metric value from the metrics
     * @param warnThreshold The threshold for warning bulletins
     * @param errorThreshold The threshold for error bulletins
     * @param severeThreshold The threshold for severe bulletins
     * @param messageTemplate The message template for bulletins
     */
    public <T extends Comparable<T>> void addThresholdRule(String name, MetricExtractor<T> metricExtractor,
            T warnThreshold, T errorThreshold, T severeThreshold, String messageTemplate) {
        thresholdRules.put(name, new ThresholdRule<>(metricExtractor, warnThreshold, errorThreshold, severeThreshold, messageTemplate));
    }
    
    /**
     * Removes a threshold rule.
     *
     * @param name The name of the rule to remove
     */
    public void removeThresholdRule(String name) {
        thresholdRules.remove(name);
    }
    
    /**
     * Starts the bulletin reporter.
     *
     * @param initialDelayMs The initial delay before starting monitoring in milliseconds
     * @param intervalMs The interval between monitoring runs in milliseconds
     */
    public void start(long initialDelayMs, long intervalMs) {
        if (running.compareAndSet(false, true)) {
            this.monitoringIntervalMs = intervalMs;
            
            // Create a single-threaded scheduler
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("FTP-BulletinReporter");
                t.setDaemon(true);
                return t;
            });
            
            // Schedule the monitoring task
            this.monitorTask = scheduler.scheduleAtFixedRate(
                    this::checkMetricsAndReport,
                    initialDelayMs,
                    intervalMs,
                    TimeUnit.MILLISECONDS);
            
            logger.info("Started FTP Connection Bulletin Reporter with interval: {} ms", intervalMs);
        }
    }
    
    /**
     * Stops the bulletin reporter.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            // Cancel the monitoring task
            if (monitorTask != null) {
                monitorTask.cancel(false);
                monitorTask = null;
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
            
            logger.info("Stopped FTP Connection Bulletin Reporter");
        }
    }
    
    /**
     * Checks metrics against thresholds and reports bulletins if needed.
     */
    private void checkMetricsAndReport() {
        if (!running.get() || processorNode == null) {
            return;
        }
        
        try {
            // Check each threshold rule
            for (Map.Entry<String, ThresholdRule> entry : thresholdRules.entrySet()) {
                String ruleName = entry.getKey();
                ThresholdRule rule = entry.getValue();
                
                // Check the rule
                Severity severity = rule.checkThreshold(metrics);
                if (severity != null) {
                    // Generate the bulletin message
                    String message = rule.generateMessage(metrics);
                    
                    // Check if we should throttle this bulletin
                    if (shouldReportBulletin(ruleName, severity)) {
                        // Report the bulletin
                        reportBulletin(severity, message);
                        lastReportTimeMap.put(ruleName, System.currentTimeMillis());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error checking metrics and reporting bulletins: {}", new Object[] { e.getMessage() }, e);
        }
    }
    
    /**
     * Checks if a bulletin should be reported based on throttling rules.
     *
     * @param ruleName The name of the rule
     * @param severity The severity of the bulletin
     * @return true if the bulletin should be reported, false otherwise
     */
    private boolean shouldReportBulletin(String ruleName, Severity severity) {
        // Get the last report time for this rule
        Long lastReportTime = lastReportTimeMap.get(ruleName);
        if (lastReportTime == null) {
            return true; // First report for this rule
        }
        
        // Calculate the time since the last report
        long timeSinceLastReport = System.currentTimeMillis() - lastReportTime;
        
        // Determine the throttle interval based on severity
        long effectiveThrottleMs = throttleIntervalMs;
        switch (severity) {
            case WARNING:
                effectiveThrottleMs = throttleIntervalMs;
                break;
            case ERROR:
                effectiveThrottleMs = throttleIntervalMs / 2; // Report errors more frequently
                break;
            case CRITICAL:
                effectiveThrottleMs = throttleIntervalMs / 4; // Report critical issues even more frequently
                break;
        }
        
        // Check if the throttle interval has elapsed
        return timeSinceLastReport >= effectiveThrottleMs;
    }
    
    /**
     * Reports a bulletin to the NiFi bulletin repository.
     *
     * @param severity The severity of the bulletin
     * @param message The message for the bulletin
     */
    private void reportBulletin(Severity severity, String message) {
        try {
            // Get the bulletin repository
            BulletinRepository bulletinRepository = processorNode.getProcessGroup().getBulletinRepository();
            
            // Create and add the bulletin
            bulletinRepository.addBulletin(BulletinFactory.createBulletin(
                    "FTP Connection Pool",
                    severity,
                    message));
            
            logger.info("Reported FTP Connection bulletin: {} - {}", new Object[] { severity, message });
            
        } catch (Exception e) {
            logger.error("Error reporting bulletin: {}", new Object[] { e.getMessage() }, e);
        }
    }
    
    /**
     * Sets the throttle interval for bulletins.
     *
     * @param throttleIntervalMs The throttle interval in milliseconds
     */
    public void setThrottleInterval(long throttleIntervalMs) {
        this.throttleIntervalMs = throttleIntervalMs;
    }
    
    /**
     * Sets the monitoring interval.
     *
     * @param monitoringIntervalMs The monitoring interval in milliseconds
     */
    public void setMonitoringInterval(long monitoringIntervalMs) {
        this.monitoringIntervalMs = monitoringIntervalMs;
        
        // If running, restart with the new interval
        if (running.get() && monitorTask != null) {
            monitorTask.cancel(false);
            monitorTask = scheduler.scheduleAtFixedRate(
                    this::checkMetricsAndReport,
                    0,
                    monitoringIntervalMs,
                    TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Checks if the reporter is running.
     *
     * @return true if the reporter is running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
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
    
    /**
     * Functional interface for extracting a metric value from the metrics.
     *
     * @param <T> The type of the metric value
     */
    @FunctionalInterface
    public interface MetricExtractor<T> {
        /**
         * Extracts a metric value from the metrics.
         *
         * @param metrics The metrics to extract from
         * @return The extracted metric value
         */
        T extractMetric(FTPConnectionMetrics metrics);
    }
    
    /**
     * A rule for checking a metric against thresholds.
     *
     * @param <T> The type of the metric value
     */
    private static class ThresholdRule<T extends Comparable<T>> {
        private final MetricExtractor<T> metricExtractor;
        private final T warnThreshold;
        private final T errorThreshold;
        private final T severeThreshold;
        private final String messageTemplate;
        
        /**
         * Creates a new threshold rule.
         *
         * @param metricExtractor The function to extract the metric value
         * @param warnThreshold The threshold for warning bulletins
         * @param errorThreshold The threshold for error bulletins
         * @param severeThreshold The threshold for severe bulletins
         * @param messageTemplate The message template for bulletins
         */
        public ThresholdRule(MetricExtractor<T> metricExtractor, T warnThreshold,
                T errorThreshold, T severeThreshold, String messageTemplate) {
            this.metricExtractor = metricExtractor;
            this.warnThreshold = warnThreshold;
            this.errorThreshold = errorThreshold;
            this.severeThreshold = severeThreshold;
            this.messageTemplate = messageTemplate;
        }
        
        /**
         * Checks the metric against thresholds.
         *
         * @param metrics The metrics to check
         * @return The severity level if a threshold is exceeded, or null if no thresholds are exceeded
         */
        public Severity checkThreshold(FTPConnectionMetrics metrics) {
            T value = metricExtractor.extractMetric(metrics);
            
            if (value.compareTo(severeThreshold) >= 0) {
                return Severity.CRITICAL;
            } else if (value.compareTo(errorThreshold) >= 0) {
                return Severity.ERROR;
            } else if (value.compareTo(warnThreshold) >= 0) {
                return Severity.WARNING;
            }
            
            return null; // No threshold exceeded
        }
        
        /**
         * Generates a bulletin message for the current metric value.
         *
         * @param metrics The metrics to use
         * @return The formatted message
         */
        public String generateMessage(FTPConnectionMetrics metrics) {
            T value = metricExtractor.extractMetric(metrics);
            
            // Format the message based on the metric type
            if (value instanceof Integer) {
                return String.format(messageTemplate, ((Integer) value).intValue());
            } else if (value instanceof Long) {
                return String.format(messageTemplate, ((Long) value).longValue());
            } else if (value instanceof Double) {
                return String.format(messageTemplate, ((Double) value).doubleValue());
            } else if (value instanceof Float) {
                return String.format(messageTemplate, ((Float) value).floatValue());
            } else if (value instanceof Boolean) {
                return String.format(messageTemplate, ((Boolean) value).booleanValue());
            } else {
                return String.format(messageTemplate, value.toString());
            }
        }
    }
}