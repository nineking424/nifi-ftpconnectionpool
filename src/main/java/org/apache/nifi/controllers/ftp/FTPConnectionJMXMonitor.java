package org.apache.nifi.controllers.ftp;

import org.apache.nifi.logging.ComponentLog;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Exposes FTP Connection Pool metrics via JMX for monitoring.
 * This allows external monitoring systems to track the health and performance of the connection pool.
 */
public class FTPConnectionJMXMonitor {
    
    private final ComponentLog logger;
    private final FTPConnectionMetrics metrics;
    private final String mbeanName;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private FTPConnectionPoolMXBean mbean;
    
    /**
     * Creates a new JMX monitor for FTP connection metrics.
     *
     * @param metrics The metrics to expose via JMX
     * @param serviceName The name of the FTP service (for the MBean name)
     * @param logger The logger to use
     */
    public FTPConnectionJMXMonitor(FTPConnectionMetrics metrics, String serviceName, ComponentLog logger) {
        this.logger = logger;
        this.metrics = metrics;
        this.mbeanName = "org.apache.nifi.controllers.ftp:type=FTPConnectionPool,name=" + 
                sanitizeServiceName(serviceName);
    }
    
    /**
     * Registers the MBean with the JMX server.
     *
     * @return true if registration was successful, false otherwise
     */
    public boolean register() {
        if (registered.get()) {
            return true; // Already registered
        }
        
        try {
            // Create the MBean
            this.mbean = new FTPConnectionPoolMXBeanImpl(metrics);
            
            // Get the platform MBean server
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            
            // Create an ObjectName for the MBean
            ObjectName objectName = new ObjectName(mbeanName);
            
            // Check if MBean is already registered
            if (mbs.isRegistered(objectName)) {
                logger.warn("MBean already registered: {}", mbeanName);
                mbs.unregisterMBean(objectName);
            }
            
            // Register the MBean
            mbs.registerMBean(new StandardMBean(mbean, FTPConnectionPoolMXBean.class), objectName);
            
            logger.info("Registered JMX MBean: {}", mbeanName);
            registered.set(true);
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to register JMX MBean: {}", new Object[] { e.getMessage() }, e);
            return false;
        }
    }
    
    /**
     * Unregisters the MBean from the JMX server.
     */
    public void unregister() {
        if (!registered.get()) {
            return; // Not registered
        }
        
        try {
            // Get the platform MBean server
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            
            // Create an ObjectName for the MBean
            ObjectName objectName = new ObjectName(mbeanName);
            
            // Check if MBean is registered
            if (mbs.isRegistered(objectName)) {
                // Unregister the MBean
                mbs.unregisterMBean(objectName);
                logger.info("Unregistered JMX MBean: {}", mbeanName);
            }
            
            registered.set(false);
            
        } catch (Exception e) {
            logger.error("Failed to unregister JMX MBean: {}", new Object[] { e.getMessage() }, e);
        }
    }
    
    /**
     * Checks if the MBean is registered.
     *
     * @return true if the MBean is registered, false otherwise
     */
    public boolean isRegistered() {
        return registered.get();
    }
    
    /**
     * Sanitizes a service name for use in an MBean name.
     *
     * @param serviceName The service name to sanitize
     * @return The sanitized service name
     */
    private String sanitizeServiceName(String serviceName) {
        if (serviceName == null || serviceName.isEmpty()) {
            return "DefaultFTPService";
        }
        
        // Replace non-alphanumeric characters with underscores
        return serviceName.replaceAll("[^a-zA-Z0-9]", "_");
    }
    
    /**
     * MXBean interface for FTP Connection Pool metrics.
     */
    public interface FTPConnectionPoolMXBean {
        
        // Connection pool metrics
        int getTotalConnections();
        int getActiveConnections();
        int getIdleConnections();
        int getFailedConnections();
        long getConnectionsCreated();
        long getConnectionsDestroyed();
        
        // Performance metrics
        long getTotalOperations();
        long getSuccessfulOperations();
        long getFailedOperations();
        double getAverageOperationTimeMs();
        double getMaxOperationTimeMs();
        double getSuccessRate();
        
        // Throughput metrics
        long getBytesUploaded();
        long getBytesDownloaded();
        long getFilesUploaded();
        long getFilesDownloaded();
        long getUploadRateBytes();
        long getDownloadRateBytes();
        
        // Wait time metrics
        double getAverageWaitTimeMs();
        double getMaxWaitTimeMs();
        
        // Resource metrics
        long getCurrentMemoryUsageBytes();
        long getPeakMemoryUsageBytes();
        int getActiveThreads();
        
        // Health metrics
        String getHealthStatus();
        long getLastSuccessfulOperationTime();
        long getLastFailedOperationTime();
        String getLastOperationError();
        
        // Management operations
        void resetMetrics();
        Map<String, Object> getAllMetrics();
    }
    
    /**
     * Implementation of the FTP Connection Pool MXBean interface.
     */
    private static class FTPConnectionPoolMXBeanImpl implements FTPConnectionPoolMXBean {
        
        private final FTPConnectionMetrics metrics;
        
        /**
         * Creates a new MXBean implementation.
         *
         * @param metrics The metrics to expose
         */
        public FTPConnectionPoolMXBeanImpl(FTPConnectionMetrics metrics) {
            this.metrics = metrics;
        }
        
        @Override
        public int getTotalConnections() {
            return (int) metrics.getMetrics().get("connectionPool", Map.class).get("totalConnections");
        }
        
        @Override
        public int getActiveConnections() {
            return (int) metrics.getMetrics().get("connectionPool", Map.class).get("activeConnections");
        }
        
        @Override
        public int getIdleConnections() {
            return (int) metrics.getMetrics().get("connectionPool", Map.class).get("idleConnections");
        }
        
        @Override
        public int getFailedConnections() {
            return (int) metrics.getMetrics().get("connectionPool", Map.class).get("failedConnections");
        }
        
        @Override
        public long getConnectionsCreated() {
            return (long) metrics.getMetrics().get("connectionPool", Map.class).get("connectionsCreated");
        }
        
        @Override
        public long getConnectionsDestroyed() {
            return (long) metrics.getMetrics().get("connectionPool", Map.class).get("connectionsDestroyed");
        }
        
        @Override
        public long getTotalOperations() {
            return (long) metrics.getMetrics().get("performance", Map.class).get("totalOperations");
        }
        
        @Override
        public long getSuccessfulOperations() {
            return (long) metrics.getMetrics().get("performance", Map.class).get("successfulOperations");
        }
        
        @Override
        public long getFailedOperations() {
            return (long) metrics.getMetrics().get("performance", Map.class).get("failedOperations");
        }
        
        @Override
        public double getAverageOperationTimeMs() {
            return (double) metrics.getMetrics().get("performance", Map.class).get("averageOperationTimeMs");
        }
        
        @Override
        public double getMaxOperationTimeMs() {
            return (double) metrics.getMetrics().get("performance", Map.class).get("maxOperationTimeMs");
        }
        
        @Override
        public double getSuccessRate() {
            return (double) metrics.getMetrics().get("health", Map.class).get("successRate");
        }
        
        @Override
        public long getBytesUploaded() {
            return (long) metrics.getMetrics().get("throughput", Map.class).get("bytesUploaded");
        }
        
        @Override
        public long getBytesDownloaded() {
            return (long) metrics.getMetrics().get("throughput", Map.class).get("bytesDownloaded");
        }
        
        @Override
        public long getFilesUploaded() {
            return (long) metrics.getMetrics().get("throughput", Map.class).get("filesUploaded");
        }
        
        @Override
        public long getFilesDownloaded() {
            return (long) metrics.getMetrics().get("throughput", Map.class).get("filesDownloaded");
        }
        
        @Override
        public long getUploadRateBytes() {
            return (long) metrics.getMetrics().get("throughput", Map.class).get("uploadRateBytes");
        }
        
        @Override
        public long getDownloadRateBytes() {
            return (long) metrics.getMetrics().get("throughput", Map.class).get("downloadRateBytes");
        }
        
        @Override
        public double getAverageWaitTimeMs() {
            return (double) metrics.getMetrics().get("waitTime", Map.class).get("averageWaitTimeMs");
        }
        
        @Override
        public double getMaxWaitTimeMs() {
            return (double) metrics.getMetrics().get("waitTime", Map.class).get("maxWaitTimeMs");
        }
        
        @Override
        public long getCurrentMemoryUsageBytes() {
            return (long) metrics.getMetrics().get("resources", Map.class).get("currentMemoryUsageBytes");
        }
        
        @Override
        public long getPeakMemoryUsageBytes() {
            return (long) metrics.getMetrics().get("resources", Map.class).get("peakMemoryUsageBytes");
        }
        
        @Override
        public int getActiveThreads() {
            return (int) metrics.getMetrics().get("resources", Map.class).get("activeThreads");
        }
        
        @Override
        public String getHealthStatus() {
            // Calculate health status based on success rate and connection health
            double successRate = getSuccessRate();
            int failedConns = getFailedConnections();
            
            if (successRate >= 99.0 && failedConns == 0) {
                return "EXCELLENT";
            } else if (successRate >= 95.0 && failedConns <= 1) {
                return "GOOD";
            } else if (successRate >= 90.0 && failedConns <= 2) {
                return "FAIR";
            } else if (successRate >= 80.0) {
                return "DEGRADED";
            } else {
                return "POOR";
            }
        }
        
        @Override
        public long getLastSuccessfulOperationTime() {
            Object timestamp = metrics.getMetrics().get("health", Map.class).get("lastSuccessfulOperationTime");
            if (timestamp instanceof java.util.Date) {
                return ((java.util.Date) timestamp).getTime();
            }
            return 0;
        }
        
        @Override
        public long getLastFailedOperationTime() {
            Object timestamp = metrics.getMetrics().get("health", Map.class).get("lastFailedOperationTime");
            if (timestamp instanceof java.util.Date) {
                return ((java.util.Date) timestamp).getTime();
            }
            return 0;
        }
        
        @Override
        public String getLastOperationError() {
            return (String) metrics.getMetrics().get("health", Map.class).get("lastOperationError");
        }
        
        @Override
        public void resetMetrics() {
            metrics.reset();
        }
        
        @Override
        public Map<String, Object> getAllMetrics() {
            Map<String, Object> allMetrics = metrics.getMetrics();
            
            // Create a flattened map for JMX compatibility
            Map<String, Object> flattenedMetrics = new HashMap<>();
            flattenMetrics(allMetrics, "", flattenedMetrics);
            
            return flattenedMetrics;
        }
        
        /**
         * Flattens a hierarchical metrics map into a single-level map.
         *
         * @param metrics The metrics map to flatten
         * @param prefix The prefix to use for keys
         * @param result The result map to populate
         */
        @SuppressWarnings("unchecked")
        private void flattenMetrics(Map<String, Object> metrics, String prefix, Map<String, Object> result) {
            for (Map.Entry<String, Object> entry : metrics.entrySet()) {
                String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                Object value = entry.getValue();
                
                if (value instanceof Map) {
                    // Recurse into nested maps
                    flattenMetrics((Map<String, Object>) value, key, result);
                } else {
                    // Add leaf value to result
                    result.put(key, value);
                }
            }
        }
    }
}