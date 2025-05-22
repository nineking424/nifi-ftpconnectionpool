package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of FTPConnectionPool using Apache Commons Pool2.
 */
public class FTPConnectionPoolImpl implements FTPConnectionPool {
    private final FTPConnectionPoolConfig poolConfig;
    private final FTPConnectionManager connectionManager;
    private final ComponentLog logger;
    private final GenericObjectPool<FTPClient> pool;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    
    // Connection health management
    private FTPConnectionHealthManager healthManager;
    private FTPConnectionMonitor connectionMonitor;
    private FTPKeepAliveManager keepAliveManager;
    private FTPConnectionTester connectionTester;
    
    // Metrics tracking
    private final ConcurrentHashMap<FTPClient, String> connectionTags = new ConcurrentHashMap<>();
    private final AtomicLong totalBorrows = new AtomicLong(0);
    private final AtomicLong totalReturns = new AtomicLong(0);
    private final AtomicLong totalInvalidations = new AtomicLong(0);
    private final AtomicLong createdConnections = new AtomicLong(0);
    private final AtomicLong destroyedConnections = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> customMetrics = new ConcurrentHashMap<>();
    
    /**
     * Creates a new FTP connection pool with the given configuration and logger.
     *
     * @param poolConfig the pool configuration
     * @param logger the logger to use
     */
    public FTPConnectionPoolImpl(FTPConnectionPoolConfig poolConfig, ComponentLog logger) {
        this.poolConfig = poolConfig;
        this.logger = logger;
        
        // Create the connection manager
        this.connectionManager = new FTPConnectionManager(
                poolConfig.getConnectionConfig(),
                logger);
        
        // Create the connection factory
        FTPConnectionFactory factory = new FTPConnectionFactory();
        
        // Create the pool configuration
        GenericObjectPoolConfig<FTPClient> objectPoolConfig = poolConfig.createPoolConfig();
        
        // Create the connection pool
        this.pool = new GenericObjectPool<>(factory, objectPoolConfig);
        
        // Initialize health management components
        initializeHealthManagement();
        
        logger.info("Created FTP connection pool with max connections: {}, min idle: {}",
                new Object[] { objectPoolConfig.getMaxTotal(), objectPoolConfig.getMinIdle() });
    }
    
    @Override
    public FTPClient borrowConnection() throws IOException, FTPConnectionException {
        if (shutdown.get()) {
            throw new IllegalStateException("Cannot borrow connection from closed pool");
        }
        
        try {
            logger.debug("Borrowing connection from pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
            // Borrow a connection from the pool
            FTPClient client = pool.borrowObject();
            
            // Update metrics
            totalBorrows.incrementAndGet();
            
            // Verify the borrowed connection is healthy
            if (healthManager != null) {
                if (!healthManager.testConnection(client)) {
                    // Connection is not healthy, invalidate it and try again
                    logger.warn("Borrowed unhealthy connection, invalidating and trying again");
                    invalidateConnection(client);
                    
                    // Recursively try again - IMPORTANT: There's a potential for 
                    // infinite recursion here if all connections are unhealthy
                    return borrowConnection();
                }
            }
            
            // Register connection with keep-alive manager if available
            if (keepAliveManager != null) {
                // In a real implementation, we'd find the FTPConnection object for this client
                // and register it with the keep-alive manager
            }
            
            logger.debug("Borrowed connection from pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
            return client;
            
        } catch (Exception e) {
            if (e instanceof FTPConnectionException) {
                throw (FTPConnectionException) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new FTPConnectionException(
                        FTPConnectionException.ErrorType.POOL_ERROR,
                        poolConfig.getConnectionConfig().getHostname(),
                        poolConfig.getConnectionConfig().getPort(),
                        0,
                        "Failed to borrow connection from pool: " + e.getMessage());
            }
        }
    }
    
    @Override
    public void returnConnection(FTPClient client) {
        if (client == null) {
            return;
        }
        
        if (shutdown.get()) {
            // Pool is closed, just close the connection
            safeCloseConnection(client);
            return;
        }
        
        try {
            logger.debug("Returning connection to pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
            // Check if the connection is still healthy before returning to pool
            boolean isHealthy = true;
            if (healthManager != null) {
                FTPConnectionHealth.HealthStatus status = healthManager.getConnectionStatus(client);
                isHealthy = (status == FTPConnectionHealth.HealthStatus.HEALTHY);
            }
            
            if (!isHealthy) {
                // Connection is not healthy, invalidate instead of returning to pool
                logger.warn("Returning unhealthy connection, invalidating instead");
                invalidateConnection(client);
                return;
            }
            
            // Return the connection to the pool
            pool.returnObject(client);
            
            // Update metrics
            totalReturns.incrementAndGet();
            
            logger.debug("Returned connection to pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
        } catch (Exception e) {
            logger.warn("Error returning connection to pool: {}", new Object[] { e.getMessage() });
            
            // If we can't return it, at least try to close it
            safeCloseConnection(client);
        }
    }
    
    @Override
    public void invalidateConnection(FTPClient client) {
        if (client == null) {
            return;
        }
        
        if (shutdown.get()) {
            // Pool is closed, just close the connection
            safeCloseConnection(client);
            return;
        }
        
        try {
            logger.debug("Invalidating connection in pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
            // Invalidate the connection in the pool
            pool.invalidateObject(client);
            
            // Update metrics
            totalInvalidations.incrementAndGet();
            
            logger.debug("Invalidated connection in pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
        } catch (Exception e) {
            logger.warn("Error invalidating connection in pool: {}", new Object[] { e.getMessage() });
            
            // If we can't invalidate it through the pool, at least try to close it
            safeCloseConnection(client);
        }
    }
    
    @Override
    public int getActiveConnectionCount() {
        return pool.getNumActive();
    }
    
    @Override
    public int getIdleConnectionCount() {
        return pool.getNumIdle();
    }
    
    @Override
    public int getMaxConnections() {
        return pool.getMaxTotal();
    }
    
    @Override
    public int getMinConnections() {
        return pool.getMinIdle();
    }
    
    @Override
    public Map<String, Object> getPoolMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Pool size metrics
        metrics.put("activeConnections", pool.getNumActive());
        metrics.put("idleConnections", pool.getNumIdle());
        metrics.put("maxConnections", pool.getMaxTotal());
        metrics.put("minIdleConnections", pool.getMinIdle());
        
        // Usage metrics
        metrics.put("totalBorrows", totalBorrows.get());
        metrics.put("totalReturns", totalReturns.get());
        metrics.put("totalInvalidations", totalInvalidations.get());
        metrics.put("createdConnections", createdConnections.get());
        metrics.put("destroyedConnections", destroyedConnections.get());
        
        // Pool state
        metrics.put("isShutdown", shutdown.get());
        metrics.put("waitingThreadCount", pool.getNumWaiters());
        metrics.put("meanActiveTime", pool.getMeanActiveTimeMillis());
        metrics.put("meanIdleTime", pool.getMeanIdleTimeMillis());
        metrics.put("meanBorrowWaitTime", pool.getMeanBorrowWaitTimeMillis());
        
        // Add any custom metrics
        for (Map.Entry<String, AtomicLong> entry : customMetrics.entrySet()) {
            metrics.put(entry.getKey(), entry.getValue().get());
        }
        
        // Add health management metrics
        if (healthManager != null) {
            metrics.put("healthMetrics", healthManager.getHealthMetrics());
        }
        
        if (connectionMonitor != null) {
            metrics.put("monitoringMetrics", connectionMonitor.getMonitoringMetrics());
        }
        
        if (keepAliveManager != null) {
            metrics.put("keepAliveMetrics", keepAliveManager.getKeepAliveMetrics());
        }
        
        return metrics;
    }
    
    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("Shutting down FTP connection pool");
            
            try {
                // Shutdown health management components
                if (connectionMonitor != null) {
                    connectionMonitor.stopMonitoring();
                    connectionMonitor = null;
                }
                
                if (keepAliveManager != null) {
                    keepAliveManager.shutdown();
                    keepAliveManager = null;
                }
                
                if (healthManager != null) {
                    healthManager.shutdown();
                    healthManager = null;
                }
                
                // Close the pool
                pool.close();
                logger.debug("Connection pool closed");
                
                // Shutdown the connection manager
                connectionManager.shutdown();
                logger.debug("Connection manager shut down");
                
            } catch (Exception e) {
                logger.warn("Error during FTP connection pool shutdown: {}", new Object[] { e.getMessage() });
            }
        }
    }
    
    @Override
    public void clear() {
        if (shutdown.get()) {
            return;
        }
        
        try {
            logger.debug("Clearing connection pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
            // Clear the pool (close idle connections)
            pool.clear();
            
            logger.debug("Cleared connection pool (active: {}, idle: {})",
                    new Object[] { pool.getNumActive(), pool.getNumIdle() });
            
        } catch (Exception e) {
            logger.warn("Error clearing connection pool: {}", new Object[] { e.getMessage() });
        }
    }
    
    @Override
    public void refreshIdleConnections() {
        if (shutdown.get()) {
            return;
        }
        
        try {
            logger.debug("Refreshing idle connections in pool (idle: {})", pool.getNumIdle());
            
            // This will test all idle objects in the pool and destroy invalid ones
            pool.preparePool();
            
            // Also trigger a health management maintenance cycle
            if (healthManager != null) {
                int repairedCount = healthManager.performMaintenanceCycle();
                if (repairedCount > 0) {
                    logger.info("Repaired {} connections during refresh", repairedCount);
                }
            }
            
            logger.debug("Refreshed idle connections in pool (idle: {})", pool.getNumIdle());
            
        } catch (Exception e) {
            logger.warn("Error refreshing idle connections: {}", new Object[] { e.getMessage() });
        }
    }
    
    /**
     * Safely closes a connection without throwing exceptions.
     *
     * @param client the FTPClient to close
     */
    private void safeCloseConnection(FTPClient client) {
        try {
            connectionManager.closeConnection(client);
        } catch (Exception e) {
            logger.debug("Error closing connection: {}", new Object[] { e.getMessage() });
        }
    }
    
    /**
     * Associates a tag with a connection.
     *
     * @param client the FTPClient to tag
     * @param tag the tag to associate
     */
    public void tagConnection(FTPClient client, String tag) {
        if (client != null && tag != null) {
            connectionTags.put(client, tag);
        }
    }
    
    /**
     * Gets the tag associated with a connection.
     *
     * @param client the FTPClient to get the tag for
     * @return the tag, or null if none
     */
    public String getConnectionTag(FTPClient client) {
        return connectionTags.get(client);
    }
    
    /**
     * Adds or updates a custom metric.
     *
     * @param name the metric name
     * @param value the metric value
     */
    public void setCustomMetric(String name, long value) {
        customMetrics.computeIfAbsent(name, k -> new AtomicLong(0)).set(value);
    }
    
    /**
     * Increments a custom metric.
     *
     * @param name the metric name
     * @return the new value
     */
    public long incrementCustomMetric(String name) {
        return customMetrics.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Gets the connection manager used by this pool.
     *
     * @return the connection manager
     */
    protected FTPConnectionManager getConnectionManager() {
        return connectionManager;
    }
    
    /**
     * Factory for creating and managing FTPClient objects in the pool.
     */
    private class FTPConnectionFactory extends BasePooledObjectFactory<FTPClient> {
        
        @Override
        public FTPClient create() throws Exception {
            // Create a new FTP connection
            FTPClient client = connectionManager.createConnection();
            
            // Update metrics
            createdConnections.incrementAndGet();
            
            logger.debug("Created new FTP connection");
            return client;
        }
        
        @Override
        public PooledObject<FTPClient> wrap(FTPClient client) {
            return new DefaultPooledObject<>(client);
        }
        
        @Override
        public boolean validateObject(PooledObject<FTPClient> pooledObject) {
            FTPClient client = pooledObject.getObject();
            
            // Validate the connection
            boolean valid = connectionManager.validateConnection(client);
            
            if (valid) {
                logger.debug("Validated FTP connection (created: {}, active: {})",
                        pooledObject.getCreateTime(), pooledObject.getLastBorrowTime());
            } else {
                logger.debug("FTP connection validation failed");
            }
            
            return valid;
        }
        
        @Override
        public void destroyObject(PooledObject<FTPClient> pooledObject) throws Exception {
            FTPClient client = pooledObject.getObject();
            
            // Remove any tags associated with this connection
            connectionTags.remove(client);
            
            // Close the connection
            connectionManager.closeConnection(client);
            
            // Update metrics
            destroyedConnections.incrementAndGet();
            
            logger.debug("Destroyed FTP connection (created: {}, active: {})",
                    pooledObject.getCreateTime(), pooledObject.getLastBorrowTime());
        }
        
        @Override
        public void activateObject(PooledObject<FTPClient> pooledObject) throws Exception {
            // Called when a connection is borrowed from the pool
            FTPClient client = pooledObject.getObject();
            
            // Reset the client to a known good state if needed
            // For FTP, we may want to ensure we're in the root directory
            try {
                client.changeWorkingDirectory("/");
            } catch (IOException e) {
                logger.debug("Error changing to root directory during activation: {}", 
                        new Object[] { e.getMessage() });
                // We don't fail here as the connection may still be usable
            }
            
            logger.debug("Activated FTP connection (idle time: {}ms)",
                    pooledObject.getIdleTimeMillis());
        }
        
        @Override
        public void passivateObject(PooledObject<FTPClient> pooledObject) throws Exception {
            // Called when a connection is returned to the pool
            // For FTP, we don't need to do anything special here
            logger.debug("Passivated FTP connection (active time: {}ms)",
                    pooledObject.getActiveTimeMillis());
        }
    }
    
    /**
     * Initializes the health management components.
     */
    private void initializeHealthManagement() {
        // Determine health check intervals from pool config
        long idleTimeout = poolConfig.getConnectionConfig().getConnectionIdleTimeout();
        long keepAliveInterval = poolConfig.getConnectionConfig().getKeepAliveInterval();
        
        // Use reasonable defaults if not configured
        long healthCheckInterval = Math.min(
            idleTimeout > 0 ? idleTimeout / 4 : 60000,
            keepAliveInterval > 0 ? keepAliveInterval / 2 : 30000
        );
        healthCheckInterval = Math.max(healthCheckInterval, 15000); // At least 15 seconds
        
        // Create health manager
        this.healthManager = new FTPConnectionHealthManager(
                connectionManager,
                logger,
                idleTimeout > 0 ? idleTimeout : 300000, // 5 minutes default
                idleTimeout > 0 ? idleTimeout / 3 : 60000, // 1 minute default
                healthCheckInterval,
                30000, // 30 seconds between repair attempts
                3, // 3 max repair attempts
                100 // 100 max error history entries
        );
        
        // Create connection monitor
        this.connectionMonitor = new FTPConnectionMonitor(
                healthManager,
                connectionManager,
                logger,
                idleTimeout > 0 ? idleTimeout / 3 : 60000 // Check every minute by default
        );
        
        // Create keep-alive manager
        this.keepAliveManager = new FTPKeepAliveManager(
                healthManager,
                logger,
                keepAliveInterval > 0 ? keepAliveInterval : 60000 // 1 minute default
        );
        
        // Create connection tester
        this.connectionTester = new FTPConnectionTester(logger);
        
        // Start health monitoring and keep-alive
        connectionMonitor.startMonitoring();
        keepAliveManager.startKeepAlive();
        
        logger.info("Initialized FTP connection health management with healthCheckInterval={} ms, " +
                "keepAliveInterval={} ms", healthCheckInterval, 
                keepAliveInterval > 0 ? keepAliveInterval : 60000);
    }
    
    /**
     * Gets the health manager.
     * 
     * @return the health manager
     */
    public FTPConnectionHealthManager getHealthManager() {
        return healthManager;
    }
    
    /**
     * Gets the connection monitor.
     * 
     * @return the connection monitor
     */
    public FTPConnectionMonitor getConnectionMonitor() {
        return connectionMonitor;
    }
    
    /**
     * Gets the keep-alive manager.
     * 
     * @return the keep-alive manager
     */
    public FTPKeepAliveManager getKeepAliveManager() {
        return keepAliveManager;
    }
    
    /**
     * Gets the connection tester.
     * 
     * @return the connection tester
     */
    public FTPConnectionTester getConnectionTester() {
        return connectionTester;
    }
    
    /**
     * Tests a connection using the connection tester.
     * 
     * @param connection the connection to test
     * @param fullTest whether to perform a full test with file operations
     * @return the test result
     */
    public FTPConnectionTester.TestResult testConnection(FTPConnection connection, boolean fullTest) {
        if (connectionTester == null) {
            throw new IllegalStateException("Connection tester is not available");
        }
        return connectionTester.testConnection(connection, fullTest, 30000); // 30-second timeout
    }
}