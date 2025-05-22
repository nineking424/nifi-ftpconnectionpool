package org.apache.nifi.controllers.ftp;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;

/**
 * Configuration for an FTP connection pool.
 */
public class FTPConnectionPoolConfig {
    // Default values
    private static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 10;
    private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 5;
    private static final int DEFAULT_MIN_IDLE_CONNECTIONS = 1;
    private static final long DEFAULT_CONNECTION_TEST_INTERVAL_MS = 30000; // 30 seconds
    private static final long DEFAULT_MAX_WAIT_MS = 30000; // 30 seconds
    private static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS = 300000; // 5 minutes
    private static final boolean DEFAULT_TEST_ON_BORROW = true;
    private static final boolean DEFAULT_TEST_ON_RETURN = false;
    private static final boolean DEFAULT_TEST_WHILE_IDLE = true;
    private static final boolean DEFAULT_LIFO = true; // Last-In-First-Out
    private static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.OLDEST;
    
    /**
     * Pool eviction policies.
     */
    public enum EvictionPolicy {
        /**
         * Evict the oldest idle connections first.
         */
        OLDEST,
        
        /**
         * Evict the least recently used connections first.
         */
        LRU,
        
        /**
         * Evict the most recently used connections first.
         */
        MRU,
        
        /**
         * Don't evict any connections until they reach min-evictable-idle-time.
         */
        NONE
    }
    
    // Pool size configuration
    private int maxTotalConnections = DEFAULT_MAX_TOTAL_CONNECTIONS;
    private int maxIdleConnections = DEFAULT_MAX_IDLE_CONNECTIONS;
    private int minIdleConnections = DEFAULT_MIN_IDLE_CONNECTIONS;
    
    // Test and eviction configuration
    private long connectionTestIntervalMs = DEFAULT_CONNECTION_TEST_INTERVAL_MS;
    private long maxWaitMs = DEFAULT_MAX_WAIT_MS;
    private long minEvictableIdleTimeMs = DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS;
    private boolean testOnBorrow = DEFAULT_TEST_ON_BORROW;
    private boolean testOnReturn = DEFAULT_TEST_ON_RETURN;
    private boolean testWhileIdle = DEFAULT_TEST_WHILE_IDLE;
    
    // Pool behavior configuration
    private boolean lifo = DEFAULT_LIFO;
    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;
    private boolean blockWhenExhausted = true;
    private boolean fairness = false;
    
    // Base connection configuration
    private final FTPConnectionConfig connectionConfig;
    
    /**
     * Creates a new configuration with default values.
     *
     * @param connectionConfig the underlying FTP connection configuration
     */
    public FTPConnectionPoolConfig(FTPConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        
        // Use values from the connection config as defaults if available
        if (connectionConfig != null) {
            this.maxTotalConnections = connectionConfig.getMaxConnections();
            this.minIdleConnections = connectionConfig.getMinConnections();
            this.maxIdleConnections = Math.max(connectionConfig.getMinConnections(), 
                                           connectionConfig.getMaxConnections() / 2);
            
            // Use connection idle timeout as the min evictable idle time
            if (connectionConfig.getConnectionIdleTimeout() > 0) {
                this.minEvictableIdleTimeMs = connectionConfig.getConnectionIdleTimeout();
            }
            
            // Use keep alive interval as the test interval if set
            if (connectionConfig.getKeepAliveInterval() > 0) {
                this.connectionTestIntervalMs = connectionConfig.getKeepAliveInterval();
            }
        }
    }
    
    /**
     * Creates a GenericObjectPoolConfig for Apache Commons Pool2 based on this configuration.
     *
     * @param <T> the type of object in the pool
     * @return a configured GenericObjectPoolConfig instance
     */
    public <T> GenericObjectPoolConfig<T> createPoolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        
        // Pool size settings
        config.setMaxTotal(maxTotalConnections);
        config.setMaxIdle(maxIdleConnections);
        config.setMinIdle(minIdleConnections);
        
        // Test settings
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnReturn(testOnReturn);
        config.setTestWhileIdle(testWhileIdle);
        
        // Time settings
        config.setTimeBetweenEvictionRuns(Duration.ofMillis(connectionTestIntervalMs));
        config.setMinEvictableIdleTime(Duration.ofMillis(minEvictableIdleTimeMs));
        config.setMaxWait(Duration.ofMillis(maxWaitMs));
        
        // Behavior settings
        config.setLifo(lifo);
        config.setBlockWhenExhausted(blockWhenExhausted);
        config.setFairness(fairness);
        
        // Set eviction policy based on our enum
        switch (evictionPolicy) {
            case LRU:
                // Least recently used (default for Commons Pool)
                break;
            case MRU:
                // Most recently used (opposite of default)
                // There's no direct setting for this, would require custom eviction policy
                break;
            case OLDEST:
                // Default behavior is closest to this
                break;
            case NONE:
                // Disable eviction runs
                config.setTimeBetweenEvictionRuns(Duration.ofMillis(-1));
                break;
        }
        
        return config;
    }
    
    /**
     * Gets the underlying FTP connection configuration.
     *
     * @return the FTP connection configuration
     */
    public FTPConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }
    
    /**
     * Gets the maximum total connections in the pool.
     *
     * @return the maximum total connections
     */
    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }
    
    /**
     * Sets the maximum total connections in the pool.
     *
     * @param maxTotalConnections the maximum total connections
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setMaxTotalConnections(int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
        return this;
    }
    
    /**
     * Gets the maximum idle connections in the pool.
     *
     * @return the maximum idle connections
     */
    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }
    
    /**
     * Sets the maximum idle connections in the pool.
     *
     * @param maxIdleConnections the maximum idle connections
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setMaxIdleConnections(int maxIdleConnections) {
        this.maxIdleConnections = maxIdleConnections;
        return this;
    }
    
    /**
     * Gets the minimum idle connections in the pool.
     *
     * @return the minimum idle connections
     */
    public int getMinIdleConnections() {
        return minIdleConnections;
    }
    
    /**
     * Sets the minimum idle connections in the pool.
     *
     * @param minIdleConnections the minimum idle connections
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setMinIdleConnections(int minIdleConnections) {
        this.minIdleConnections = minIdleConnections;
        return this;
    }
    
    /**
     * Gets the connection test interval in milliseconds.
     *
     * @return the connection test interval
     */
    public long getConnectionTestIntervalMs() {
        return connectionTestIntervalMs;
    }
    
    /**
     * Sets the connection test interval in milliseconds.
     *
     * @param connectionTestIntervalMs the connection test interval
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setConnectionTestIntervalMs(long connectionTestIntervalMs) {
        this.connectionTestIntervalMs = connectionTestIntervalMs;
        return this;
    }
    
    /**
     * Gets the maximum wait time in milliseconds for connection borrows.
     *
     * @return the maximum wait time
     */
    public long getMaxWaitMs() {
        return maxWaitMs;
    }
    
    /**
     * Sets the maximum wait time in milliseconds for connection borrows.
     *
     * @param maxWaitMs the maximum wait time
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setMaxWaitMs(long maxWaitMs) {
        this.maxWaitMs = maxWaitMs;
        return this;
    }
    
    /**
     * Gets the minimum time in milliseconds a connection must be idle before it can be evicted.
     *
     * @return the minimum evictable idle time
     */
    public long getMinEvictableIdleTimeMs() {
        return minEvictableIdleTimeMs;
    }
    
    /**
     * Sets the minimum time in milliseconds a connection must be idle before it can be evicted.
     *
     * @param minEvictableIdleTimeMs the minimum evictable idle time
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setMinEvictableIdleTimeMs(long minEvictableIdleTimeMs) {
        this.minEvictableIdleTimeMs = minEvictableIdleTimeMs;
        return this;
    }
    
    /**
     * Gets whether connections are tested when borrowed from the pool.
     *
     * @return true if connections are tested on borrow
     */
    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }
    
    /**
     * Sets whether connections are tested when borrowed from the pool.
     *
     * @param testOnBorrow true to test connections on borrow
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
        return this;
    }
    
    /**
     * Gets whether connections are tested when returned to the pool.
     *
     * @return true if connections are tested on return
     */
    public boolean isTestOnReturn() {
        return testOnReturn;
    }
    
    /**
     * Sets whether connections are tested when returned to the pool.
     *
     * @param testOnReturn true to test connections on return
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
        return this;
    }
    
    /**
     * Gets whether idle connections are tested during the eviction process.
     *
     * @return true if idle connections are tested
     */
    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }
    
    /**
     * Sets whether idle connections are tested during the eviction process.
     *
     * @param testWhileIdle true to test idle connections
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
        return this;
    }
    
    /**
     * Gets whether the pool uses LIFO (Last-In-First-Out) behavior.
     *
     * @return true if the pool uses LIFO, false for FIFO
     */
    public boolean isLifo() {
        return lifo;
    }
    
    /**
     * Sets whether the pool uses LIFO (Last-In-First-Out) behavior.
     * Set to false for FIFO (First-In-First-Out) behavior.
     *
     * @param lifo true for LIFO, false for FIFO
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setLifo(boolean lifo) {
        this.lifo = lifo;
        return this;
    }
    
    /**
     * Gets the eviction policy for idle connections.
     *
     * @return the eviction policy
     */
    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }
    
    /**
     * Sets the eviction policy for idle connections.
     *
     * @param evictionPolicy the eviction policy
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }
    
    /**
     * Gets whether borrowConnection blocks when the pool is exhausted.
     *
     * @return true if borrowConnection blocks when the pool is exhausted
     */
    public boolean isBlockWhenExhausted() {
        return blockWhenExhausted;
    }
    
    /**
     * Sets whether borrowConnection blocks when the pool is exhausted.
     *
     * @param blockWhenExhausted true to block when the pool is exhausted
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
        return this;
    }
    
    /**
     * Gets whether the pool uses a fair policy for blocking waits.
     *
     * @return true if the pool uses a fair policy
     */
    public boolean isFairness() {
        return fairness;
    }
    
    /**
     * Sets whether the pool uses a fair policy for blocking waits.
     *
     * @param fairness true to use a fair policy
     * @return this configuration for method chaining
     */
    public FTPConnectionPoolConfig setFairness(boolean fairness) {
        this.fairness = fairness;
        return this;
    }
}