package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for an FTP connection pool that manages FTP connections.
 */
public interface FTPConnectionPool {
    
    /**
     * Borrows a connection from the pool. The connection must be returned to the pool
     * using either returnConnection or invalidateConnection.
     *
     * @return a configured and connected FTPClient
     * @throws IOException if unable to get a connection from the pool
     * @throws FTPConnectionException if there's a problem with the connection
     */
    FTPClient borrowConnection() throws IOException, FTPConnectionException;
    
    /**
     * Returns a connection to the pool so it can be reused.
     *
     * @param client the FTPClient to return to the pool
     */
    void returnConnection(FTPClient client);
    
    /**
     * Invalidates a connection and removes it from the pool.
     * This should be used when the connection is known to be bad.
     *
     * @param client the FTPClient to invalidate
     */
    void invalidateConnection(FTPClient client);
    
    /**
     * Gets the number of active connections in the pool.
     *
     * @return the number of active connections
     */
    int getActiveConnectionCount();
    
    /**
     * Gets the number of idle connections in the pool.
     *
     * @return the number of idle connections
     */
    int getIdleConnectionCount();
    
    /**
     * Gets the maximum number of connections allowed in the pool.
     *
     * @return the maximum number of connections
     */
    int getMaxConnections();
    
    /**
     * Gets the minimum number of idle connections to maintain in the pool.
     *
     * @return the minimum number of idle connections
     */
    int getMinConnections();
    
    /**
     * Gets detailed metrics about the connection pool.
     *
     * @return a map of metrics about the connection pool
     */
    Map<String, Object> getPoolMetrics();
    
    /**
     * Closes all connections in the pool and releases resources.
     */
    void shutdown();
    
    /**
     * Clears the pool, closing all idle connections.
     * Active connections will remain open but will not be returned to the pool.
     */
    void clear();
    
    /**
     * Refreshes all idle connections in the pool.
     */
    void refreshIdleConnections();
}