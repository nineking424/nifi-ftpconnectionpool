package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;

import java.util.List;
import java.util.Map;

/**
 * Interface for monitoring and managing the health of FTP connections.
 */
public interface FTPConnectionHealth {
    
    /**
     * Enum representing the health status of a connection.
     */
    enum HealthStatus {
        /**
         * The connection is healthy and operating normally.
         */
        HEALTHY,
        
        /**
         * The connection is operational but experiencing issues.
         */
        DEGRADED,
        
        /**
         * The connection has failed and needs to be repaired.
         */
        FAILED,
        
        /**
         * The connection is currently being tested or repaired.
         */
        REPAIRING
    }
    
    /**
     * Tests if a connection is still valid and operational.
     *
     * @param client the FTP client to test
     * @return true if the connection is valid, false otherwise
     */
    boolean testConnection(FTPClient client);
    
    /**
     * Tests if a connection is still valid and operational.
     *
     * @param connection the FTP connection to test
     * @return true if the connection is valid, false otherwise
     */
    boolean testConnection(FTPConnection connection);
    
    /**
     * Attempts to refresh a connection by reestablishing it.
     *
     * @param client the FTP client to refresh
     * @return true if the connection was successfully refreshed, false otherwise
     */
    boolean refreshConnection(FTPClient client);
    
    /**
     * Attempts to refresh a connection by reestablishing it.
     *
     * @param connection the FTP connection to refresh
     * @return true if the connection was successfully refreshed, false otherwise
     */
    boolean refreshConnection(FTPConnection connection);
    
    /**
     * Sends a keep-alive message to a connection to prevent it from timing out.
     *
     * @param client the FTP client to keep alive
     * @return true if the keep-alive was successful, false otherwise
     */
    boolean keepAlive(FTPClient client);
    
    /**
     * Sends a keep-alive message to a connection to prevent it from timing out.
     *
     * @param connection the FTP connection to keep alive
     * @return true if the keep-alive was successful, false otherwise
     */
    boolean keepAlive(FTPConnection connection);
    
    /**
     * Gets the health status of a connection.
     *
     * @param client the FTP client to check
     * @return the health status of the connection
     */
    HealthStatus getConnectionStatus(FTPClient client);
    
    /**
     * Gets the health status of a connection.
     *
     * @param connection the FTP connection to check
     * @return the health status of the connection
     */
    HealthStatus getConnectionStatus(FTPConnection connection);
    
    /**
     * Gets the last time a connection was successfully tested.
     *
     * @param client the FTP client
     * @return the timestamp of the last successful test in milliseconds, or 0 if never tested
     */
    long getLastSuccessfulTestTime(FTPClient client);
    
    /**
     * Gets the last time a connection was successfully tested.
     *
     * @param connection the FTP connection
     * @return the timestamp of the last successful test in milliseconds, or 0 if never tested
     */
    long getLastSuccessfulTestTime(FTPConnection connection);
    
    /**
     * Gets a list of recent connection errors.
     *
     * @return a list of error messages in chronological order (oldest first)
     */
    List<String> getConnectionErrorHistory();
    
    /**
     * Gets detailed health metrics for all connections.
     *
     * @return a map of health metrics
     */
    Map<String, Object> getHealthMetrics();
    
    /**
     * Analyzes a connection's health and returns detailed diagnostic information.
     *
     * @param connection the FTP connection to analyze
     * @return a map of diagnostic information
     */
    Map<String, Object> analyzeConnection(FTPConnection connection);
    
    /**
     * Performs maintenance on all connections, testing and refreshing as needed.
     *
     * @return the number of connections that were repaired
     */
    int performMaintenanceCycle();
}