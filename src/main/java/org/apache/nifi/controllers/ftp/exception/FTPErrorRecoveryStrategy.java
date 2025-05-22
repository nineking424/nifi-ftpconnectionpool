package org.apache.nifi.controllers.ftp.exception;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.controllers.ftp.FTPConnection;
import org.apache.nifi.controllers.ftp.FTPConnectionManager;
import org.apache.nifi.controllers.ftp.FTPConnectionState;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Provides advanced recovery strategies for different types of FTP errors.
 * This class contains specialized handling for different error scenarios to maximize
 * recovery success rates while minimizing unnecessary retries for non-recoverable errors.
 */
public class FTPErrorRecoveryStrategy {
    
    private final ComponentLog logger;
    private final FTPConnectionManager connectionManager;
    private final Map<FTPErrorType, RecoveryAction> recoveryStrategies = new HashMap<>();
    private final Map<FTPErrorType, AtomicLong> errorCounts = new HashMap<>();
    private final Map<FTPErrorType, AtomicLong> successfulRecoveries = new HashMap<>();
    
    /**
     * Creates a new error recovery strategy.
     *
     * @param logger The logger to use
     * @param connectionManager The connection manager for reconnecting and validating connections
     */
    public FTPErrorRecoveryStrategy(ComponentLog logger, FTPConnectionManager connectionManager) {
        this.logger = logger;
        this.connectionManager = connectionManager;
        
        // Initialize error counters for all error types
        for (FTPErrorType errorType : FTPErrorType.values()) {
            errorCounts.put(errorType, new AtomicLong(0));
            successfulRecoveries.put(errorType, new AtomicLong(0));
        }
        
        // Set up default recovery strategies for different error types
        initializeRecoveryStrategies();
    }
    
    /**
     * Initializes the default recovery strategies for different error types.
     */
    private void initializeRecoveryStrategies() {
        // Connection-related error strategies
        recoveryStrategies.put(FTPErrorType.CONNECTION_ERROR, 
                (connection, client, exception) -> reconnectWithBackoff(connection));
        
        recoveryStrategies.put(FTPErrorType.CONNECTION_TIMEOUT, 
                (connection, client, exception) -> reconnectWithBackoff(connection));
        
        recoveryStrategies.put(FTPErrorType.CONNECTION_CLOSED, 
                (connection, client, exception) -> reconnectWithBackoff(connection));
        
        recoveryStrategies.put(FTPErrorType.CONNECTION_REFUSED, 
                (connection, client, exception) -> reconnectWithBackoff(connection));
        
        // Authentication-related error strategies - these are typically not recoverable
        recoveryStrategies.put(FTPErrorType.AUTHENTICATION_ERROR, 
                (connection, client, exception) -> false);
        
        recoveryStrategies.put(FTPErrorType.INVALID_CREDENTIALS, 
                (connection, client, exception) -> false);
        
        recoveryStrategies.put(FTPErrorType.INSUFFICIENT_PERMISSIONS, 
                (connection, client, exception) -> false);
        
        // File operation error strategies
        recoveryStrategies.put(FTPErrorType.FILE_NOT_FOUND, 
                (connection, client, exception) -> false); // Non-recoverable
        
        recoveryStrategies.put(FTPErrorType.FILE_ALREADY_EXISTS, 
                (connection, client, exception) -> false); // Non-recoverable
        
        recoveryStrategies.put(FTPErrorType.DIRECTORY_NOT_FOUND, 
                (connection, client, exception) -> false); // Non-recoverable
        
        recoveryStrategies.put(FTPErrorType.DIRECTORY_NOT_EMPTY, 
                (connection, client, exception) -> false); // Non-recoverable
        
        recoveryStrategies.put(FTPErrorType.INVALID_PATH, 
                (connection, client, exception) -> false); // Non-recoverable
        
        // Transfer-related error strategies
        recoveryStrategies.put(FTPErrorType.TRANSFER_ERROR, 
                (connection, client, exception) -> {
                    // Try to abort any ongoing transfer first
                    try {
                        if (client != null && client.isConnected()) {
                            client.abort();
                        }
                    } catch (IOException e) {
                        logger.warn("Failed to abort transfer after error: {}", new Object[] { e.getMessage() });
                    }
                    
                    // Then try to reconnect
                    return reconnectWithBackoff(connection);
                });
        
        recoveryStrategies.put(FTPErrorType.TRANSFER_ABORTED, 
                (connection, client, exception) -> validateAndFixConnection(connection, client));
        
        recoveryStrategies.put(FTPErrorType.TRANSFER_TIMEOUT, 
                (connection, client, exception) -> reconnectWithBackoff(connection));
        
        recoveryStrategies.put(FTPErrorType.INSUFFICIENT_STORAGE, 
                (connection, client, exception) -> false); // Non-recoverable
        
        // Data channel error strategies
        recoveryStrategies.put(FTPErrorType.DATA_CONNECTION_ERROR, 
                (connection, client, exception) -> validateAndFixConnection(connection, client));
        
        recoveryStrategies.put(FTPErrorType.DATA_CONNECTION_TIMEOUT, 
                (connection, client, exception) -> validateAndFixConnection(connection, client));
        
        // Server error strategies
        recoveryStrategies.put(FTPErrorType.SERVER_ERROR, 
                (connection, client, exception) -> false); // Generally not recoverable
        
        recoveryStrategies.put(FTPErrorType.COMMAND_NOT_SUPPORTED, 
                (connection, client, exception) -> false); // Not recoverable
        
        recoveryStrategies.put(FTPErrorType.INVALID_SEQUENCE, 
                (connection, client, exception) -> reconnectWithBackoff(connection));
        
        // Client error strategies
        recoveryStrategies.put(FTPErrorType.CLIENT_ERROR, 
                (connection, client, exception) -> false); // Generally not recoverable
        
        recoveryStrategies.put(FTPErrorType.INVALID_CONFIGURATION, 
                (connection, client, exception) -> false); // Not recoverable
        
        // Pool-related error strategies
        recoveryStrategies.put(FTPErrorType.POOL_EXHAUSTED, 
                (connection, client, exception) -> false); // Not recoverable
        
        recoveryStrategies.put(FTPErrorType.POOL_ERROR, 
                (connection, client, exception) -> false); // Not recoverable
        
        // General error strategies
        recoveryStrategies.put(FTPErrorType.UNEXPECTED_ERROR, 
                (connection, client, exception) -> false); // Conservative approach
        
        recoveryStrategies.put(FTPErrorType.VALIDATION_ERROR, 
                (connection, client, exception) -> false); // Not recoverable
    }
    
    /**
     * Attempts to recover from an FTP error.
     *
     * @param exception The exception that occurred
     * @param connection The FTP connection in use, if available
     * @param client The FTP client in use, if available
     * @return true if recovery was successful, false otherwise
     */
    public boolean attemptRecovery(FTPOperationException exception, FTPConnection connection, FTPClient client) {
        if (exception == null) {
            return false;
        }
        
        FTPErrorType errorType = exception.getErrorType();
        errorCounts.get(errorType).incrementAndGet();
        
        logger.debug("Attempting recovery for {} error: {}", 
                new Object[] { errorType, exception.getMessage() });
        
        // If the error is not recoverable, don't waste time trying
        if (!errorType.isRecoverable()) {
            logger.debug("Error type {} is not recoverable, skipping recovery attempt", errorType);
            return false;
        }
        
        // Look up the appropriate recovery strategy
        RecoveryAction recoveryAction = recoveryStrategies.get(errorType);
        if (recoveryAction == null) {
            logger.warn("No recovery strategy defined for error type: {}", errorType);
            return false;
        }
        
        try {
            // Execute the recovery strategy
            boolean recovered = recoveryAction.recover(connection, client, exception);
            
            // Track successful recoveries
            if (recovered) {
                successfulRecoveries.get(errorType).incrementAndGet();
                logger.info("Successfully recovered from {} error", errorType);
            } else {
                logger.debug("Recovery attempt failed for {} error", errorType);
            }
            
            return recovered;
        } catch (Exception e) {
            logger.error("Error during recovery attempt for {}: {}", 
                    new Object[] { errorType, e.getMessage() }, e);
            return false;
        }
    }
    
    /**
     * Attempts to reconnect a connection with exponential backoff.
     *
     * @param connection The connection to reconnect
     * @return true if reconnection was successful, false otherwise
     */
    private boolean reconnectWithBackoff(FTPConnection connection) {
        if (connection == null) {
            return false;
        }
        
        try {
            // Mark connection as failed to ensure it will be reconnected
            if (connection.getState() != FTPConnectionState.FAILED) {
                connection.setState(FTPConnectionState.FAILED);
            }
            
            // Attempt to reconnect using the connection manager
            return connectionManager.reconnect(connection);
        } catch (Exception e) {
            logger.error("Error during reconnection attempt: {}", 
                    new Object[] { e.getMessage() }, e);
            return false;
        }
    }
    
    /**
     * Validates a connection and attempts to fix it if it's not valid.
     *
     * @param connection The connection to validate
     * @param client The client to validate
     * @return true if the connection is valid or was fixed, false otherwise
     */
    private boolean validateAndFixConnection(FTPConnection connection, FTPClient client) {
        if (connection != null) {
            try {
                // Try to validate the connection
                boolean valid = connectionManager.validateConnection(connection);
                if (valid) {
                    return true;
                }
                
                // If not valid, try to reconnect
                return connectionManager.reconnect(connection);
            } catch (Exception e) {
                logger.error("Error validating connection: {}", new Object[] { e.getMessage() }, e);
                return false;
            }
        } else if (client != null) {
            try {
                // For raw clients without a connection object
                return connectionManager.validateConnection(client);
            } catch (Exception e) {
                logger.error("Error validating client: {}", new Object[] { e.getMessage() }, e);
                return false;
            }
        }
        
        return false;
    }
    
    /**
     * Gets error statistics for monitoring.
     *
     * @return A map of statistics about errors and recovery attempts
     */
    public Map<String, Object> getErrorStatistics() {
        Map<String, Object> stats = new HashMap<>();
        
        // Add total counts
        long totalErrors = errorCounts.values().stream()
                .mapToLong(AtomicLong::get)
                .sum();
        
        long totalRecoveries = successfulRecoveries.values().stream()
                .mapToLong(AtomicLong::get)
                .sum();
        
        stats.put("totalErrors", totalErrors);
        stats.put("totalSuccessfulRecoveries", totalRecoveries);
        
        // Calculate recovery success rate if errors have occurred
        double recoveryRate = totalErrors > 0 
                ? (double) totalRecoveries / totalErrors * 100.0 
                : 0.0;
        stats.put("recoverySuccessRate", recoveryRate);
        
        // Add counts for each error type
        Map<String, Object> errorTypeStats = new HashMap<>();
        for (FTPErrorType errorType : FTPErrorType.values()) {
            long errors = errorCounts.get(errorType).get();
            long recoveries = successfulRecoveries.get(errorType).get();
            
            Map<String, Object> typeStats = new HashMap<>();
            typeStats.put("errors", errors);
            typeStats.put("recoveries", recoveries);
            typeStats.put("recoveryRate", errors > 0 ? (double) recoveries / errors * 100.0 : 0.0);
            
            errorTypeStats.put(errorType.name(), typeStats);
        }
        
        stats.put("byErrorType", errorTypeStats);
        
        return stats;
    }
    
    /**
     * Resets all error statistics.
     */
    public void resetStatistics() {
        for (AtomicLong count : errorCounts.values()) {
            count.set(0);
        }
        
        for (AtomicLong count : successfulRecoveries.values()) {
            count.set(0);
        }
    }
    
    /**
     * Functional interface for recovery actions.
     */
    @FunctionalInterface
    private interface RecoveryAction {
        /**
         * Attempts to recover from an error.
         *
         * @param connection The connection that experienced the error, if available
         * @param client The client that experienced the error, if available
         * @param exception The exception that occurred
         * @return true if recovery was successful, false otherwise
         */
        boolean recover(FTPConnection connection, FTPClient client, FTPOperationException exception);
    }
    
    /**
     * Sets a custom recovery strategy for a specific error type.
     *
     * @param errorType The error type to set a strategy for
     * @param recoveryAction The recovery action to use
     */
    public void setRecoveryStrategy(FTPErrorType errorType, RecoveryAction recoveryAction) {
        recoveryStrategies.put(errorType, recoveryAction);
    }
    
    /**
     * Creates a custom recovery strategy from a lambda.
     *
     * @param handler The recovery handler function
     * @return A RecoveryAction
     */
    public static RecoveryAction createStrategy(RecoveryAction handler) {
        return handler;
    }
}