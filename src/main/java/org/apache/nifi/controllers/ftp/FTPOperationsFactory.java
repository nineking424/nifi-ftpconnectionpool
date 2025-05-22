package org.apache.nifi.controllers.ftp;

import org.apache.nifi.logging.ComponentLog;

/**
 * Factory class for creating FTPOperations instances with various error handling capabilities.
 */
public class FTPOperationsFactory {
    
    /**
     * Creates a basic FTPOperations instance.
     *
     * @param connectionPool The connection pool to use
     * @param logger The logger to use
     * @return A new FTPOperations instance
     */
    public static FTPOperations createBasicFTPOperations(FTPConnectionPool connectionPool, ComponentLog logger) {
        return new FTPOperations(connectionPool, logger);
    }
    
    /**
     * Creates an FTPOperations instance with enhanced error handling.
     *
     * @param connectionPool The connection pool to use
     * @param logger The logger to use
     * @return A new FTPOperationsWithErrorHandling instance
     */
    public static FTPOperationsWithErrorHandling createFTPOperationsWithErrorHandling(
            FTPConnectionPool connectionPool, ComponentLog logger) {
        
        FTPOperations basicOperations = createBasicFTPOperations(connectionPool, logger);
        return new FTPOperationsWithErrorHandling(basicOperations, logger);
    }
    
    /**
     * Creates an FTPOperations instance with enhanced error handling and custom retry settings.
     *
     * @param connectionPool The connection pool to use
     * @param logger The logger to use
     * @param maxRetries The maximum number of retries for recoverable errors
     * @param retryDelay The delay between retries in milliseconds
     * @return A new FTPOperationsWithErrorHandling instance
     */
    public static FTPOperationsWithErrorHandling createFTPOperationsWithErrorHandling(
            FTPConnectionPool connectionPool, ComponentLog logger, int maxRetries, long retryDelay) {
        
        FTPOperations basicOperations = createBasicFTPOperations(connectionPool, logger);
        return new FTPOperationsWithErrorHandling(basicOperations, logger, maxRetries, retryDelay);
    }
}