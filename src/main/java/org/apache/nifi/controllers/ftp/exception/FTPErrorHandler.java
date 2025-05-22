package org.apache.nifi.controllers.ftp.exception;

import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

/**
 * Utility class for handling FTP errors and providing retry mechanisms.
 */
public class FTPErrorHandler {
    
    private final ComponentLog logger;
    private final int defaultMaxRetries;
    private final long defaultRetryDelay;
    private final boolean logAllErrors;
    
    /**
     * Creates a new FTPErrorHandler with default settings.
     *
     * @param logger The logger to use for error messages
     */
    public FTPErrorHandler(ComponentLog logger) {
        this(logger, 3, 1000, true);
    }
    
    /**
     * Creates a new FTPErrorHandler with custom settings.
     *
     * @param logger The logger to use for error messages
     * @param defaultMaxRetries The default maximum retries for recoverable errors
     * @param defaultRetryDelay The default delay between retries in milliseconds
     * @param logAllErrors Whether to log all errors, or just the final failure
     */
    public FTPErrorHandler(ComponentLog logger, int defaultMaxRetries, long defaultRetryDelay, boolean logAllErrors) {
        this.logger = logger;
        this.defaultMaxRetries = defaultMaxRetries;
        this.defaultRetryDelay = defaultRetryDelay;
        this.logAllErrors = logAllErrors;
    }
    
    /**
     * Executes an FTP operation with retry capability.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationDescription A description of the operation for logging
     * @return The result of the operation
     * @throws IOException if the operation fails after retries
     */
    public <T> T executeWithRetry(Callable<T> operation, String operationDescription) throws IOException {
        return executeWithRetry(operation, operationDescription, defaultMaxRetries, defaultRetryDelay);
    }
    
    /**
     * Executes an FTP operation with retry capability and custom retry settings.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationDescription A description of the operation for logging
     * @param maxRetries The maximum number of retries for recoverable errors
     * @param retryDelay The delay between retries in milliseconds
     * @return The result of the operation
     * @throws IOException if the operation fails after retries
     */
    public <T> T executeWithRetry(Callable<T> operation, String operationDescription, int maxRetries, long retryDelay) 
            throws IOException {
        
        IOException lastException = null;
        int currentAttempt = 0;
        
        while (currentAttempt <= maxRetries) {
            try {
                if (currentAttempt > 0) {
                    logger.debug("Retry attempt {} of {} for operation: {}", 
                            new Object[] { currentAttempt, maxRetries, operationDescription });
                }
                
                return operation.call();
                
            } catch (Exception e) {
                // Convert to IOException if it's not already
                IOException ioException;
                if (e instanceof IOException) {
                    ioException = (IOException) e;
                } else {
                    ioException = new IOException("Error during FTP operation: " + e.getMessage(), e);
                }
                
                // Check if this is a recoverable FTP exception
                if (ioException instanceof FTPOperationException) {
                    FTPOperationException ftpEx = (FTPOperationException) ioException;
                    
                    if (ftpEx.isRecoverable() && currentAttempt < maxRetries) {
                        // Log the error if configured to do so
                        if (logAllErrors) {
                            logger.warn("Recoverable error during FTP operation (attempt {} of {}): {}: {}. Will retry in {} ms.", 
                                    new Object[] { currentAttempt + 1, maxRetries + 1, operationDescription, 
                                    ftpEx.getMessage(), retryDelay });
                        }
                        
                        // Increment the retry count in the exception
                        ftpEx.incrementRetryCount();
                        
                        // Wait before retrying
                        try {
                            Thread.sleep(retryDelay);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw ftpEx;
                        }
                        
                        currentAttempt++;
                        lastException = ftpEx;
                        continue;
                    }
                    
                    // If it's not recoverable or we've used all retries, throw the exception
                    if (!ftpEx.isRecoverable()) {
                        logger.error("Non-recoverable error during FTP operation {}: {}", 
                                new Object[] { operationDescription, ftpEx.getMessage() });
                    } else {
                        logger.error("Recoverable error during FTP operation {} failed after {} retries: {}", 
                                new Object[] { operationDescription, maxRetries, ftpEx.getMessage() });
                    }
                    
                    throw ftpEx;
                }
                
                // For non-FTP exceptions, log and retry if we have attempts left
                if (currentAttempt < maxRetries) {
                    if (logAllErrors) {
                        logger.warn("Error during FTP operation (attempt {} of {}): {}: {}. Will retry in {} ms.", 
                                new Object[] { currentAttempt + 1, maxRetries + 1, operationDescription, 
                                ioException.getMessage(), retryDelay });
                    }
                    
                    // Wait before retrying
                    try {
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ioException;
                    }
                    
                    currentAttempt++;
                    lastException = ioException;
                    continue;
                }
                
                // We've used all retries or it's not a FTP exception
                logger.error("Error during FTP operation {} failed after {} retries: {}", 
                        new Object[] { operationDescription, maxRetries, ioException.getMessage() });
                
                throw ioException;
            }
        }
        
        // This should never be reached, but just in case
        throw lastException;
    }
    
    /**
     * Executes an FTP operation that doesn't return a value with retry capability.
     *
     * @param operation The operation to execute
     * @param operationDescription A description of the operation for logging
     * @throws IOException if the operation fails after retries
     */
    public void executeWithRetry(FTPOperation operation, String operationDescription) throws IOException {
        executeWithRetry(operation, operationDescription, defaultMaxRetries, defaultRetryDelay);
    }
    
    /**
     * Executes an FTP operation that doesn't return a value with retry capability and custom retry settings.
     *
     * @param operation The operation to execute
     * @param operationDescription A description of the operation for logging
     * @param maxRetries The maximum number of retries for recoverable errors
     * @param retryDelay The delay between retries in milliseconds
     * @throws IOException if the operation fails after retries
     */
    public void executeWithRetry(FTPOperation operation, String operationDescription, int maxRetries, long retryDelay) 
            throws IOException {
        
        executeWithRetry(() -> {
            operation.execute();
            return null;
        }, operationDescription, maxRetries, retryDelay);
    }
    
    /**
     * Creates an FTPOperationException with the specified error type and message.
     *
     * @param errorType The error type
     * @param message The error message
     * @return The exception
     */
    public FTPOperationException createException(FTPErrorType errorType, String message) {
        return new FTPOperationException(errorType, message);
    }
    
    /**
     * Creates an FTPOperationException with the specified error type, message, and cause.
     *
     * @param errorType The error type
     * @param message The error message
     * @param cause The cause of the exception
     * @return The exception
     */
    public FTPOperationException createException(FTPErrorType errorType, String message, Throwable cause) {
        return new FTPOperationException(errorType, message, cause);
    }
    
    /**
     * Creates an FTPOperationException with the specified error type, path, and message.
     *
     * @param errorType The error type
     * @param path The file or directory path
     * @param message The error message
     * @return The exception
     */
    public FTPOperationException createException(FTPErrorType errorType, String path, String message) {
        return new FTPOperationException(errorType, path, message);
    }
    
    /**
     * Creates an FTPTransferException for a file transfer error.
     *
     * @param errorType The error type
     * @param remotePath The remote file path
     * @param bytesTransferred The number of bytes transferred before the error
     * @param message The error message
     * @return The exception
     */
    public FTPTransferException createTransferException(FTPErrorType errorType, String remotePath, 
            long bytesTransferred, String message) {
        return new FTPTransferException(errorType, remotePath, bytesTransferred, message);
    }
    
    /**
     * Creates an FTPFileOperationException for a file operation error.
     *
     * @param errorType The error type
     * @param remotePath The remote file path
     * @param operation The file operation
     * @param message The error message
     * @return The exception
     */
    public FTPFileOperationException createFileOperationException(FTPErrorType errorType, String remotePath, 
            FTPFileOperationException.FileOperation operation, String message) {
        return new FTPFileOperationException(errorType, remotePath, operation, message);
    }
    
    /**
     * Creates an FTPConnectionException for a connection error.
     *
     * @param errorType The error type
     * @param message The error message
     * @return The exception
     */
    public FTPConnectionException createConnectionException(FTPErrorType errorType, String message) {
        return new FTPConnectionException(errorType, message);
    }
    
    /**
     * Converts an arbitrary exception to an FTPOperationException if it isn't one already.
     *
     * @param e The exception to convert
     * @param defaultErrorType The default error type to use if the exception is not an FTPOperationException
     * @param defaultMessage The default message to use if the exception is not an FTPOperationException
     * @return The converted exception
     */
    public FTPOperationException convertException(Exception e, FTPErrorType defaultErrorType, String defaultMessage) {
        if (e instanceof FTPOperationException) {
            return (FTPOperationException) e;
        }
        
        return new FTPOperationException(defaultErrorType, defaultMessage + ": " + e.getMessage(), e);
    }
    
    /**
     * Handles a potential recovery situation by checking an FTP server reply code and creating an appropriate exception.
     *
     * @param replyCode The FTP reply code
     * @param errorType The error type to use if the code indicates an error
     * @param path The file or directory path
     * @param replyMessage The server reply message
     * @throws FTPOperationException if the reply code indicates an error
     */
    public void handleReplyCode(int replyCode, FTPErrorType errorType, String path, String replyMessage) 
            throws FTPOperationException {
        
        // Check if the reply code indicates success (2xx)
        if (replyCode >= 200 && replyCode < 300) {
            return;
        }
        
        // Handle positive intermediate reply (3xx) - usually not an error
        if (replyCode >= 300 && replyCode < 400) {
            logger.debug("Intermediate reply from server: Code {}, Message: '{}'", 
                    new Object[] { replyCode, replyMessage });
            return;
        }
        
        // Handle temporary error (4xx)
        if (replyCode >= 400 && replyCode < 500) {
            // Create a recoverable exception
            FTPOperationException ex = new FTPOperationException(errorType, path, replyCode, 
                    "Temporary error: " + replyMessage);
            logger.warn("Temporary error from server: Code {}, Message: '{}'", 
                    new Object[] { replyCode, replyMessage });
            throw ex;
        }
        
        // Handle permanent error (5xx)
        if (replyCode >= 500 && replyCode < 600) {
            // Create a non-recoverable exception
            FTPOperationException ex = new FTPOperationException(
                    mapPermanentErrorToType(replyCode, errorType), 
                    path, replyCode, "Permanent error: " + replyMessage);
            logger.error("Permanent error from server: Code {}, Message: '{}'", 
                    new Object[] { replyCode, replyMessage });
            throw ex;
        }
        
        // Unknown reply code
        logger.warn("Unknown reply code from server: Code {}, Message: '{}'", 
                new Object[] { replyCode, replyMessage });
        throw new FTPOperationException(errorType, path, replyCode, "Unknown reply code: " + replyMessage);
    }
    
    /**
     * Maps a permanent error reply code to a more specific error type.
     *
     * @param replyCode The FTP reply code
     * @param defaultErrorType The default error type if no specific mapping is found
     * @return The mapped error type
     */
    private FTPErrorType mapPermanentErrorToType(int replyCode, FTPErrorType defaultErrorType) {
        switch (replyCode) {
            case 501: // Syntax error in parameters
            case 502: // Command not implemented
                return FTPErrorType.COMMAND_NOT_SUPPORTED;
            case 503: // Bad sequence of commands
                return FTPErrorType.INVALID_SEQUENCE;
            case 504: // Command not implemented for that parameter
                return FTPErrorType.COMMAND_NOT_SUPPORTED;
            case 530: // Not logged in
            case 532: // Need account for storing files
                return FTPErrorType.AUTHENTICATION_ERROR;
            case 550: // File not found / No access
                return FTPErrorType.FILE_NOT_FOUND;
            case 551: // Requested action aborted: page type unknown
            case 552: // Requested file action aborted. Exceeded storage allocation
                return FTPErrorType.INSUFFICIENT_STORAGE;
            case 553: // Requested action not taken. File name not allowed
                return FTPErrorType.INVALID_PATH;
            default:
                return defaultErrorType;
        }
    }
    
    /**
     * Interface for FTP operations that don't return a value.
     */
    @FunctionalInterface
    public interface FTPOperation {
        /**
         * Executes the operation.
         *
         * @throws IOException if an error occurs during execution
         */
        void execute() throws IOException;
    }
}