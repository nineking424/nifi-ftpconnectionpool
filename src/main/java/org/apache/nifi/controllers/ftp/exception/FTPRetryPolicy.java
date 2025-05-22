package org.apache.nifi.controllers.ftp.exception;

import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a retry policy with exponential backoff for FTP operations.
 * This allows operations to be automatically retried with increasing wait times
 * between attempts when recoverable errors occur.
 */
public class FTPRetryPolicy {
    private final ComponentLog logger;
    private final String operationName;
    private final int maxRetries;
    private final long initialBackoffMs;
    private final double backoffMultiplier;
    private final long maxBackoffMs;
    private final boolean logRetryAttempts;
    private final AtomicInteger attemptCount = new AtomicInteger(0);
    
    /**
     * Creates a new retry policy with custom settings.
     *
     * @param logger The logger to use
     * @param operationName A name for the operation (for logging)
     * @param maxRetries The maximum number of retry attempts
     * @param initialBackoffMs The initial backoff delay in milliseconds
     * @param backoffMultiplier The factor to multiply the backoff by for each retry
     * @param maxBackoffMs The maximum backoff delay in milliseconds
     * @param logRetryAttempts Whether to log each retry attempt
     */
    public FTPRetryPolicy(ComponentLog logger, String operationName, int maxRetries, 
            long initialBackoffMs, double backoffMultiplier, long maxBackoffMs, 
            boolean logRetryAttempts) {
        this.logger = logger;
        this.operationName = operationName;
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoffMs = maxBackoffMs;
        this.logRetryAttempts = logRetryAttempts;
    }
    
    /**
     * Creates a new retry policy with default settings.
     *
     * @param logger The logger to use
     * @param operationName A name for the operation
     */
    public FTPRetryPolicy(ComponentLog logger, String operationName) {
        this(logger, operationName, 3, 1000, 2.0, 30000, true);
    }
    
    /**
     * Creates a new retry policy with custom retry count and default other settings.
     *
     * @param logger The logger to use
     * @param operationName A name for the operation
     * @param maxRetries The maximum number of retry attempts
     */
    public FTPRetryPolicy(ComponentLog logger, String operationName, int maxRetries) {
        this(logger, operationName, maxRetries, 1000, 2.0, 30000, true);
    }
    
    /**
     * Executes an operation with retry handling.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @return The result of the operation
     * @throws IOException if the operation fails after all retry attempts
     */
    public <T> T execute(Callable<T> operation) throws IOException {
        attemptCount.set(0);
        IOException lastException = null;
        int currentAttempt = 0;
        long currentBackoffMs = initialBackoffMs;
        
        while (currentAttempt <= maxRetries) {
            try {
                if (currentAttempt > 0 && logRetryAttempts) {
                    logger.debug("Retry attempt {} of {} for operation: {}", 
                            new Object[] { currentAttempt, maxRetries, operationName });
                }
                
                T result = operation.call();
                
                // On success, if this was a retry, log it
                if (currentAttempt > 0 && logRetryAttempts) {
                    logger.info("Operation '{}' succeeded after {} retries", 
                            new Object[] { operationName, currentAttempt });
                }
                
                return result;
                
            } catch (Exception e) {
                // Convert exception to IOException if it's not already
                IOException ioException;
                if (e instanceof IOException) {
                    ioException = (IOException) e;
                } else {
                    ioException = new IOException("Error during FTP operation: " + e.getMessage(), e);
                }
                
                lastException = ioException;
                currentAttempt++;
                attemptCount.set(currentAttempt);
                
                // Check if we should retry
                boolean shouldRetry = shouldRetry(ioException, currentAttempt);
                
                if (shouldRetry && currentAttempt <= maxRetries) {
                    // Log retry info at appropriate level
                    if (logRetryAttempts) {
                        logger.warn("Recoverable error during FTP operation '{}' (attempt {} of {}): {}. Will retry in {} ms.", 
                                new Object[] { operationName, currentAttempt, maxRetries + 1, 
                                ioException.getMessage(), currentBackoffMs });
                    }
                    
                    // Wait before retrying
                    try {
                        Thread.sleep(currentBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Retry interrupted", ie);
                    }
                    
                    // Calculate next backoff time with exponential increase
                    currentBackoffMs = Math.min((long)(currentBackoffMs * backoffMultiplier), maxBackoffMs);
                    
                } else {
                    // We've exhausted retries or encountered a non-recoverable error
                    if (currentAttempt > 1) {
                        logger.error("Operation '{}' failed after {} retries: {}", 
                                new Object[] { operationName, currentAttempt - 1, ioException.getMessage() });
                    }
                    
                    throw lastException;
                }
            }
        }
        
        // This should only be reached if maxRetries == 0 and the operation fails
        throw lastException;
    }
    
    /**
     * Determines whether an exception should trigger a retry.
     *
     * @param exception The exception to check
     * @param currentAttempt The current attempt number
     * @return true if the operation should be retried, false otherwise
     */
    protected boolean shouldRetry(IOException exception, int currentAttempt) {
        // Check if it's a known FTP exception type
        if (exception instanceof FTPOperationException) {
            FTPOperationException ftpException = (FTPOperationException) exception;
            return ftpException.isRecoverable();
        }
        
        // For other IOException types, examine the message for common recoverable patterns
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        
        // Common network/connection issues that are typically transient
        return message.contains("timed out") || 
               message.contains("connection reset") || 
               message.contains("broken pipe") || 
               message.contains("connection closed") ||
               message.contains("connection refused") || 
               message.contains("network is unreachable") ||
               message.contains("no route to host") ||
               message.contains("unexpected end of file") ||
               message.contains("socket closed");
    }
    
    /**
     * Gets the current attempt count.
     *
     * @return The number of attempts made so far
     */
    public int getAttemptCount() {
        return attemptCount.get();
    }
    
    /**
     * Gets the maximum number of retries.
     *
     * @return The maximum retry count
     */
    public int getMaxRetries() {
        return maxRetries;
    }
    
    /**
     * Calculates the backoff time for a specific retry attempt.
     *
     * @param attemptNumber The attempt number (1-based)
     * @return The backoff time in milliseconds
     */
    public long getBackoffTimeMs(int attemptNumber) {
        if (attemptNumber <= 1) {
            return 0; // No backoff for first attempt
        }
        
        long backoff = initialBackoffMs;
        for (int i = 2; i <= attemptNumber; i++) {
            backoff = Math.min((long)(backoff * backoffMultiplier), maxBackoffMs);
        }
        
        return backoff;
    }
    
    /**
     * Functional interface for operations that don't return a value.
     */
    @FunctionalInterface
    public interface VoidOperation {
        /**
         * Executes the operation.
         *
         * @throws IOException if an error occurs
         */
        void execute() throws IOException;
    }
    
    /**
     * Executes an operation that doesn't return a value with retry handling.
     *
     * @param operation The operation to execute
     * @throws IOException if the operation fails after all retry attempts
     */
    public void executeVoid(VoidOperation operation) throws IOException {
        execute(() -> {
            operation.execute();
            return null;
        });
    }
}