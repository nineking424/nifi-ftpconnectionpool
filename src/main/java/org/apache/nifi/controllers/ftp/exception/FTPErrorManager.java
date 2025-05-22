package org.apache.nifi.controllers.ftp.exception;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.controllers.ftp.FTPConnection;
import org.apache.nifi.controllers.ftp.FTPConnectionManager;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorNode;
import org.apache.nifi.reporting.Severity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive error management system for FTP operations.
 * Combines retry mechanisms, circuit breaker, error categorization, and detailed reporting.
 */
public class FTPErrorManager {
    
    private final ComponentLog logger;
    private final ProcessorNode processorNode;
    private final FTPConnectionManager connectionManager;
    
    // Sub-components
    private final FTPBulletinReporter bulletinReporter;
    private final FTPErrorRecoveryStrategy recoveryStrategy;
    private final FTPErrorMetrics errorMetrics;
    private final Map<String, FTPRetryPolicy> retryPolicies = new ConcurrentHashMap<>();
    private final Map<String, FTPCircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
    
    // Default timeout values
    private static final long DEFAULT_BULLETIN_THROTTLE_MS = TimeUnit.SECONDS.toMillis(30);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_INITIAL_BACKOFF_MS = 1000;
    private static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;
    private static final long DEFAULT_MAX_BACKOFF_MS = 30000;
    private static final int DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5;
    private static final long DEFAULT_CIRCUIT_BREAKER_RESET_MS = TimeUnit.MINUTES.toMillis(2);
    
    // Error tracking over time for health check
    private final AtomicLong totalErrorCount = new AtomicLong(0);
    private final AtomicLong successfulOperationCount = new AtomicLong(0);
    private final AtomicLong lastErrorTime = new AtomicLong(0);
    private volatile String lastErrorMessage = null;
    private volatile FTPErrorType lastErrorType = null;
    
    /**
     * Creates a new error manager with all components.
     *
     * @param logger The logger to use
     * @param processorNode The processor node for bulletins (can be null if bulletin reporting is not needed)
     * @param connectionManager The connection manager for reconnection and validation
     */
    public FTPErrorManager(ComponentLog logger, ProcessorNode processorNode, 
            FTPConnectionManager connectionManager) {
        this.logger = logger;
        this.processorNode = processorNode;
        this.connectionManager = connectionManager;
        
        // Initialize sub-components
        this.bulletinReporter = new FTPBulletinReporter(logger, processorNode, DEFAULT_BULLETIN_THROTTLE_MS);
        this.recoveryStrategy = new FTPErrorRecoveryStrategy(logger, connectionManager);
        this.errorMetrics = new FTPErrorMetrics();
        
        // Set up default retry policies for common operations
        this.retryPolicies.put("default", new FTPRetryPolicy(logger, "default", 
                DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_BACKOFF_MS, 
                DEFAULT_BACKOFF_MULTIPLIER, DEFAULT_MAX_BACKOFF_MS, true));
        
        this.retryPolicies.put("connection", new FTPRetryPolicy(logger, "connection", 
                5, 1000, 2.0, 60000, true));
        
        this.retryPolicies.put("listing", new FTPRetryPolicy(logger, "listing", 
                3, 500, 2.0, 15000, true));
        
        this.retryPolicies.put("transfer", new FTPRetryPolicy(logger, "transfer", 
                3, 2000, 2.0, 30000, true));
        
        // Set up default circuit breakers
        this.circuitBreakers.put("server", new FTPCircuitBreaker(logger, "server", 
                DEFAULT_CIRCUIT_BREAKER_THRESHOLD, DEFAULT_CIRCUIT_BREAKER_RESET_MS));
    }
    
    /**
     * Executes an FTP operation with full error handling, retry logic, and circuit breaker protection.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationName A name for the operation (for logging and metrics)
     * @return The result of the operation
     * @throws IOException if the operation fails after all recovery attempts
     */
    public <T> T executeWithErrorHandling(Callable<T> operation, String operationName) throws IOException {
        return executeWithErrorHandling(operation, operationName, null, null);
    }
    
    /**
     * Executes an FTP operation with full error handling, retry logic, and circuit breaker protection.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationName A name for the operation (for logging and metrics)
     * @param connection The FTP connection being used, if available
     * @param client The FTP client being used, if available
     * @return The result of the operation
     * @throws IOException if the operation fails after all recovery attempts
     */
    public <T> T executeWithErrorHandling(Callable<T> operation, String operationName, 
            FTPConnection connection, FTPClient client) throws IOException {
        
        // Get the appropriate retry policy for this operation type
        FTPRetryPolicy retryPolicy = getRetryPolicy(operationName);
        
        // Get the circuit breaker for server protection
        FTPCircuitBreaker circuitBreaker = circuitBreakers.get("server");
        
        // Check circuit breaker state first
        if (circuitBreaker != null && circuitBreaker.getState() == FTPCircuitBreaker.State.OPEN) {
            IOException exception = new FTPOperationException(FTPErrorType.SERVER_ERROR, 
                    "Circuit breaker open - server appears to be unavailable");
            recordError(FTPErrorType.SERVER_ERROR, exception.getMessage());
            throw exception;
        }
        
        IOException lastException = null;
        int currentAttempt = 0;
        
        while (currentAttempt <= retryPolicy.getMaxRetries()) {
            try {
                // Execute the operation
                T result = operation.call();
                
                // Record successful operation
                recordSuccess();
                
                // Reset circuit breaker on success if needed
                if (circuitBreaker != null && circuitBreaker.getState() != FTPCircuitBreaker.State.CLOSED) {
                    circuitBreaker.reset();
                }
                
                return result;
                
            } catch (Exception e) {
                // Convert exception if needed
                IOException ioException;
                if (e instanceof IOException) {
                    ioException = (IOException) e;
                } else {
                    ioException = new IOException("Error during FTP operation: " + e.getMessage(), e);
                }
                
                // If it's an FTPOperationException, handle it specially
                if (ioException instanceof FTPOperationException) {
                    FTPOperationException ftpException = (FTPOperationException) ioException;
                    
                    // Record error for metrics
                    errorMetrics.recordError(ftpException, operationName);
                    
                    // Record the error in our own metrics
                    recordError(ftpException.getErrorType(), ftpException.getMessage());
                    
                    // Report to bulletins if appropriate
                    if (processorNode != null) {
                        bulletinReporter.reportException(ftpException);
                    }
                    
                    // If circuit breaker is configured and this is a server error, record failure
                    if (circuitBreaker != null && isServerError(ftpException.getErrorType())) {
                        circuitBreaker.recordFailure(e);
                    }
                    
                    // Store the last exception
                    lastException = ftpException;
                    
                    // Check if recovery should be attempted
                    if (currentAttempt < retryPolicy.getMaxRetries() && ftpException.isRecoverable()) {
                        // Attempt recovery
                        boolean recovered = recoveryStrategy.attemptRecovery(ftpException, connection, client);
                        
                        if (recovered) {
                            // If recovery succeeded, retry the operation immediately
                            errorMetrics.recordRetryAttempt(operationName);
                            logger.debug("Recovery successful, retrying operation immediately: {}", operationName);
                            currentAttempt++;
                            continue;
                        } else {
                            // If recovery failed but operation is still recoverable, retry with backoff
                            currentAttempt++;
                            errorMetrics.recordRetryAttempt(operationName);
                            
                            // Calculate backoff time
                            long backoffMs = retryPolicy.getBackoffTimeMs(currentAttempt);
                            
                            logger.warn("Recovery failed, will retry operation '{}' after backoff ({} ms): {}", 
                                    new Object[] { operationName, backoffMs, ftpException.getMessage() });
                            
                            // Wait before retrying
                            try {
                                if (backoffMs > 0) {
                                    Thread.sleep(backoffMs);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw ftpException;
                            }
                            
                            continue;
                        }
                    }
                    
                } else {
                    // For non-FTPOperationException errors, convert and handle
                    lastException = ioException;
                    recordError(FTPErrorType.UNEXPECTED_ERROR, ioException.getMessage());
                    
                    // If retries are possible, attempt a simple retry with backoff
                    if (currentAttempt < retryPolicy.getMaxRetries() && isLikelyRecoverable(ioException)) {
                        currentAttempt++;
                        errorMetrics.recordRetryAttempt(operationName);
                        
                        // Calculate backoff time
                        long backoffMs = retryPolicy.getBackoffTimeMs(currentAttempt);
                        
                        logger.warn("Recoverable IO error, will retry operation '{}' after backoff ({} ms): {}", 
                                new Object[] { operationName, backoffMs, ioException.getMessage() });
                        
                        // Wait before retrying
                        try {
                            if (backoffMs > 0) {
                                Thread.sleep(backoffMs);
                            }
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw ioException;
                        }
                        
                        continue;
                    }
                }
                
                // If we reach here, either the error isn't recoverable or we've used all retries
                throw lastException;
            }
        }
        
        // This should be unreachable, but just in case
        throw lastException != null ? lastException : 
                new IOException("Operation failed without a specific exception");
    }
    
    /**
     * Executes an FTP operation with error handling for NiFi processors.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationName A name for the operation (for logging and metrics)
     * @param context The process context
     * @param session The process session
     * @return The result of the operation
     * @throws IOException if the operation fails after all recovery attempts
     */
    public <T> T executeWithErrorHandling(Callable<T> operation, String operationName,
            ProcessContext context, ProcessSession session) throws IOException {
        
        try {
            return executeWithErrorHandling(operation, operationName);
        } catch (IOException e) {
            // Penalize session on failure if available
            if (session != null) {
                session.penalize();
            }
            
            // If we have an FTP exception, report it to NiFi bulletins
            if (e instanceof FTPOperationException) {
                FTPOperationException ftpException = (FTPOperationException) e;
                
                Severity severity = ftpException.isRecoverable() ? 
                        Severity.WARNING : Severity.ERROR;
                
                bulletinReporter.reportError(context, session, 
                        ftpException.getErrorType(), severity, 
                        "FTP operation '" + operationName + "' failed: " + ftpException.getMessage());
            }
            
            throw e;
        }
    }
    
    /**
     * Performs a simple operation with void return and error handling.
     *
     * @param operation The operation to perform
     * @param operationName A name for the operation
     * @throws IOException if the operation fails after all recovery attempts
     */
    public void executeVoidWithErrorHandling(FTPRetryPolicy.VoidOperation operation, 
            String operationName) throws IOException {
        
        executeWithErrorHandling(() -> {
            operation.execute();
            return null;
        }, operationName);
    }
    
    /**
     * Records an error for metrics tracking.
     *
     * @param errorType The type of error
     * @param errorMessage The error message
     */
    private void recordError(FTPErrorType errorType, String errorMessage) {
        totalErrorCount.incrementAndGet();
        lastErrorTime.set(System.currentTimeMillis());
        lastErrorMessage = errorMessage;
        lastErrorType = errorType;
    }
    
    /**
     * Records a successful operation.
     */
    private void recordSuccess() {
        successfulOperationCount.incrementAndGet();
    }
    
    /**
     * Gets the appropriate retry policy for an operation.
     *
     * @param operationName The name of the operation
     * @return The retry policy to use
     */
    public FTPRetryPolicy getRetryPolicy(String operationName) {
        // Check for specific retry policies
        if (operationName != null) {
            // Convert operation name to lower case for case-insensitive matching
            String lowerName = operationName.toLowerCase();
            
            // Check for operation-specific policies
            if (lowerName.contains("list") || lowerName.contains("directory") || lowerName.contains("ls")) {
                return retryPolicies.getOrDefault("listing", retryPolicies.get("default"));
            } else if (lowerName.contains("transfer") || lowerName.contains("upload") || 
                    lowerName.contains("download") || lowerName.contains("get") || 
                    lowerName.contains("put") || lowerName.contains("stream")) {
                return retryPolicies.getOrDefault("transfer", retryPolicies.get("default"));
            } else if (lowerName.contains("connect") || lowerName.contains("login") || 
                    lowerName.contains("authentication")) {
                return retryPolicies.getOrDefault("connection", retryPolicies.get("default"));
            }
        }
        
        // Default policy if no specific one is found
        return retryPolicies.get("default");
    }
    
    /**
     * Checks if an error type is related to server health.
     *
     * @param errorType The error type to check
     * @return true if the error indicates a server issue
     */
    private boolean isServerError(FTPErrorType errorType) {
        return errorType == FTPErrorType.SERVER_ERROR ||
               errorType == FTPErrorType.CONNECTION_REFUSED ||
               errorType == FTPErrorType.CONNECTION_TIMEOUT ||
               errorType == FTPErrorType.CONNECTION_CLOSED;
    }
    
    /**
     * Attempts to determine if a generic IOException is likely to be recoverable.
     *
     * @param exception The exception to check
     * @return true if the exception is likely recoverable
     */
    private boolean isLikelyRecoverable(IOException exception) {
        if (exception == null || exception.getMessage() == null) {
            return false;
        }
        
        String message = exception.getMessage().toLowerCase();
        
        // Check for common network-related errors that might be transient
        return message.contains("timeout") || 
               message.contains("connection reset") || 
               message.contains("broken pipe") || 
               message.contains("end of stream") || 
               message.contains("connection closed") ||
               message.contains("socket") ||
               message.contains("reset") ||
               message.contains("i/o error");
    }
    
    /**
     * Adds a custom retry policy.
     *
     * @param name The name of the policy
     * @param policy The retry policy to add
     */
    public void addRetryPolicy(String name, FTPRetryPolicy policy) {
        retryPolicies.put(name, policy);
    }
    
    /**
     * Adds a custom circuit breaker.
     *
     * @param name The name of the circuit breaker
     * @param circuitBreaker The circuit breaker to add
     */
    public void addCircuitBreaker(String name, FTPCircuitBreaker circuitBreaker) {
        circuitBreakers.put(name, circuitBreaker);
    }
    
    /**
     * Resets all circuit breakers.
     */
    public void resetCircuitBreakers() {
        circuitBreakers.values().forEach(FTPCircuitBreaker::reset);
    }
    
    /**
     * Gets metrics about errors and recovery attempts.
     *
     * @return A map of error metrics
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Add basic counts
        metrics.put("totalErrors", totalErrorCount.get());
        metrics.put("successfulOperations", successfulOperationCount.get());
        metrics.put("lastErrorTime", lastErrorTime.get() > 0 ? 
                new java.util.Date(lastErrorTime.get()) : null);
        metrics.put("lastErrorMessage", lastErrorMessage);
        metrics.put("lastErrorType", lastErrorType != null ? lastErrorType.name() : null);
        
        // Calculate error rate
        long total = totalErrorCount.get() + successfulOperationCount.get();
        double errorRate = total > 0 ? (double) totalErrorCount.get() / total * 100.0 : 0.0;
        metrics.put("errorRate", errorRate);
        
        // Add detailed recovery metrics
        metrics.put("recoveryMetrics", recoveryStrategy.getErrorStatistics());
        
        // Add circuit breaker states
        Map<String, String> breakerStates = new HashMap<>();
        for (Map.Entry<String, FTPCircuitBreaker> entry : circuitBreakers.entrySet()) {
            breakerStates.put(entry.getKey(), entry.getValue().getState().name());
        }
        metrics.put("circuitBreakerStates", breakerStates);
        
        // Add detailed error metrics
        metrics.put("errorMetrics", errorMetrics.getAllMetrics());
        
        return metrics;
    }
    
    /**
     * Gets the error recovery strategy component.
     *
     * @return The error recovery strategy
     */
    public FTPErrorRecoveryStrategy getRecoveryStrategy() {
        return recoveryStrategy;
    }
    
    /**
     * Gets the bulletin reporter component.
     *
     * @return The bulletin reporter
     */
    public FTPBulletinReporter getBulletinReporter() {
        return bulletinReporter;
    }
    
    /**
     * Gets the error metrics component.
     *
     * @return The error metrics
     */
    public FTPErrorMetrics getErrorMetrics() {
        return errorMetrics;
    }
    
    /**
     * Gets a circuit breaker by name.
     *
     * @param name The name of the circuit breaker
     * @return The circuit breaker, or null if not found
     */
    public FTPCircuitBreaker getCircuitBreaker(String name) {
        return circuitBreakers.get(name);
    }
    
    /**
     * Gets the default circuit breaker for server protection.
     *
     * @return The server circuit breaker
     */
    public FTPCircuitBreaker getServerCircuitBreaker() {
        return circuitBreakers.get("server");
    }
}