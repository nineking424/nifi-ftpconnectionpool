package org.apache.nifi.controllers.ftp.exception;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects metrics about FTP errors for monitoring and diagnostics.
 */
public class FTPErrorMetrics {
    
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalRecoverableErrors = new AtomicLong(0);
    private final AtomicLong totalNonRecoverableErrors = new AtomicLong(0);
    private final AtomicLong totalRetriedOperations = new AtomicLong(0);
    private final AtomicLong totalSuccessfulRetries = new AtomicLong(0);
    private final Map<FTPErrorType, AtomicLong> errorTypeCounts = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> replyCodeCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> operationErrorCounts = new ConcurrentHashMap<>();
    
    /**
     * Records an error occurred during an FTP operation.
     *
     * @param exception The exception that occurred
     * @param operationName The name of the operation that failed
     */
    public void recordError(FTPOperationException exception, String operationName) {
        // Update total error counts
        totalErrors.incrementAndGet();
        
        if (exception.isRecoverable()) {
            totalRecoverableErrors.incrementAndGet();
        } else {
            totalNonRecoverableErrors.incrementAndGet();
        }
        
        // Update error type counts
        errorTypeCounts.computeIfAbsent(exception.getErrorType(), k -> new AtomicLong(0)).incrementAndGet();
        
        // Update reply code counts if available
        int replyCode = exception.getFtpReplyCode();
        if (replyCode != -1) {
            replyCodeCounts.computeIfAbsent(replyCode, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        // Update operation error counts
        operationErrorCounts.computeIfAbsent(operationName, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Records a retry attempt for an operation.
     *
     * @param operationName The name of the operation being retried
     */
    public void recordRetryAttempt(String operationName) {
        totalRetriedOperations.incrementAndGet();
    }
    
    /**
     * Records a successful retry for an operation.
     *
     * @param operationName The name of the operation that succeeded after retry
     */
    public void recordSuccessfulRetry(String operationName) {
        totalSuccessfulRetries.incrementAndGet();
    }
    
    /**
     * Gets the total number of errors recorded.
     *
     * @return The total error count
     */
    public long getTotalErrors() {
        return totalErrors.get();
    }
    
    /**
     * Gets the total number of recoverable errors recorded.
     *
     * @return The recoverable error count
     */
    public long getTotalRecoverableErrors() {
        return totalRecoverableErrors.get();
    }
    
    /**
     * Gets the total number of non-recoverable errors recorded.
     *
     * @return The non-recoverable error count
     */
    public long getTotalNonRecoverableErrors() {
        return totalNonRecoverableErrors.get();
    }
    
    /**
     * Gets the total number of operations that were retried.
     *
     * @return The retried operation count
     */
    public long getTotalRetriedOperations() {
        return totalRetriedOperations.get();
    }
    
    /**
     * Gets the total number of operations that succeeded after being retried.
     *
     * @return The successful retry count
     */
    public long getTotalSuccessfulRetries() {
        return totalSuccessfulRetries.get();
    }
    
    /**
     * Gets the count of errors by error type.
     *
     * @return A map of error types to their counts
     */
    public Map<FTPErrorType, Long> getErrorTypeCounts() {
        Map<FTPErrorType, Long> result = new ConcurrentHashMap<>();
        for (Map.Entry<FTPErrorType, AtomicLong> entry : errorTypeCounts.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
    
    /**
     * Gets the count of errors by reply code.
     *
     * @return A map of reply codes to their counts
     */
    public Map<Integer, Long> getReplyCodeCounts() {
        Map<Integer, Long> result = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, AtomicLong> entry : replyCodeCounts.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
    
    /**
     * Gets the count of errors by operation.
     *
     * @return A map of operation names to their error counts
     */
    public Map<String, Long> getOperationErrorCounts() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, AtomicLong> entry : operationErrorCounts.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
    
    /**
     * Resets all error metrics to zero.
     */
    public void reset() {
        totalErrors.set(0);
        totalRecoverableErrors.set(0);
        totalNonRecoverableErrors.set(0);
        totalRetriedOperations.set(0);
        totalSuccessfulRetries.set(0);
        errorTypeCounts.clear();
        replyCodeCounts.clear();
        operationErrorCounts.clear();
    }
    
    /**
     * Gets the retry success rate as a percentage.
     *
     * @return The percentage of retries that succeeded, or 0 if no retries have been attempted
     */
    public double getRetrySuccessRate() {
        long retries = totalRetriedOperations.get();
        if (retries == 0) {
            return 0.0;
        }
        return (double) totalSuccessfulRetries.get() / retries * 100.0;
    }
    
    /**
     * Gets a map of all metrics.
     *
     * @return A map of metric names to their values
     */
    public Map<String, Object> getAllMetrics() {
        Map<String, Object> metrics = new ConcurrentHashMap<>();
        
        metrics.put("totalErrors", getTotalErrors());
        metrics.put("totalRecoverableErrors", getTotalRecoverableErrors());
        metrics.put("totalNonRecoverableErrors", getTotalNonRecoverableErrors());
        metrics.put("totalRetriedOperations", getTotalRetriedOperations());
        metrics.put("totalSuccessfulRetries", getTotalSuccessfulRetries());
        metrics.put("retrySuccessRate", getRetrySuccessRate());
        metrics.put("errorTypeCounts", getErrorTypeCounts());
        metrics.put("replyCodeCounts", getReplyCodeCounts());
        metrics.put("operationErrorCounts", getOperationErrorCounts());
        
        return metrics;
    }
}