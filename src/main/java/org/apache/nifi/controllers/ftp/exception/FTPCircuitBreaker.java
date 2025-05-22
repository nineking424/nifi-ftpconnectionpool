package org.apache.nifi.controllers.ftp.exception;

import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of the Circuit Breaker pattern for FTP operations.
 * The circuit breaker prevents cascading failures by failing fast when a service appears to be unavailable.
 */
public class FTPCircuitBreaker {
    
    private final ComponentLog logger;
    private final String name;
    private final int failureThreshold;
    private final long resetTimeoutMs;
    
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastErrorTime = new AtomicLong(0);
    private final AtomicLong lastStateTransitionTime = new AtomicLong(System.currentTimeMillis());
    
    /**
     * The possible states of the circuit breaker.
     */
    public enum State {
        /**
         * Circuit is closed, operations are allowed.
         */
        CLOSED,
        
        /**
         * Circuit is open, operations fail fast.
         */
        OPEN,
        
        /**
         * Circuit is half-open, allowing a single test operation to check if service is back.
         */
        HALF_OPEN
    }
    
    /**
     * Creates a new FTPCircuitBreaker.
     *
     * @param logger The logger to use
     * @param name A name for this circuit breaker (for logging)
     * @param failureThreshold The number of consecutive failures that will trip the circuit
     * @param resetTimeoutMs The time in milliseconds after which to attempt to close the circuit again
     */
    public FTPCircuitBreaker(ComponentLog logger, String name, int failureThreshold, long resetTimeoutMs) {
        this.logger = logger;
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }
    
    /**
     * Executes an operation with circuit breaker protection.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationDescription A description of the operation for logging
     * @return The result of the operation
     * @throws IOException if the operation fails or the circuit is open
     */
    public <T> T execute(Callable<T> operation, String operationDescription) throws IOException {
        State currentState = state.get();
        
        // If the circuit is open, check if it's time to try again
        if (currentState == State.OPEN) {
            if (System.currentTimeMillis() - lastErrorTime.get() > resetTimeoutMs) {
                // Try to transition to half-open state
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    logger.info("Circuit breaker '{}' transitioning from OPEN to HALF_OPEN state", name);
                    recordStateTransition();
                }
            } else {
                // Circuit is still open, fail fast
                throw new FTPOperationException(FTPErrorType.CONNECTION_ERROR, 
                        "Circuit breaker '" + name + "' is open. Operation: " + operationDescription);
            }
        }
        
        // At this point, the circuit is either CLOSED or HALF_OPEN
        try {
            T result = operation.call();
            recordSuccess();
            return result;
        } catch (Exception e) {
            recordFailure(e);
            
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Error executing operation: " + e.getMessage(), e);
            }
        }
    }
    
    /**
     * Records a successful operation.
     */
    private void recordSuccess() {
        State currentState = state.get();
        
        if (currentState == State.HALF_OPEN) {
            // Reset the circuit if we were testing in half-open state
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                logger.info("Circuit breaker '{}' transitioning from HALF_OPEN to CLOSED state", name);
                recordStateTransition();
            }
        }
        
        // Reset failure count on success
        failureCount.set(0);
    }
    
    /**
     * Records a failed operation.
     *
     * @param e The exception that occurred
     */
    private void recordFailure(Exception e) {
        State currentState = state.get();
        
        // Update last error time
        lastErrorTime.set(System.currentTimeMillis());
        
        // Handle based on current state
        if (currentState == State.CLOSED) {
            int count = failureCount.incrementAndGet();
            if (count >= failureThreshold) {
                // Trip the circuit
                if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                    logger.warn("Circuit breaker '{}' tripped OPEN after {} consecutive failures. Last error: {}", 
                            new Object[] { name, count, e.getMessage() });
                    recordStateTransition();
                }
            }
        } else if (currentState == State.HALF_OPEN) {
            // Trip back to open on failure in half-open state
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                logger.warn("Circuit breaker '{}' returned to OPEN state from HALF_OPEN due to failure: {}", 
                        new Object[] { name, e.getMessage() });
                recordStateTransition();
            }
        }
    }
    
    /**
     * Records the time of a state transition.
     */
    private void recordStateTransition() {
        lastStateTransitionTime.set(System.currentTimeMillis());
    }
    
    /**
     * Gets the current state of the circuit breaker.
     *
     * @return The current state
     */
    public State getState() {
        return state.get();
    }
    
    /**
     * Gets the current failure count.
     *
     * @return The number of consecutive failures
     */
    public int getFailureCount() {
        return failureCount.get();
    }
    
    /**
     * Gets the time of the last error.
     *
     * @return The timestamp of the last error in milliseconds since the epoch
     */
    public long getLastErrorTime() {
        return lastErrorTime.get();
    }
    
    /**
     * Gets the time of the last state transition.
     *
     * @return The timestamp of the last state transition in milliseconds since the epoch
     */
    public long getLastStateTransitionTime() {
        return lastStateTransitionTime.get();
    }
    
    /**
     * Forcibly resets the circuit breaker to CLOSED state.
     */
    public void reset() {
        state.set(State.CLOSED);
        failureCount.set(0);
        recordStateTransition();
        logger.info("Circuit breaker '{}' manually reset to CLOSED state", name);
    }
}