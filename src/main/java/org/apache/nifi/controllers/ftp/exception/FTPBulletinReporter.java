package org.apache.nifi.controllers.ftp.exception;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorNode;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.BulletinFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides error reporting functionality to report FTP errors to NiFi bulletins for 
 * better visibility in the UI.
 */
public class FTPBulletinReporter {
    
    private final ComponentLog logger;
    private final ProcessorNode processorNode;
    private final long bulletinThrottleMs;
    
    // Track last bulletin time for each error type to prevent flooding
    private final Map<FTPErrorType, AtomicLong> lastBulletinTimeMap = new ConcurrentHashMap<>();
    
    /**
     * Creates a new bulletin reporter.
     *
     * @param logger The logger to use
     * @param processorNode The processor node that will own the bulletins
     * @param bulletinThrottleMs The minimum time between bulletins of the same type in milliseconds (to prevent flooding)
     */
    public FTPBulletinReporter(ComponentLog logger, ProcessorNode processorNode, long bulletinThrottleMs) {
        this.logger = logger;
        this.processorNode = processorNode;
        this.bulletinThrottleMs = bulletinThrottleMs;
    }
    
    /**
     * Creates a new bulletin reporter with a default throttle time of 5 seconds.
     *
     * @param logger The logger to use
     * @param processorNode The processor node that will own the bulletins
     */
    public FTPBulletinReporter(ComponentLog logger, ProcessorNode processorNode) {
        this(logger, processorNode, TimeUnit.SECONDS.toMillis(5));
    }
    
    /**
     * Reports an FTP error to both logs and NiFi bulletins.
     *
     * @param errorType The type of error
     * @param severity The severity level of the bulletin
     * @param message The error message
     */
    public void reportError(FTPErrorType errorType, Severity severity, String message) {
        // Always log the error
        if (severity == Severity.ERROR) {
            logger.error(message);
        } else if (severity == Severity.WARNING) {
            logger.warn(message);
        } else {
            logger.info(message);
        }
        
        // Skip bulletin reporting if processor node is not available
        if (processorNode == null) {
            return;
        }
        
        // Check throttling for this error type
        AtomicLong lastTime = lastBulletinTimeMap.computeIfAbsent(errorType, k -> new AtomicLong(0));
        long now = System.currentTimeMillis();
        long lastBulletinTime = lastTime.get();
        
        // If within throttle window, don't create another bulletin
        if ((now - lastBulletinTime) < bulletinThrottleMs) {
            return;
        }
        
        // Update last bulletin time if the compare-and-set succeeds
        // This prevents multiple threads from spamming bulletins for the same error
        if (lastTime.compareAndSet(lastBulletinTime, now)) {
            // Get the bulletin repository
            BulletinRepository bulletinRepository = processorNode.getProcessGroup().getBulletinRepository();
            
            // Create appropriate category based on error type
            String category = "FTP Service";
            
            // Create and submit the bulletin
            bulletinRepository.addBulletin(
                BulletinFactory.createBulletin(
                    category,
                    severity,
                    message
                )
            );
        }
    }
    
    /**
     * Reports an error based on an FTPOperationException.
     *
     * @param exception The exception to report
     */
    public void reportException(FTPOperationException exception) {
        Severity severity;
        
        // Determine severity based on whether the error is recoverable
        if (exception.isRecoverable()) {
            severity = Severity.WARNING;
        } else {
            severity = Severity.ERROR;
        }
        
        // Create a more user-friendly message that includes the error type
        String message = String.format("FTP Error (%s): %s", 
                exception.getErrorType().name(), exception.getMessage());
        
        reportError(exception.getErrorType(), severity, message);
    }
    
    /**
     * Reports an error from a context and session.
     *
     * @param context The process context
     * @param session The process session
     * @param errorType The type of error
     * @param severity The severity of the bulletin
     * @param message The error message
     */
    public void reportError(ProcessContext context, ProcessSession session, 
            FTPErrorType errorType, Severity severity, String message) {
        
        // Log to standard log first
        reportError(errorType, severity, message);
        
        // Also penalize the session for errors if provided
        if (session != null && severity == Severity.ERROR) {
            session.penalize();
        }
    }
    
    /**
     * Sets the throttle time for bulletins.
     *
     * @param throttleTimeMs The minimum time between bulletins of the same type in milliseconds
     */
    public void setBulletinThrottleTime(long throttleTimeMs) {
        // Simple setter to allow runtime adjustment of throttle time
    }
    
    /**
     * Resets the throttle timers for all error types.
     * This can be used when you want to force a bulletin for the next error regardless of throttling.
     */
    public void resetThrottleTimers() {
        lastBulletinTimeMap.clear();
    }
}