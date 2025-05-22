package org.apache.nifi.controllers.ftp.exception;

import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;

/**
 * Base exception class for FTP operations.
 * This is the parent class for all FTP-related exceptions in the FTP connection pool.
 */
public class FTPOperationException extends ProcessException {
    
    private final FTPErrorType errorType;
    private final int ftpReplyCode;
    private final String path;
    private final int maxRetries;
    private final long retryDelay;
    private int retryCount;
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param message The error message
     */
    public FTPOperationException(FTPErrorType errorType, String message) {
        this(errorType, null, -1, message, null);
    }
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPOperationException(FTPErrorType errorType, String message, Throwable cause) {
        this(errorType, null, -1, message, cause);
    }
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param path The path of the file or directory involved in the operation
     * @param message The error message
     */
    public FTPOperationException(FTPErrorType errorType, String path, String message) {
        this(errorType, path, -1, message, null);
    }
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param path The path of the file or directory involved in the operation
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPOperationException(FTPErrorType errorType, String path, String message, Throwable cause) {
        this(errorType, path, -1, message, cause);
    }
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param ftpReplyCode The FTP server reply code, or -1 if not applicable
     * @param message The error message
     */
    public FTPOperationException(FTPErrorType errorType, int ftpReplyCode, String message) {
        this(errorType, null, ftpReplyCode, message, null);
    }
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param path The path of the file or directory involved in the operation
     * @param ftpReplyCode The FTP server reply code, or -1 if not applicable
     * @param message The error message
     */
    public FTPOperationException(FTPErrorType errorType, String path, int ftpReplyCode, String message) {
        this(errorType, path, ftpReplyCode, message, null);
    }
    
    /**
     * Creates a new FTPOperationException.
     *
     * @param errorType The type of error that occurred
     * @param path The path of the file or directory involved in the operation
     * @param ftpReplyCode The FTP server reply code, or -1 if not applicable
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPOperationException(FTPErrorType errorType, String path, int ftpReplyCode, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.path = path;
        this.ftpReplyCode = ftpReplyCode;
        
        // Set default retry values based on error type
        if (errorType.isRecoverable()) {
            this.maxRetries = 3;
            this.retryDelay = 1000; // 1 second
        } else {
            this.maxRetries = 0;
            this.retryDelay = 0;
        }
        
        this.retryCount = 0;
    }
    
    /**
     * Creates a new FTPOperationException with custom retry settings.
     *
     * @param errorType The type of error that occurred
     * @param path The path of the file or directory involved in the operation
     * @param ftpReplyCode The FTP server reply code, or -1 if not applicable
     * @param message The error message
     * @param maxRetries The maximum number of retries for this error
     * @param retryDelay The delay between retries in milliseconds
     */
    public FTPOperationException(FTPErrorType errorType, String path, int ftpReplyCode, 
            String message, int maxRetries, long retryDelay) {
        super(message);
        this.errorType = errorType;
        this.path = path;
        this.ftpReplyCode = ftpReplyCode;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        this.retryCount = 0;
    }
    
    /**
     * Gets the type of error that occurred.
     *
     * @return The error type
     */
    public FTPErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Gets the path of the file or directory involved in the operation.
     *
     * @return The path, or null if not applicable
     */
    public String getPath() {
        return path;
    }
    
    /**
     * Gets the FTP server reply code.
     *
     * @return The FTP reply code, or -1 if not applicable
     */
    public int getFtpReplyCode() {
        return ftpReplyCode;
    }
    
    /**
     * Gets the maximum number of retries for this error.
     *
     * @return The maximum number of retries
     */
    public int getMaxRetries() {
        return maxRetries;
    }
    
    /**
     * Gets the delay between retries in milliseconds.
     *
     * @return The retry delay in milliseconds
     */
    public long getRetryDelay() {
        return retryDelay;
    }
    
    /**
     * Gets the current retry count.
     *
     * @return The current retry count
     */
    public int getRetryCount() {
        return retryCount;
    }
    
    /**
     * Increments the retry count.
     *
     * @return The new retry count
     */
    public int incrementRetryCount() {
        return ++retryCount;
    }
    
    /**
     * Checks if this exception represents a recoverable error.
     * A recoverable error is one that might succeed if retried.
     *
     * @return true if the error is recoverable, false otherwise
     */
    public boolean isRecoverable() {
        return errorType.isRecoverable();
    }
    
    /**
     * Checks if this error can be retried.
     *
     * @return true if the error can be retried, false otherwise
     */
    public boolean canRetry() {
        return isRecoverable() && retryCount < maxRetries;
    }
    
    /**
     * Interprets the FTP reply code to provide more context about the error.
     * RFC 959 defines standard reply codes for FTP.
     *
     * @return A human-readable interpretation of the FTP reply code, or null if not applicable
     */
    public String getReplyCodeInterpretation() {
        if (ftpReplyCode == -1) {
            return null;
        }
        
        // First digit indicates success (2xx), partial success (3xx), failure (4xx), or server error (5xx)
        int firstDigit = ftpReplyCode / 100;
        
        switch (firstDigit) {
            case 1: return "Positive Preliminary Reply - The requested action is being initiated";
            case 2: return "Positive Completion Reply - The requested action has been successfully completed";
            case 3: return "Positive Intermediate Reply - The command has been accepted, but requires additional information";
            case 4: return "Transient Negative Completion Reply - The command was not successful, but the error is temporary";
            case 5: return "Permanent Negative Completion Reply - The command was not successful, and the error is permanent";
            default: return "Unknown Reply Code";
        }
    }
    
    /**
     * Determines if a reply code indicates a temporary (recoverable) error.
     *
     * @return true if the reply code indicates a temporary error, false otherwise
     */
    public boolean isTemporaryError() {
        return ftpReplyCode >= 400 && ftpReplyCode < 500;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[errorType=").append(errorType);
        
        if (path != null) {
            sb.append(", path='").append(path).append("'");
        }
        
        if (ftpReplyCode != -1) {
            sb.append(", replyCode=").append(ftpReplyCode);
        }
        
        sb.append(", recoverable=").append(isRecoverable());
        sb.append(", retries=").append(retryCount).append("/").append(maxRetries);
        sb.append(", message='").append(getMessage()).append("'");
        
        Throwable cause = getCause();
        if (cause != null) {
            sb.append(", cause=").append(cause.getClass().getSimpleName())
              .append(": ").append(cause.getMessage());
        }
        
        sb.append("]");
        return sb.toString();
    }
}