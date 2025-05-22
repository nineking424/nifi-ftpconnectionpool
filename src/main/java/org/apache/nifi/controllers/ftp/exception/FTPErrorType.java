package org.apache.nifi.controllers.ftp.exception;

/**
 * Enumeration of error types for FTP operations.
 */
public enum FTPErrorType {
    // Connection Errors
    CONNECTION_ERROR(true, "Failed to establish connection to FTP server"),
    CONNECTION_TIMEOUT(true, "Connection timed out while connecting to FTP server"),
    CONNECTION_CLOSED(true, "Connection was closed by the server"),
    CONNECTION_REFUSED(true, "Connection was refused by the server"),
    
    // Authentication Errors
    AUTHENTICATION_ERROR(false, "Failed to authenticate with the FTP server"),
    INVALID_CREDENTIALS(false, "Invalid username or password"),
    INSUFFICIENT_PERMISSIONS(false, "Insufficient permissions for the requested operation"),
    
    // File Operation Errors
    FILE_NOT_FOUND(false, "File not found"),
    FILE_ALREADY_EXISTS(false, "File already exists"),
    DIRECTORY_NOT_FOUND(false, "Directory not found"),
    DIRECTORY_NOT_EMPTY(false, "Directory is not empty"),
    INVALID_PATH(false, "Invalid file path"),
    
    // Transfer Errors
    TRANSFER_ERROR(true, "Error during file transfer"),
    TRANSFER_ABORTED(true, "File transfer was aborted"),
    TRANSFER_TIMEOUT(true, "File transfer timed out"),
    INSUFFICIENT_STORAGE(false, "Insufficient storage space on the server"),
    
    // Data Channel Errors
    DATA_CONNECTION_ERROR(true, "Failed to establish data connection"),
    DATA_CONNECTION_TIMEOUT(true, "Data connection timed out"),
    
    // Server Errors
    SERVER_ERROR(true, "FTP server error"),
    COMMAND_NOT_SUPPORTED(false, "Command not supported by the server"),
    INVALID_SEQUENCE(false, "Invalid command sequence"),
    
    // Client Errors
    CLIENT_ERROR(false, "FTP client error"),
    INVALID_CONFIGURATION(false, "Invalid client configuration"),
    
    // Pool Errors
    POOL_EXHAUSTED(true, "Connection pool exhausted"),
    POOL_ERROR(false, "Error in connection pool management"),
    
    // General Errors
    UNEXPECTED_ERROR(false, "Unexpected error occurred"),
    VALIDATION_ERROR(false, "Validation error");
    
    private final boolean recoverable;
    private final String defaultMessage;
    
    /**
     * Creates a new FTPErrorType.
     *
     * @param recoverable Whether the error is potentially recoverable
     * @param defaultMessage The default error message
     */
    FTPErrorType(boolean recoverable, String defaultMessage) {
        this.recoverable = recoverable;
        this.defaultMessage = defaultMessage;
    }
    
    /**
     * Checks if this error type is potentially recoverable.
     * A recoverable error is one that might succeed if retried.
     *
     * @return true if the error is recoverable, false otherwise
     */
    public boolean isRecoverable() {
        return recoverable;
    }
    
    /**
     * Gets the default error message for this error type.
     *
     * @return The default error message
     */
    public String getDefaultMessage() {
        return defaultMessage;
    }
}