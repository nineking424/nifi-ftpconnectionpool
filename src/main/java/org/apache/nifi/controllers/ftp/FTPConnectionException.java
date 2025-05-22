package org.apache.nifi.controllers.ftp;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * Exception thrown when there is an error with an FTP connection.
 */
public class FTPConnectionException extends ProcessException {
    
    /**
     * The type of FTP connection error.
     */
    public enum ErrorType {
        /**
         * Error during connection establishment.
         */
        CONNECTION_ERROR("Failed to establish connection"),
        
        /**
         * Error during authentication.
         */
        AUTHENTICATION_ERROR("Authentication failed"),
        
        /**
         * Error during data transfer.
         */
        TRANSFER_ERROR("Data transfer failed"),
        
        /**
         * Connection timeout.
         */
        TIMEOUT_ERROR("Connection timed out"),
        
        /**
         * Error with connection configuration.
         */
        CONFIGURATION_ERROR("Invalid connection configuration"),
        
        /**
         * Connection was closed unexpectedly.
         */
        CONNECTION_CLOSED("Connection closed unexpectedly"),
        
        /**
         * Error occurred during connection validation.
         */
        VALIDATION_ERROR("Connection validation failed"),
        
        /**
         * No valid connection available.
         */
        NO_CONNECTION("No valid connection available"),
        
        /**
         * Server returned an error response.
         */
        SERVER_ERROR("Server error"),
        
        /**
         * Error during connection pool operations.
         */
        POOL_ERROR("Connection pool error"),
        
        /**
         * Unknown or unspecified error.
         */
        UNKNOWN_ERROR("Unknown error");
        
        private final String defaultMessage;
        
        ErrorType(String defaultMessage) {
            this.defaultMessage = defaultMessage;
        }
        
        /**
         * Gets the default message for this error type.
         * 
         * @return the default message
         */
        public String getDefaultMessage() {
            return defaultMessage;
        }
    }
    
    private final ErrorType errorType;
    private final String connectionId;
    private final String host;
    private final Integer port;
    private final Integer replyCode;
    
    /**
     * Creates a new FTPConnectionException with the specified error type and message.
     * 
     * @param errorType the type of error
     * @param message the error message
     */
    public FTPConnectionException(ErrorType errorType, String message) {
        super(message);
        this.errorType = errorType;
        this.connectionId = null;
        this.host = null;
        this.port = null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPConnectionException with the specified error type, message, and cause.
     * 
     * @param errorType the type of error
     * @param message the error message
     * @param cause the cause of the error
     */
    public FTPConnectionException(ErrorType errorType, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.connectionId = null;
        this.host = null;
        this.port = null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPConnectionException with the specified error type and connection details.
     * 
     * @param errorType the type of error
     * @param connection the FTP connection
     * @param message the error message
     */
    public FTPConnectionException(ErrorType errorType, FTPConnection connection, String message) {
        super(message);
        this.errorType = errorType;
        this.connectionId = connection != null ? connection.getId() : null;
        this.host = connection != null ? connection.getConfig().getHostname() : null;
        this.port = connection != null ? connection.getConfig().getPort() : null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPConnectionException with the specified error type, connection details, and cause.
     * 
     * @param errorType the type of error
     * @param connection the FTP connection
     * @param message the error message
     * @param cause the cause of the error
     */
    public FTPConnectionException(ErrorType errorType, FTPConnection connection, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.connectionId = connection != null ? connection.getId() : null;
        this.host = connection != null ? connection.getConfig().getHostname() : null;
        this.port = connection != null ? connection.getConfig().getPort() : null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPConnectionException with the specified error type, host, port, and reply code.
     * 
     * @param errorType the type of error
     * @param host the FTP server hostname
     * @param port the FTP server port
     * @param replyCode the FTP server reply code
     * @param message the error message
     */
    public FTPConnectionException(ErrorType errorType, String host, int port, int replyCode, String message) {
        super(message);
        this.errorType = errorType;
        this.connectionId = null;
        this.host = host;
        this.port = port;
        this.replyCode = replyCode;
    }
    
    /**
     * Gets the type of error.
     * 
     * @return the error type
     */
    public ErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Gets the ID of the connection that had the error.
     * 
     * @return the connection ID, or null if not available
     */
    public String getConnectionId() {
        return connectionId;
    }
    
    /**
     * Gets the hostname of the FTP server.
     * 
     * @return the hostname, or null if not available
     */
    public String getHost() {
        return host;
    }
    
    /**
     * Gets the port of the FTP server.
     * 
     * @return the port, or null if not available
     */
    public Integer getPort() {
        return port;
    }
    
    /**
     * Gets the FTP server reply code.
     * 
     * @return the reply code, or null if not available
     */
    public Integer getReplyCode() {
        return replyCode;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FTP ");
        sb.append(errorType.name());
        
        if (host != null) {
            sb.append(" for server ").append(host);
            if (port != null) {
                sb.append(":").append(port);
            }
        }
        
        if (connectionId != null) {
            sb.append(" on connection ").append(connectionId);
        }
        
        if (replyCode != null) {
            sb.append(" (reply code: ").append(replyCode).append(")");
        }
        
        sb.append(": ").append(getMessage());
        
        return sb.toString();
    }
}