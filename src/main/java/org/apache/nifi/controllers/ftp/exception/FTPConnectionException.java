package org.apache.nifi.controllers.ftp.exception;

import org.apache.nifi.controllers.ftp.FTPConnection;

/**
 * Exception thrown when an error occurs with an FTP connection.
 */
public class FTPConnectionException extends FTPOperationException {
    
    private final FTPConnection connection;
    
    /**
     * Creates a new FTPConnectionException.
     *
     * @param errorType The type of error that occurred
     * @param message The error message
     */
    public FTPConnectionException(FTPErrorType errorType, String message) {
        super(errorType, message);
        this.connection = null;
    }
    
    /**
     * Creates a new FTPConnectionException.
     *
     * @param errorType The type of error that occurred
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPConnectionException(FTPErrorType errorType, String message, Throwable cause) {
        super(errorType, message, cause);
        this.connection = null;
    }
    
    /**
     * Creates a new FTPConnectionException.
     *
     * @param errorType The type of error that occurred
     * @param connection The FTP connection that had the error
     * @param message The error message
     */
    public FTPConnectionException(FTPErrorType errorType, FTPConnection connection, String message) {
        super(errorType, message);
        this.connection = connection;
    }
    
    /**
     * Creates a new FTPConnectionException.
     *
     * @param errorType The type of error that occurred
     * @param connection The FTP connection that had the error
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPConnectionException(FTPErrorType errorType, FTPConnection connection, String message, Throwable cause) {
        super(errorType, message, cause);
        this.connection = connection;
    }
    
    /**
     * Creates a new FTPConnectionException.
     *
     * @param errorType The type of error that occurred
     * @param ftpReplyCode The FTP server reply code
     * @param connection The FTP connection that had the error
     * @param message The error message
     */
    public FTPConnectionException(FTPErrorType errorType, int ftpReplyCode, FTPConnection connection, String message) {
        super(errorType, ftpReplyCode, message);
        this.connection = connection;
    }
    
    /**
     * Gets the FTP connection that had the error.
     *
     * @return The FTP connection, or null if not available
     */
    public FTPConnection getConnection() {
        return connection;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[errorType=").append(getErrorType());
        
        if (getFtpReplyCode() != -1) {
            sb.append(", replyCode=").append(getFtpReplyCode());
        }
        
        if (connection != null) {
            sb.append(", connectionId=").append(connection.getId());
            sb.append(", connectionState=").append(connection.getState());
        }
        
        sb.append(", message=").append(getMessage());
        
        Throwable cause = getCause();
        if (cause != null) {
            sb.append(", cause=").append(cause.getClass().getSimpleName())
              .append(": ").append(cause.getMessage());
        }
        
        sb.append("]");
        return sb.toString();
    }
}