package org.apache.nifi.controllers.ftp.exception;

/**
 * Exception thrown when authentication with an FTP server fails.
 */
public class FTPAuthenticationException extends FTPConnectionException {
    
    private final String username;
    
    /**
     * Creates a new FTPAuthenticationException.
     *
     * @param errorType The type of error that occurred
     * @param username The username that was used for authentication
     * @param message The error message
     */
    public FTPAuthenticationException(FTPErrorType errorType, String username, String message) {
        super(errorType, message);
        this.username = username;
    }
    
    /**
     * Creates a new FTPAuthenticationException.
     *
     * @param errorType The type of error that occurred
     * @param username The username that was used for authentication
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPAuthenticationException(FTPErrorType errorType, String username, String message, Throwable cause) {
        super(errorType, message, cause);
        this.username = username;
    }
    
    /**
     * Creates a new FTPAuthenticationException.
     *
     * @param errorType The type of error that occurred
     * @param ftpReplyCode The FTP server reply code
     * @param username The username that was used for authentication
     * @param message The error message
     */
    public FTPAuthenticationException(FTPErrorType errorType, int ftpReplyCode, String username, String message) {
        super(errorType, ftpReplyCode, null, message);
        this.username = username;
    }
    
    /**
     * Gets the username that was used for authentication.
     *
     * @return The username
     */
    public String getUsername() {
        return username;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[errorType=").append(getErrorType());
        
        if (getFtpReplyCode() != -1) {
            sb.append(", replyCode=").append(getFtpReplyCode());
        }
        
        sb.append(", username=").append(username);
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