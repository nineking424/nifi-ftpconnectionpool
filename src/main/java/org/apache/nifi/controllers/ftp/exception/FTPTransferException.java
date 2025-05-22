package org.apache.nifi.controllers.ftp.exception;

/**
 * Exception thrown when an error occurs during file transfer operations.
 */
public class FTPTransferException extends FTPOperationException {
    
    private final String remotePath;
    private final long bytesTransferred;
    
    /**
     * Creates a new FTPTransferException.
     *
     * @param errorType The type of error that occurred
     * @param remotePath The remote file path
     * @param message The error message
     */
    public FTPTransferException(FTPErrorType errorType, String remotePath, String message) {
        this(errorType, remotePath, -1, message, null);
    }
    
    /**
     * Creates a new FTPTransferException.
     *
     * @param errorType The type of error that occurred
     * @param remotePath The remote file path
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPTransferException(FTPErrorType errorType, String remotePath, String message, Throwable cause) {
        this(errorType, remotePath, -1, message, cause);
    }
    
    /**
     * Creates a new FTPTransferException.
     *
     * @param errorType The type of error that occurred
     * @param remotePath The remote file path
     * @param bytesTransferred The number of bytes transferred before the error occurred
     * @param message The error message
     */
    public FTPTransferException(FTPErrorType errorType, String remotePath, long bytesTransferred, String message) {
        this(errorType, remotePath, bytesTransferred, message, null);
    }
    
    /**
     * Creates a new FTPTransferException.
     *
     * @param errorType The type of error that occurred
     * @param remotePath The remote file path
     * @param bytesTransferred The number of bytes transferred before the error occurred
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPTransferException(FTPErrorType errorType, String remotePath, long bytesTransferred, String message, Throwable cause) {
        super(errorType, message, cause);
        this.remotePath = remotePath;
        this.bytesTransferred = bytesTransferred;
    }
    
    /**
     * Creates a new FTPTransferException.
     *
     * @param errorType The type of error that occurred
     * @param ftpReplyCode The FTP server reply code
     * @param remotePath The remote file path
     * @param bytesTransferred The number of bytes transferred before the error occurred
     * @param message The error message
     */
    public FTPTransferException(FTPErrorType errorType, int ftpReplyCode, String remotePath, long bytesTransferred, String message) {
        super(errorType, ftpReplyCode, message);
        this.remotePath = remotePath;
        this.bytesTransferred = bytesTransferred;
    }
    
    /**
     * Gets the remote file path.
     *
     * @return The remote file path
     */
    public String getRemotePath() {
        return remotePath;
    }
    
    /**
     * Gets the number of bytes transferred before the error occurred.
     *
     * @return The number of bytes transferred, or -1 if not applicable
     */
    public long getBytesTransferred() {
        return bytesTransferred;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[errorType=").append(getErrorType());
        
        if (getFtpReplyCode() != -1) {
            sb.append(", replyCode=").append(getFtpReplyCode());
        }
        
        sb.append(", remotePath=").append(remotePath);
        
        if (bytesTransferred >= 0) {
            sb.append(", bytesTransferred=").append(bytesTransferred);
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