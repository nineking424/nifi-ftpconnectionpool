package org.apache.nifi.controllers.ftp.exception;

/**
 * Exception thrown when an error occurs during file operations (non-transfer).
 */
public class FTPFileOperationException extends FTPOperationException {
    
    private final String remotePath;
    private final FileOperation operation;
    
    /**
     * Enumeration of file operations.
     */
    public enum FileOperation {
        LIST,
        DELETE,
        RENAME,
        MAKE_DIRECTORY,
        REMOVE_DIRECTORY,
        GET_ATTRIBUTES,
        SET_ATTRIBUTES,
        CHECK_EXISTS,
        GET_SIZE,
        GET_MODIFICATION_TIME,
        SET_MODIFICATION_TIME,
        SET_PERMISSIONS
    }
    
    /**
     * Creates a new FTPFileOperationException.
     *
     * @param errorType The type of error that occurred
     * @param remotePath The remote file path
     * @param operation The file operation that failed
     * @param message The error message
     */
    public FTPFileOperationException(FTPErrorType errorType, String remotePath, FileOperation operation, String message) {
        super(errorType, message);
        this.remotePath = remotePath;
        this.operation = operation;
    }
    
    /**
     * Creates a new FTPFileOperationException.
     *
     * @param errorType The type of error that occurred
     * @param remotePath The remote file path
     * @param operation The file operation that failed
     * @param message The error message
     * @param cause The cause of the exception
     */
    public FTPFileOperationException(FTPErrorType errorType, String remotePath, FileOperation operation, String message, Throwable cause) {
        super(errorType, message, cause);
        this.remotePath = remotePath;
        this.operation = operation;
    }
    
    /**
     * Creates a new FTPFileOperationException.
     *
     * @param errorType The type of error that occurred
     * @param ftpReplyCode The FTP server reply code
     * @param remotePath The remote file path
     * @param operation The file operation that failed
     * @param message The error message
     */
    public FTPFileOperationException(FTPErrorType errorType, int ftpReplyCode, String remotePath, FileOperation operation, String message) {
        super(errorType, ftpReplyCode, message);
        this.remotePath = remotePath;
        this.operation = operation;
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
     * Gets the file operation that failed.
     *
     * @return The file operation
     */
    public FileOperation getOperation() {
        return operation;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("[errorType=").append(getErrorType());
        
        if (getFtpReplyCode() != -1) {
            sb.append(", replyCode=").append(getFtpReplyCode());
        }
        
        sb.append(", operation=").append(operation);
        sb.append(", remotePath=").append(remotePath);
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