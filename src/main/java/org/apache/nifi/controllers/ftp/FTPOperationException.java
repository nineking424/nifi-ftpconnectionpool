package org.apache.nifi.controllers.ftp;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * Exception thrown when there is an error performing an FTP operation.
 */
public class FTPOperationException extends ProcessException {
    
    /**
     * The type of FTP operation error.
     */
    public enum ErrorType {
        /**
         * Error during file listing.
         */
        LISTING_ERROR("Failed to list files"),
        
        /**
         * Error during file download.
         */
        DOWNLOAD_ERROR("Failed to download file"),
        
        /**
         * Error during file upload.
         */
        UPLOAD_ERROR("Failed to upload file"),
        
        /**
         * Error during file deletion.
         */
        DELETION_ERROR("Failed to delete file"),
        
        /**
         * Error during directory creation.
         */
        MKDIR_ERROR("Failed to create directory"),
        
        /**
         * Error during file/directory renaming.
         */
        RENAME_ERROR("Failed to rename file/directory"),
        
        /**
         * Error during file attribute operations.
         */
        ATTRIBUTE_ERROR("Failed to get/set file attributes"),
        
        /**
         * Error during file existence check.
         */
        EXISTENCE_ERROR("Failed to check file existence"),
        
        /**
         * Error during working directory operations.
         */
        PWD_ERROR("Failed to change/get working directory"),
        
        /**
         * Error during directory listing.
         */
        DIRECTORY_LISTING_ERROR("Failed to list directories"),
        
        /**
         * Error during directory copying.
         */
        COPY_ERROR("Failed to copy directory structure"),
        
        /**
         * Error during directory size calculation.
         */
        SIZE_CALCULATION_ERROR("Failed to calculate directory size"),
        
        /**
         * Error during directory exists check.
         */
        DIRECTORY_EXISTS_ERROR("Failed to check if directory exists"),
        
        /**
         * Unknown or unspecified error.
         */
        UNKNOWN_ERROR("Unknown FTP operation error");
        
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
    private final String path;
    private final Integer replyCode;
    
    /**
     * Creates a new FTPOperationException with the default error type.
     * 
     * @param message the error message
     */
    public FTPOperationException(String message) {
        super(message);
        this.errorType = ErrorType.UNKNOWN_ERROR;
        this.path = null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPOperationException with the default error type and a cause.
     * 
     * @param message the error message
     * @param cause the cause of the error
     */
    public FTPOperationException(String message, Throwable cause) {
        super(message, cause);
        this.errorType = ErrorType.UNKNOWN_ERROR;
        this.path = null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPOperationException with the specified error type.
     * 
     * @param errorType the type of error
     * @param message the error message
     */
    public FTPOperationException(ErrorType errorType, String message) {
        super(message);
        this.errorType = errorType;
        this.path = null;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPOperationException with the specified error type and path.
     * 
     * @param errorType the type of error
     * @param path the path on which the operation was being performed
     * @param message the error message
     */
    public FTPOperationException(ErrorType errorType, String path, String message) {
        super(message);
        this.errorType = errorType;
        this.path = path;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPOperationException with the specified error type, path, and cause.
     * 
     * @param errorType the type of error
     * @param path the path on which the operation was being performed
     * @param message the error message
     * @param cause the cause of the error
     */
    public FTPOperationException(ErrorType errorType, String path, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.path = path;
        this.replyCode = null;
    }
    
    /**
     * Creates a new FTPOperationException with the specified error type, path, reply code, and message.
     * 
     * @param errorType the type of error
     * @param path the path on which the operation was being performed
     * @param replyCode the FTP server reply code
     * @param message the error message
     */
    public FTPOperationException(ErrorType errorType, String path, int replyCode, String message) {
        super(message);
        this.errorType = errorType;
        this.path = path;
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
     * Gets the path on which the operation was being performed.
     * 
     * @return the path, or null if not available
     */
    public String getPath() {
        return path;
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
        
        if (path != null) {
            sb.append(" on path '").append(path).append("'");
        }
        
        if (replyCode != null) {
            sb.append(" (reply code: ").append(replyCode).append(")");
        }
        
        sb.append(": ").append(getMessage());
        
        return sb.toString();
    }
}