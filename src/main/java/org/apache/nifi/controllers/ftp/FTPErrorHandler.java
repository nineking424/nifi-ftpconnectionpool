package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.controllers.ftp.exception.FTPAuthenticationException;
import org.apache.nifi.controllers.ftp.exception.FTPConnectionException;
import org.apache.nifi.controllers.ftp.exception.FTPErrorType;
import org.apache.nifi.controllers.ftp.exception.FTPFileOperationException;
import org.apache.nifi.controllers.ftp.exception.FTPOperationException;
import org.apache.nifi.controllers.ftp.exception.FTPTransferException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;

/**
 * Utility class for handling FTP errors and exceptions.
 */
public class FTPErrorHandler {
    
    /**
     * Determines if an FTP reply code indicates a successful operation.
     *
     * @param replyCode The FTP reply code
     * @return true if the reply code indicates success, false otherwise
     */
    public static boolean isSuccessfulReplyCode(int replyCode) {
        return FTPReply.isPositiveCompletion(replyCode) || FTPReply.isPositivePreliminary(replyCode);
    }
    
    /**
     * Handles a failed FTP operation by translating the exception and determining recovery actions.
     *
     * @param exception The exception that occurred
     * @param client The FTP client being used
     * @param connection The FTP connection being used
     * @param connectionManager The connection manager
     * @param logger The logger to use
     * @return true if the operation should be retried, false otherwise
     */
    public static boolean handleFTPError(IOException exception, FTPClient client, FTPConnection connection,
                                         FTPConnectionManager connectionManager, ComponentLog logger) {
        
        if (exception == null) {
            return false;
        }
        
        // Log the error
        logger.error("FTP error occurred: {}", new Object[] { exception.getMessage() }, exception);
        
        // Check if the client is still connected
        boolean clientConnected = client != null && client.isConnected();
        
        // Determine if the connection is still usable
        boolean connectionUsable = connection != null && connection.getState().isUsable();
        
        // If exception is already an FTPOperationException, handle it directly
        if (exception instanceof FTPOperationException) {
            FTPOperationException ftpException = (FTPOperationException) exception;
            
            // Log specific error information
            logger.error("FTP operation error: type={}, recoverable={}, message={}",
                    new Object[] { ftpException.getErrorType(), ftpException.isRecoverable(), ftpException.getMessage() });
            
            // If the connection is not usable, mark it as failed
            if (!connectionUsable && connection != null) {
                connection.setState(FTPConnectionState.FAILED);
                connection.setLastErrorMessage(ftpException.getMessage());
            }
            
            // If it's a connection exception and the connection is still available, handle it
            if (ftpException instanceof FTPConnectionException && connection != null) {
                handleConnectionException((FTPConnectionException) ftpException, connection, connectionManager, logger);
            }
            
            // Return whether the operation is recoverable
            return ftpException.isRecoverable();
        }
        
        // For other exceptions, analyze and translate
        if (isConnectionException(exception)) {
            // Handle connection-related exceptions
            FTPErrorType errorType = determineConnectionErrorType(exception);
            
            // Create and log a specialized exception
            FTPConnectionException connectionException = new FTPConnectionException(errorType, connection,
                    "Connection error: " + exception.getMessage(), exception);
            
            logger.error("FTP connection error: type={}, recoverable={}, message={}",
                    new Object[] { errorType, connectionException.isRecoverable(), connectionException.getMessage() });
            
            // Mark the connection as failed
            if (connection != null) {
                connection.setState(FTPConnectionState.FAILED);
                connection.setLastErrorMessage(exception.getMessage());
            }
            
            // If connection is not null, try to handle the connection exception
            if (connection != null) {
                handleConnectionException(connectionException, connection, connectionManager, logger);
            }
            
            // Connection exceptions might be recoverable
            return connectionException.isRecoverable();
        }
        
        // For all other exceptions, log and return false (not recoverable)
        logger.error("Unhandled FTP exception: {}", new Object[] { exception.getMessage() }, exception);
        return false;
    }
    
    /**
     * Handles a connection exception by determining appropriate recovery actions.
     *
     * @param exception The connection exception
     * @param connection The FTP connection
     * @param connectionManager The connection manager
     * @param logger The logger to use
     * @return true if the connection was recovered, false otherwise
     */
    private static boolean handleConnectionException(FTPConnectionException exception, FTPConnection connection,
                                                    FTPConnectionManager connectionManager, ComponentLog logger) {
        
        // If the connection is not usable, mark it as failed
        if (!connection.getState().isUsable()) {
            connection.setState(FTPConnectionState.FAILED);
            connection.setLastErrorMessage(exception.getMessage());
        }
        
        // If the error is recoverable, try to reconnect
        if (exception.isRecoverable()) {
            try {
                logger.debug("Attempting to reconnect failed connection: {}", connection.getId());
                boolean reconnected = connectionManager.reconnect(connection);
                
                if (reconnected) {
                    logger.info("Successfully reconnected connection: {}", connection.getId());
                    return true;
                } else {
                    logger.warn("Failed to reconnect connection: {}", connection.getId());
                }
            } catch (Exception e) {
                logger.error("Error during reconnection attempt: {}", new Object[] { e.getMessage() }, e);
            }
        } else {
            logger.debug("Connection exception is not recoverable, not attempting reconnection");
        }
        
        return false;
    }
    
    /**
     * Checks if an exception is related to FTP connection issues.
     *
     * @param exception The exception to check
     * @return true if the exception is connection-related, false otherwise
     */
    private static boolean isConnectionException(Throwable exception) {
        if (exception == null) {
            return false;
        }
        
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        
        // Check for connection-related exception messages
        return message.contains("connection") ||
               message.contains("timeout") ||
               message.contains("timed out") ||
               message.contains("refused") ||
               message.contains("reset") ||
               message.contains("closed") ||
               message.contains("connect") ||
               message.contains("socket");
    }
    
    /**
     * Determines the specific connection error type based on the exception.
     *
     * @param exception The exception to analyze
     * @return The determined error type
     */
    private static FTPErrorType determineConnectionErrorType(Throwable exception) {
        if (exception == null) {
            return FTPErrorType.CONNECTION_ERROR;
        }
        
        String message = exception.getMessage();
        if (message == null) {
            return FTPErrorType.CONNECTION_ERROR;
        }
        
        // Check for specific error patterns
        if (message.contains("timeout") || message.contains("timed out")) {
            return FTPErrorType.CONNECTION_TIMEOUT;
        } else if (message.contains("refused")) {
            return FTPErrorType.CONNECTION_REFUSED;
        } else if (message.contains("reset") || message.contains("closed")) {
            return FTPErrorType.CONNECTION_CLOSED;
        } else if (message.contains("login") || message.contains("authentication") ||
                  message.contains("password") || message.contains("username")) {
            return FTPErrorType.AUTHENTICATION_ERROR;
        }
        
        // Default case
        return FTPErrorType.CONNECTION_ERROR;
    }
    
    /**
     * Translates an FTP reply code to an appropriate FTP error type.
     *
     * @param replyCode The FTP reply code
     * @return The corresponding error type
     */
    public static FTPErrorType translateReplyCodeToErrorType(int replyCode) {
        // 1xx: Positive Preliminary reply
        if (replyCode >= 100 && replyCode < 200) {
            return FTPErrorType.UNEXPECTED_ERROR; // Should not happen in error cases
        }
        
        // 2xx: Positive Completion reply
        if (replyCode >= 200 && replyCode < 300) {
            return FTPErrorType.UNEXPECTED_ERROR; // Should not happen in error cases
        }
        
        // 3xx: Positive Intermediate reply
        if (replyCode >= 300 && replyCode < 400) {
            return FTPErrorType.UNEXPECTED_ERROR; // Should not happen in error cases
        }
        
        // 4xx: Transient Negative Completion reply
        if (replyCode >= 400 && replyCode < 500) {
            switch (replyCode) {
                case 421: return FTPErrorType.CONNECTION_CLOSED; // Service not available, closing control connection
                case 425: return FTPErrorType.DATA_CONNECTION_ERROR; // Can't open data connection
                case 426: return FTPErrorType.TRANSFER_ABORTED; // Connection closed; transfer aborted
                case 430: return FTPErrorType.INVALID_CREDENTIALS; // Invalid username or password
                case 450: return FTPErrorType.FILE_NOT_FOUND; // Requested file action not taken
                case 451: return FTPErrorType.TRANSFER_ERROR; // Requested action aborted: local error in processing
                case 452: return FTPErrorType.INSUFFICIENT_STORAGE; // Requested action not taken. Insufficient storage space
                default: return FTPErrorType.SERVER_ERROR; // Other transient errors
            }
        }
        
        // 5xx: Permanent Negative Completion reply
        if (replyCode >= 500 && replyCode < 600) {
            switch (replyCode) {
                case 501: return FTPErrorType.INVALID_CONFIGURATION; // Syntax error in parameters
                case 502: return FTPErrorType.COMMAND_NOT_SUPPORTED; // Command not implemented
                case 503: return FTPErrorType.INVALID_SEQUENCE; // Bad sequence of commands
                case 504: return FTPErrorType.COMMAND_NOT_SUPPORTED; // Command not implemented for that parameter
                case 530: return FTPErrorType.AUTHENTICATION_ERROR; // Not logged in
                case 532: return FTPErrorType.AUTHENTICATION_ERROR; // Need account for storing files
                case 550: return FTPErrorType.FILE_NOT_FOUND; // Requested action not taken, file unavailable
                case 551: return FTPErrorType.INVALID_PATH; // Requested action aborted. Page type unknown
                case 552: return FTPErrorType.INSUFFICIENT_STORAGE; // Requested file action aborted. Exceeded storage allocation
                case 553: return FTPErrorType.INVALID_PATH; // Requested action not taken. File name not allowed
                default: return FTPErrorType.SERVER_ERROR; // Other permanent errors
            }
        }
        
        // Unknown reply code
        return FTPErrorType.UNEXPECTED_ERROR;
    }
    
    /**
     * Creates an appropriate FTP exception based on the error type and context.
     *
     * @param errorType The type of error
     * @param replyCode The FTP reply code
     * @param message The error message
     * @param remotePath The remote file path (if applicable)
     * @param cause The cause of the exception
     * @return An appropriate FTP exception
     */
    public static FTPOperationException createException(FTPErrorType errorType, int replyCode,
                                                        String message, String remotePath, Throwable cause) {
        // If the error is authentication-related, create an FTPAuthenticationException
        if (errorType == FTPErrorType.AUTHENTICATION_ERROR || errorType == FTPErrorType.INVALID_CREDENTIALS) {
            return new FTPAuthenticationException(errorType, replyCode, "anonymous", message);
        }
        
        // If the error is file-related, create an FTPFileOperationException
        if (errorType == FTPErrorType.FILE_NOT_FOUND || 
            errorType == FTPErrorType.FILE_ALREADY_EXISTS ||
            errorType == FTPErrorType.DIRECTORY_NOT_FOUND ||
            errorType == FTPErrorType.DIRECTORY_NOT_EMPTY ||
            errorType == FTPErrorType.INVALID_PATH) {
            
            FTPFileOperationException.FileOperation operation = FTPFileOperationException.FileOperation.LIST;
            return new FTPFileOperationException(errorType, replyCode, remotePath, operation, message);
        }
        
        // If the error is transfer-related, create an FTPTransferException
        if (errorType == FTPErrorType.TRANSFER_ERROR ||
            errorType == FTPErrorType.TRANSFER_ABORTED ||
            errorType == FTPErrorType.TRANSFER_TIMEOUT ||
            errorType == FTPErrorType.INSUFFICIENT_STORAGE ||
            errorType == FTPErrorType.DATA_CONNECTION_ERROR ||
            errorType == FTPErrorType.DATA_CONNECTION_TIMEOUT) {
            
            return new FTPTransferException(errorType, replyCode, remotePath, -1, message);
        }
        
        // For connection-related errors, create an FTPConnectionException
        if (errorType == FTPErrorType.CONNECTION_ERROR ||
            errorType == FTPErrorType.CONNECTION_TIMEOUT ||
            errorType == FTPErrorType.CONNECTION_CLOSED ||
            errorType == FTPErrorType.CONNECTION_REFUSED) {
            
            return new FTPConnectionException(errorType, message, cause);
        }
        
        // Default case
        return new FTPOperationException(errorType, replyCode, message, cause);
    }
}