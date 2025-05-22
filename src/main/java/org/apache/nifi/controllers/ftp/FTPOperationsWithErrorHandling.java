package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.nifi.controllers.ftp.exception.FTPErrorHandler;
import org.apache.nifi.controllers.ftp.exception.FTPErrorManager;
import org.apache.nifi.controllers.ftp.exception.FTPErrorType;
import org.apache.nifi.controllers.ftp.exception.FTPFileOperationException;
import org.apache.nifi.controllers.ftp.exception.FTPOperationException;
import org.apache.nifi.controllers.ftp.exception.FTPTransferException;
import org.apache.nifi.logging.ComponentLog;

import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Decorates the FTPOperations class with enhanced error handling features.
 * This class enhances the basic FTPOperations with retry mechanisms, circuit breaking,
 * and error metrics collection.
 */
public class FTPOperationsWithErrorHandling {
    
    private static final String CIRCUIT_BREAKER_CONNECTION = "ftp-connection";
    private static final String CIRCUIT_BREAKER_TRANSFERS = "ftp-transfers";
    private static final String CIRCUIT_BREAKER_FILE_OPS = "ftp-file-operations";
    
    private final FTPOperations ftpOperations;
    private final FTPErrorManager errorManager;
    
    /**
     * Creates a new FTPOperationsWithErrorHandling with default error handling settings.
     *
     * @param ftpOperations The underlying FTP operations to enhance
     * @param logger The logger to use
     */
    public FTPOperationsWithErrorHandling(FTPOperations ftpOperations, ComponentLog logger) {
        this.ftpOperations = ftpOperations;
        this.errorManager = new FTPErrorManager(logger);
    }
    
    /**
     * Creates a new FTPOperationsWithErrorHandling with custom error handling settings.
     *
     * @param ftpOperations The underlying FTP operations to enhance
     * @param logger The logger to use
     * @param maxRetries The maximum number of retries for recoverable errors
     * @param retryDelay The delay between retries in milliseconds
     */
    public FTPOperationsWithErrorHandling(FTPOperations ftpOperations, ComponentLog logger, 
            int maxRetries, long retryDelay) {
        this.ftpOperations = ftpOperations;
        this.errorManager = new FTPErrorManager(logger, maxRetries, retryDelay);
    }
    
    /**
     * Lists files in a directory on the FTP server.
     *
     * @param directory the directory to list
     * @return a list of FTPFile objects representing the files in the directory
     * @throws IOException if an error occurs while listing files
     */
    public List<FTPFile> listFiles(String directory) throws IOException {
        return errorManager.executeOperation(
                () -> ftpOperations.listFiles(directory),
                "listFiles: " + directory,
                CIRCUIT_BREAKER_FILE_OPS);
    }
    
    /**
     * Lists files in a directory on the FTP server with a filter.
     *
     * @param directory the directory to list
     * @param filter a filter to apply to the file listing, or null for no filtering
     * @return a list of FTPFile objects representing the files in the directory
     * @throws IOException if an error occurs while listing files
     */
    public List<FTPFile> listFiles(String directory, FileFilter filter) throws IOException {
        return errorManager.executeOperation(
                () -> ftpOperations.listFiles(directory, filter),
                "listFiles (filtered): " + directory,
                CIRCUIT_BREAKER_FILE_OPS);
    }
    
    /**
     * Lists files in a directory on the FTP server that match a pattern.
     *
     * @param directory the directory to list
     * @param pattern a regex pattern to match file names against
     * @return a list of FTPFile objects representing the files in the directory that match the pattern
     * @throws IOException if an error occurs while listing files
     */
    public List<FTPFile> listFiles(String directory, Pattern pattern) throws IOException {
        return errorManager.executeOperation(
                () -> ftpOperations.listFiles(directory, pattern),
                "listFiles (pattern): " + directory,
                CIRCUIT_BREAKER_FILE_OPS);
    }
    
    /**
     * Lists files in a directory and its subdirectories recursively.
     *
     * @param directory the directory to list recursively
     * @param filter a filter to apply to the file listing, or null for no filtering
     * @param depth the maximum recursion depth (0 for no recursion)
     * @return a list of FTPFile objects representing the files in the directory and its subdirectories
     * @throws IOException if an error occurs while listing files
     */
    public List<FTPFile> listFilesRecursively(String directory, FileFilter filter, int depth) throws IOException {
        return errorManager.executeOperation(
                () -> ftpOperations.listFilesRecursively(directory, filter, depth),
                "listFilesRecursively: " + directory + " (depth: " + depth + ")",
                CIRCUIT_BREAKER_FILE_OPS);
    }
    
    /**
     * Lists only directories in a specified directory on the FTP server.
     * 
     * @param directory the directory to list
     * @return a list of FTPFile objects representing only the directories in the specified directory
     * @throws IOException if an error occurs while listing directories
     */
    public List<FTPFile> listDirectories(String directory) throws IOException {
        return errorManager.executeOperation(
                () -> ftpOperations.listDirectories(directory),
                "listDirectories: " + directory,
                CIRCUIT_BREAKER_FILE_OPS);
    }
    
    /**
     * Lists only directories in a specified directory on the FTP server that match a filter.
     * 
     * @param directory the directory to list
     * @param filter a filter to apply to the directory listing, or null for no filtering
     * @return a list of FTPFile objects representing only the directories in the specified directory
     * @throws IOException if an error occurs while listing directories
     */
    public List<FTPFile> listDirectories(String directory, FileFilter filter) throws IOException {
        return errorManager.executeOperation(
                () -> ftpOperations.listDirectories(directory, filter),
                "listDirectories (filtered): " + directory,
                CIRCUIT_BREAKER_FILE_OPS);
    }
    
    /**
     * Checks if a file exists on the FTP server.
     *
     * @param filePath the path to check
     * @return true if the file exists, false otherwise
     * @throws IOException if an error occurs while checking
     */
    public boolean fileExists(String filePath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.fileExists(filePath),
                    "fileExists: " + filePath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (FTPOperationException e) {
            if (e.getErrorType() == FTPErrorType.FILE_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }
    
    /**
     * Checks if a directory exists on the FTP server.
     *
     * @param directoryPath the path to check
     * @return true if the directory exists, false otherwise
     * @throws IOException if an error occurs while checking
     */
    public boolean directoryExists(String directoryPath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.directoryExists(directoryPath),
                    "directoryExists: " + directoryPath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (FTPOperationException e) {
            if (e.getErrorType() == FTPErrorType.DIRECTORY_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }
    
    /**
     * Gets the size of a file on the FTP server.
     *
     * @param filePath the path to the file
     * @return the size of the file in bytes, or -1 if the file doesn't exist
     * @throws IOException if an error occurs while getting the file size
     */
    public long getFileSize(String filePath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.getFileSize(filePath),
                    "getFileSize: " + filePath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (FTPOperationException e) {
            if (e.getErrorType() == FTPErrorType.FILE_NOT_FOUND) {
                return -1;
            }
            throw e;
        }
    }
    
    /**
     * Gets the modification time of a file or directory on the FTP server.
     *
     * @param path the path to the file or directory
     * @return the modification time as a string, or null if it doesn't exist
     * @throws IOException if an error occurs while getting the modification time
     */
    public String getModificationTime(String path) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.getModificationTime(path),
                    "getModificationTime: " + path,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (FTPOperationException e) {
            if (e.getErrorType() == FTPErrorType.FILE_NOT_FOUND || 
                    e.getErrorType() == FTPErrorType.DIRECTORY_NOT_FOUND) {
                return null;
            }
            throw e;
        }
    }
    
    /**
     * Downloads a file from the FTP server.
     *
     * @param remotePath the path to the file on the FTP server
     * @param outputStream the output stream to write the file to
     * @return true if the file was downloaded successfully, false otherwise
     * @throws IOException if an error occurs during the download
     */
    public boolean retrieveFile(String remotePath, OutputStream outputStream) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.retrieveFile(remotePath, outputStream),
                    "retrieveFile: " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.DOWNLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to download file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Opens a stream to read a file from the FTP server.
     * NOTE: The caller MUST close the returned stream when done to release the connection back to the pool.
     *
     * @param remotePath the path to the file on the FTP server
     * @return an input stream to read the file
     * @throws IOException if an error occurs while opening the stream
     */
    public InputStream retrieveFileStream(String remotePath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.retrieveFileStream(remotePath),
                    "retrieveFileStream: " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.DOWNLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to open stream to file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets a portion of a file from the FTP server.
     *
     * @param remotePath the path to the file on the FTP server
     * @param outputStream the output stream to write the file to
     * @param restartOffset the position in the remote file to start from
     * @return true if the file portion was retrieved successfully, false otherwise
     * @throws IOException if an error occurs during the download
     */
    public boolean retrieveFilePartial(String remotePath, OutputStream outputStream, long restartOffset) 
            throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.retrieveFilePartial(remotePath, outputStream, restartOffset),
                    "retrieveFilePartial: " + remotePath + " (offset: " + restartOffset + ")",
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.DOWNLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to download file portion: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFile(remotePath, inputStream),
                    "storeFile: " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to upload file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server with a specified file type.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param fileType the file type for the transfer
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream, int fileType) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFile(remotePath, inputStream, fileType),
                    "storeFile (with file type): " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to upload file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server with progress tracking.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param progressCallback callback for tracking upload progress
     * @param totalBytes the total number of bytes to be uploaded
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream, 
            FTPOperations.UploadProgressCallback progressCallback, long totalBytes) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFile(remotePath, inputStream, progressCallback, totalBytes),
                    "storeFile (with progress): " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to upload file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server with advanced options.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param fileType the file type for the transfer
     * @param progressCallback callback for tracking upload progress
     * @param totalBytes the total number of bytes to be uploaded
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream, int fileType, 
            FTPOperations.UploadProgressCallback progressCallback, long totalBytes) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFile(remotePath, inputStream, fileType, progressCallback, totalBytes),
                    "storeFile (advanced): " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to upload file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Opens a stream to upload a file to the FTP server.
     * NOTE: The caller MUST close the returned stream when done to complete the upload and release the connection.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @return an output stream to write the file contents to
     * @throws IOException if an error occurs while opening the stream
     */
    public OutputStream storeFileStream(String remotePath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFileStream(remotePath),
                    "storeFileStream: " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to open upload stream: " + e.getMessage(), e);
        }
    }
    
    /**
     * Opens a stream to upload a file to the FTP server with advanced options.
     * NOTE: The caller MUST close the returned stream when done to complete the upload and release the connection.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param fileType the file type for the transfer
     * @param restartOffset the offset in the remote file to start from
     * @param progressCallback callback for tracking upload progress
     * @return an output stream to write the file contents to
     * @throws IOException if an error occurs while opening the stream
     */
    public OutputStream storeFileStream(String remotePath, int fileType, long restartOffset, 
            FTPOperations.UploadProgressCallback progressCallback) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFileStream(remotePath, fileType, restartOffset, progressCallback),
                    "storeFileStream (advanced): " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to open upload stream: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server, resuming from a specific offset if needed.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param restartOffset the offset in the remote file to start from
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFilePartial(String remotePath, InputStream inputStream, long restartOffset) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFilePartial(remotePath, inputStream, restartOffset),
                    "storeFilePartial: " + remotePath + " (offset: " + restartOffset + ")",
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to upload file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server, resuming from a specific offset if needed, with advanced options.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param restartOffset the offset in the remote file to start from
     * @param fileType the file type for the transfer
     * @param progressCallback callback for tracking upload progress
     * @param totalBytes the total number of bytes to be uploaded
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFilePartial(String remotePath, InputStream inputStream, long restartOffset,
            int fileType, FTPOperations.UploadProgressCallback progressCallback, long totalBytes) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.storeFilePartial(remotePath, inputStream, restartOffset, 
                            fileType, progressCallback, totalBytes),
                    "storeFilePartial (advanced): " + remotePath + " (offset: " + restartOffset + ")",
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to upload file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Uploads a file to the FTP server, appending to an existing file if it exists.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean appendFile(String remotePath, InputStream inputStream) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.appendFile(remotePath, inputStream),
                    "appendFile: " + remotePath,
                    CIRCUIT_BREAKER_TRANSFERS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.UPLOAD_ERROR;
            }
            
            throw new FTPTransferException(errorType, remotePath, 
                    "Failed to append to file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates a directory on the FTP server.
     * 
     * @param directoryPath the path of the directory to create
     * @return true if the directory was created successfully, false otherwise
     * @throws IOException if an error occurs creating the directory
     */
    public boolean makeDirectory(String directoryPath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.makeDirectory(directoryPath),
                    "makeDirectory: " + directoryPath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.MKDIR_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, directoryPath, 
                    FTPFileOperationException.FileOperation.MAKE_DIRECTORY,
                    "Failed to create directory: " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates multiple directories on the FTP server in a single batch operation.
     * 
     * @param directoryPaths a list of directory paths to create
     * @param continueOnError whether to continue creating directories if an error occurs
     * @return a map of directory paths to boolean success values
     * @throws IOException if an error occurs creating the directories and continueOnError is false
     */
    public Map<String, Boolean> makeDirectories(List<String> directoryPaths, boolean continueOnError) 
            throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.makeDirectories(directoryPaths, continueOnError),
                    "makeDirectories (batch): " + directoryPaths.size() + " directories",
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.MKDIR_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, null, 
                    FTPFileOperationException.FileOperation.MAKE_DIRECTORY,
                    "Failed to create directories: " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates a directory and all parent directories if they don't exist.
     * 
     * @param directoryPath the path of the directory to create
     * @return true if the directory was created successfully, false otherwise
     * @throws IOException if an error occurs creating the directory
     */
    public boolean makeDirectories(String directoryPath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.makeDirectories(directoryPath),
                    "makeDirectories: " + directoryPath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.MKDIR_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, directoryPath, 
                    FTPFileOperationException.FileOperation.MAKE_DIRECTORY,
                    "Failed to create directory tree: " + e.getMessage(), e);
        }
    }
    
    /**
     * Deletes a file on the FTP server.
     * 
     * @param filePath the path of the file to delete
     * @return true if the file was deleted successfully, false otherwise
     * @throws IOException if an error occurs deleting the file
     */
    public boolean deleteFile(String filePath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.deleteFile(filePath),
                    "deleteFile: " + filePath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.DELETION_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, filePath, 
                    FTPFileOperationException.FileOperation.DELETE,
                    "Failed to delete file: " + e.getMessage(), e);
        }
    }
    
    /**
     * Deletes a directory on the FTP server.
     * 
     * @param directoryPath the path of the directory to delete
     * @param recursive whether to delete the directory recursively
     * @return true if the directory was deleted successfully, false otherwise
     * @throws IOException if an error occurs deleting the directory
     */
    public boolean deleteDirectory(String directoryPath, boolean recursive) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.deleteDirectory(directoryPath, recursive),
                    "deleteDirectory: " + directoryPath + (recursive ? " (recursive)" : ""),
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.DELETION_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, directoryPath, 
                    FTPFileOperationException.FileOperation.REMOVE_DIRECTORY,
                    "Failed to delete directory: " + e.getMessage(), e);
        }
    }
    
    /**
     * Renames a file or directory on the FTP server.
     * 
     * @param fromPath the current path of the file or directory
     * @param toPath the new path for the file or directory
     * @return true if the rename was successful, false otherwise
     * @throws IOException if an error occurs during the rename
     */
    public boolean rename(String fromPath, String toPath) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.rename(fromPath, toPath),
                    "rename: " + fromPath + " to " + toPath,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.RENAME_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, fromPath, 
                    FTPFileOperationException.FileOperation.RENAME,
                    "Failed to rename from " + fromPath + " to " + toPath + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets detailed information about a directory entry on the FTP server.
     * 
     * @param path the path to the file or directory
     * @return an FTPFile object containing details, or null if not found
     * @throws IOException if an error occurs getting the details
     */
    public FTPFile getFileInfo(String path) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.getFileInfo(path),
                    "getFileInfo: " + path,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.ATTRIBUTE_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, path, 
                    FTPFileOperationException.FileOperation.GET_ATTRIBUTES,
                    "Failed to get file info: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets file or directory attributes in a structured format.
     * 
     * @param path the path to the file or directory
     * @return a map of attribute names to values, or null if not found
     * @throws IOException if an error occurs getting the attributes
     */
    public Map<String, Object> getAttributes(String path) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.getAttributes(path),
                    "getAttributes: " + path,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.ATTRIBUTE_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, path, 
                    FTPFileOperationException.FileOperation.GET_ATTRIBUTES,
                    "Failed to get attributes: " + e.getMessage(), e);
        }
    }
    
    /**
     * Sets file permissions on the FTP server using SITE CHMOD command.
     * 
     * @param path the path to the file or directory
     * @param permissions the permission mode in octal format (e.g., "755" for rwxr-xr-x)
     * @return true if permissions were set successfully, false otherwise
     * @throws IOException if an error occurs setting the permissions
     */
    public boolean setPermissions(String path, String permissions) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.setPermissions(path, permissions),
                    "setPermissions: " + path + " (" + permissions + ")",
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.ATTRIBUTE_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, path, 
                    FTPFileOperationException.FileOperation.SET_PERMISSIONS,
                    "Failed to set permissions: " + e.getMessage(), e);
        }
    }
    
    /**
     * Sets the modification time of a file or directory on the FTP server.
     * 
     * @param path the path to the file or directory
     * @param modificationTime the new modification time in format "YYYYMMDDhhmmss"
     * @return true if the modification time was set successfully, false otherwise
     * @throws IOException if an error occurs setting the modification time
     */
    public boolean setModificationTime(String path, String modificationTime) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.setModificationTime(path, modificationTime),
                    "setModificationTime: " + path,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.ATTRIBUTE_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, path, 
                    FTPFileOperationException.FileOperation.SET_MODIFICATION_TIME,
                    "Failed to set modification time: " + e.getMessage(), e);
        }
    }
    
    /**
     * Sets the modification time of a file or directory on the FTP server.
     * 
     * @param path the path to the file or directory
     * @param timestamp the new modification time
     * @return true if the modification time was set successfully, false otherwise
     * @throws IOException if an error occurs setting the modification time
     */
    public boolean setModificationTime(String path, Calendar timestamp) throws IOException {
        try {
            return errorManager.executeOperation(
                    () -> ftpOperations.setModificationTime(path, timestamp),
                    "setModificationTime: " + path,
                    CIRCUIT_BREAKER_FILE_OPS);
        } catch (IOException e) {
            FTPErrorType errorType;
            if (e instanceof FTPOperationException) {
                errorType = ((FTPOperationException) e).getErrorType();
            } else {
                errorType = FTPErrorType.ATTRIBUTE_ERROR;
            }
            
            throw new FTPFileOperationException(errorType, path, 
                    FTPFileOperationException.FileOperation.SET_MODIFICATION_TIME,
                    "Failed to set modification time: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets metrics about FTP errors.
     *
     * @return The error metrics
     */
    public Map<String, Object> getErrorMetrics() {
        return errorManager.getErrorMetrics().getAllMetrics();
    }
    
    /**
     * Resets all error metrics.
     */
    public void resetErrorMetrics() {
        errorManager.resetErrorMetrics();
    }
    
    /**
     * Resets all circuit breakers.
     */
    public void resetCircuitBreakers() {
        errorManager.resetAllCircuitBreakers();
    }
}