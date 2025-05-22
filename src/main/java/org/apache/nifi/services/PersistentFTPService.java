package org.apache.nifi.services;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.nifi.controller.ControllerService;

import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Interface for a persistent FTP connection service that manages a pool of 
 * connections to an FTP server.
 */
public interface PersistentFTPService extends ControllerService {
    
    /**
     * Gets a connection from the connection pool.
     *
     * @return An FTPClient instance from the connection pool
     * @throws IOException if unable to get a connection
     */
    FTPClient getConnection() throws IOException;
    
    /**
     * Returns a connection to the connection pool.
     *
     * @param client The FTPClient to return to the pool
     */
    void releaseConnection(FTPClient client);
    
    /**
     * Tests if a connection to the FTP server can be established.
     *
     * @return true if a connection can be established, false otherwise
     */
    boolean testConnection();
    
    /**
     * Gets statistics about the connection pool.
     *
     * @return A map of statistics about the connection pool
     */
    Map<String, Object> getConnectionStats();
    
    /**
     * Lists files in a directory on the FTP server.
     *
     * @param directory The directory to list files from
     * @param filter Optional filter to apply to the file listing
     * @return A list of FTPFile objects representing the files in the directory
     * @throws IOException if an error occurs while listing files
     */
    List<FTPFile> listFiles(String directory, FileFilter filter) throws IOException;
    
    /**
     * Gets an InputStream for a file on the FTP server.
     *
     * @param remotePath The path to the file on the FTP server
     * @return An InputStream for the file
     * @throws IOException if an error occurs while retrieving the file
     */
    InputStream retrieveFileStream(String remotePath) throws IOException;
    
    /**
     * Stores a file on the FTP server.
     *
     * @param remotePath The path to store the file at on the FTP server
     * @param input An InputStream containing the file contents
     * @return true if the file was stored successfully, false otherwise
     * @throws IOException if an error occurs while storing the file
     */
    boolean storeFile(String remotePath, InputStream input) throws IOException;
    
    /**
     * Deletes a file from the FTP server.
     *
     * @param remotePath The path to the file to delete
     * @return true if the file was deleted successfully, false otherwise
     * @throws IOException if an error occurs while deleting the file
     */
    boolean deleteFile(String remotePath) throws IOException;
    
    /**
     * Invalidates a connection and removes it from the pool.
     *
     * @param client The FTPClient to invalidate
     */
    void disconnect(FTPClient client);
}