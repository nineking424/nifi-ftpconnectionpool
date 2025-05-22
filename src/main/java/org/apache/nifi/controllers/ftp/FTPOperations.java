package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.File;
import java.io.FileFilter;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Provides high-level FTP operations using a connection pool.
 * This class handles the borrowing and returning of connections from the pool,
 * as well as proper error handling and resource cleanup.
 */
public class FTPOperations {
    private final FTPConnectionPool connectionPool;
    private final ComponentLog logger;
    
    /**
     * Creates a new FTPOperations instance with the given connection pool and logger.
     *
     * @param connectionPool the connection pool to use
     * @param logger the logger to use
     */
    public FTPOperations(FTPConnectionPool connectionPool, ComponentLog logger) {
        this.connectionPool = connectionPool;
        this.logger = logger;
    }
    
    /**
     * Lists files in a directory on the FTP server.
     *
     * @param directory the directory to list
     * @return a list of FTPFile objects representing the files in the directory
     * @throws IOException if an error occurs while listing files
     */
    public List<FTPFile> listFiles(String directory) throws IOException {
        return listFiles(directory, null);
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Listing files in directory: {}", new Object[] { directory });
            
            // List files in the specified directory
            FTPFile[] files = client.listFiles(directory);
            
            // Check for errors
            if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
                String errorMsg = "Failed to list files in directory: " + directory + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new IOException(errorMsg);
            }
            
            // Apply filter if provided
            List<FTPFile> result = new ArrayList<>();
            if (files != null) {
                for (FTPFile file : files) {
                    if (file == null) {
                        continue;
                    }
                    
                    // Skip "." and ".." entries
                    String name = file.getName();
                    if (".".equals(name) || "..".equals(name)) {
                        continue;
                    }
                    
                    // Apply filter if provided
                    if (filter == null || filter.accept(new File(file.getName()))) {
                        result.add(file);
                    }
                }
            }
            
            logger.debug("Listed {} files in directory: {}", 
                    new Object[] { result.size(), directory });
            
            return result;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException("Failed to list files in directory: " + directory, e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Lists files in a directory on the FTP server that match a pattern.
     *
     * @param directory the directory to list
     * @param pattern a regex pattern to match file names against
     * @return a list of FTPFile objects representing the files in the directory that match the pattern
     * @throws IOException if an error occurs while listing files
     */
    public List<FTPFile> listFiles(String directory, final Pattern pattern) throws IOException {
        if (pattern == null) {
            return listFiles(directory);
        }
        
        // Create a filter based on the pattern
        FileFilter filter = file -> pattern.matcher(file.getName()).matches();
        
        return listFiles(directory, filter);
    }
    
    /**
     * Lists only directories in a specified directory on the FTP server.
     * 
     * @param directory the directory to list
     * @return a list of FTPFile objects representing only the directories in the specified directory
     * @throws IOException if an error occurs while listing directories
     */
    public List<FTPFile> listDirectories(String directory) throws IOException {
        return listDirectories(directory, null);
    }
    
    /**
     * Lists only directories in a specified directory on the FTP server that match a filter.
     * 
     * @param directory the directory to list
     * @param filter a filter to apply to the directory listing, or null for no filtering
     * @return a list of FTPFile objects representing only the directories in the specified directory
     * @throws IOException if an error occurs while listing directories
     */
    public List<FTPFile> listDirectories(String directory, final FileFilter filter) throws IOException {
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Listing directories in directory: {}", new Object[] { directory });
            
            // List directories in the specified directory
            FTPFile[] files = client.listDirectories(directory);
            
            // Check for errors
            if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
                String errorMsg = "Failed to list directories in directory: " + directory + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new IOException(errorMsg);
            }
            
            // Apply filter if provided
            List<FTPFile> result = new ArrayList<>();
            if (files != null) {
                for (FTPFile file : files) {
                    if (file == null) {
                        continue;
                    }
                    
                    // Skip "." and ".." entries
                    String name = file.getName();
                    if (".".equals(name) || "..".equals(name)) {
                        continue;
                    }
                    
                    // Apply filter if provided
                    if (filter == null || filter.accept(new File(file.getName()))) {
                        result.add(file);
                    }
                }
            }
            
            logger.debug("Listed {} directories in directory: {}", 
                    new Object[] { result.size(), directory });
            
            return result;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException("Failed to list directories in directory: " + directory, e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
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
    public List<FTPFile> listFilesRecursively(String directory, FileFilter filter, int depth) 
            throws IOException {
        if (depth < 0) {
            return new ArrayList<>();
        }
        
        List<FTPFile> result = new ArrayList<>();
        List<FTPFile> directoryContents = listFiles(directory);
        
        for (FTPFile file : directoryContents) {
            String fullPath = directory;
            if (!fullPath.endsWith("/")) {
                fullPath += "/";
            }
            fullPath += file.getName();
            
            // Add the file to the result if it passes the filter
            if (filter == null || filter.accept(new File(file.getName()))) {
                result.add(file);
            }
            
            // Recursively list subdirectories if depth allows
            if (file.isDirectory() && depth > 0) {
                result.addAll(listFilesRecursively(fullPath, filter, depth - 1));
            }
        }
        
        return result;
    }
    
    /**
     * Checks if a file exists on the FTP server.
     *
     * @param filePath the path to check
     * @return true if the file exists, false otherwise
     * @throws IOException if an error occurs while checking
     */
    public boolean fileExists(String filePath) throws IOException {
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Checking if file exists: {}", new Object[] { filePath });
            
            // Get the parent directory and file name
            String parentDir = new File(filePath).getParent();
            if (parentDir == null) {
                parentDir = "/";
            } else if (!parentDir.startsWith("/")) {
                parentDir = "/" + parentDir;
            }
            
            String fileName = new File(filePath).getName();
            
            // List files in the parent directory
            FTPFile[] files = client.listFiles(parentDir);
            
            // Check if the file exists
            if (files != null) {
                for (FTPFile file : files) {
                    if (file != null && fileName.equals(file.getName())) {
                        logger.debug("File exists: {}", new Object[] { filePath });
                        return true;
                    }
                }
            }
            
            logger.debug("File does not exist: {}", new Object[] { filePath });
            return false;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException("Failed to check if file exists: " + filePath, e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Checking if directory exists: {}", new Object[] { directoryPath });
            
            // Normalize the path
            if (!directoryPath.startsWith("/")) {
                directoryPath = "/" + directoryPath;
            }
            
            // Remove trailing slash if present
            if (directoryPath.endsWith("/") && !directoryPath.equals("/")) {
                directoryPath = directoryPath.substring(0, directoryPath.length() - 1);
            }
            
            // Try to change to the directory
            boolean exists = client.changeWorkingDirectory(directoryPath);
            
            // If we successfully changed to the directory, change back to root
            if (exists) {
                client.changeWorkingDirectory("/");
            }
            
            logger.debug("Directory exists: {} - {}", new Object[] { exists, directoryPath });
            return exists;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException("Failed to check if directory exists: " + directoryPath, e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Getting size of file: {}", new Object[] { filePath });
            
            // Get the file size
            long size = client.getSize(filePath);
            
            logger.debug("File size: {} bytes for file: {}", new Object[] { size, filePath });
            return size;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException("Failed to get file size: " + filePath, e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Getting modification time of: {}", new Object[] { path });
            
            // Get the modification time
            String modTime = client.getModificationTime(path);
            
            logger.debug("Modification time: {} for: {}", new Object[] { modTime, path });
            return modTime;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.ATTRIBUTE_ERROR,
                    path,
                    "Failed to get modification time: " + e.getMessage(),
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Getting file info for: {}", new Object[] { path });
            
            // Normalize path
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            
            // Get the parent directory and file name
            String parentDir = new File(path).getParent();
            if (parentDir == null) {
                parentDir = "/";
            } else if (!parentDir.startsWith("/")) {
                parentDir = "/" + parentDir;
            }
            
            String fileName = new File(path).getName();
            
            // List the parent directory to find the file
            FTPFile[] files = client.listFiles(parentDir);
            
            FTPFile result = null;
            if (files != null) {
                for (FTPFile file : files) {
                    if (file != null && fileName.equals(file.getName())) {
                        result = file;
                        break;
                    }
                }
            }
            
            if (result == null) {
                logger.debug("File or directory not found: {}", new Object[] { path });
            } else {
                logger.debug("Found file info for: {}", new Object[] { path });
            }
            
            return result;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.ATTRIBUTE_ERROR,
                    path,
                    "Failed to get file info: " + e.getMessage(),
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        FTPFile fileInfo = getFileInfo(path);
        
        if (fileInfo == null) {
            return null;
        }
        
        Map<String, Object> attributes = new HashMap<>();
        
        // Basic attributes
        attributes.put("name", fileInfo.getName());
        attributes.put("size", fileInfo.getSize());
        attributes.put("type", fileInfo.isDirectory() ? "directory" : "file");
        attributes.put("timestamp", fileInfo.getTimestamp());
        attributes.put("link", fileInfo.getLink());
        attributes.put("hardLinkCount", fileInfo.getHardLinkCount());
        attributes.put("group", fileInfo.getGroup());
        attributes.put("user", fileInfo.getUser());
        
        // Permissions
        int permissions = fileInfo.getPermissions();
        Map<String, Boolean> permMap = new HashMap<>();
        permMap.put("userRead", (permissions & FTPFile.USER_READ) != 0);
        permMap.put("userWrite", (permissions & FTPFile.USER_WRITE) != 0);
        permMap.put("userExecute", (permissions & FTPFile.USER_EXECUTE) != 0);
        permMap.put("groupRead", (permissions & FTPFile.GROUP_READ) != 0);
        permMap.put("groupWrite", (permissions & FTPFile.GROUP_WRITE) != 0);
        permMap.put("groupExecute", (permissions & FTPFile.GROUP_EXECUTE) != 0);
        permMap.put("worldRead", (permissions & FTPFile.WORLD_READ) != 0);
        permMap.put("worldWrite", (permissions & FTPFile.WORLD_WRITE) != 0);
        permMap.put("worldExecute", (permissions & FTPFile.WORLD_EXECUTE) != 0);
        attributes.put("permissions", permMap);
        
        // Raw permissions string like "-rw-r--r--"
        attributes.put("permissionsString", fileInfo.getRawListing());
        
        return attributes;
    }
    
    /**
     * Sets file permissions on the FTP server using SITE CHMOD command.
     * Note that not all FTP servers support this command.
     * 
     * @param path the path to the file or directory
     * @param permissions the permission mode in octal format (e.g., "755" for rwxr-xr-x)
     * @return true if permissions were set successfully, false otherwise
     * @throws IOException if an error occurs setting the permissions
     */
    public boolean setPermissions(String path, String permissions) throws IOException {
        if (path == null || permissions == null) {
            throw new IllegalArgumentException("Path and permissions cannot be null");
        }
        
        // Verify permissions format (must be valid octal)
        if (!permissions.matches("^[0-7]{3,4}$")) {
            throw new IllegalArgumentException("Permissions must be in octal format (e.g., '755')");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Setting permissions {} for path: {}", new Object[] { permissions, path });
            
            // Use SITE CHMOD command
            boolean success = client.sendSiteCommand("CHMOD " + permissions + " " + path);
            
            if (!success) {
                String errorMsg = "Failed to set permissions for path: " + path + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                
                // Check if the server doesn't support SITE CHMOD
                if (client.getReplyCode() == 500 || client.getReplyCode() == 502) {
                    throw new FTPOperationException(
                            FTPOperationException.ErrorType.ATTRIBUTE_ERROR, 
                            path, 
                            client.getReplyCode(), 
                            "Server does not support SITE CHMOD command: " + client.getReplyString());
                }
                
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.ATTRIBUTE_ERROR, 
                        path, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Permissions set successfully for path: {}", new Object[] { path });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.ATTRIBUTE_ERROR, 
                    path, 
                    "Failed to set permissions: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Sets file or directory permissions directly using FTP permission flags.
     * This is an alternative to setPermissions() when working with individual permission bits
     * rather than octal notation.
     * 
     * @param path the path to the file or directory
     * @param userRead whether the user can read the file
     * @param userWrite whether the user can write to the file
     * @param userExecute whether the user can execute the file
     * @param groupRead whether the group can read the file
     * @param groupWrite whether the group can write to the file
     * @param groupExecute whether the group can execute the file
     * @param worldRead whether everyone can read the file
     * @param worldWrite whether everyone can write to the file
     * @param worldExecute whether everyone can execute the file
     * @return true if permissions were set successfully, false otherwise
     * @throws IOException if an error occurs setting the permissions
     */
    public boolean setPermissions(String path, 
            boolean userRead, boolean userWrite, boolean userExecute,
            boolean groupRead, boolean groupWrite, boolean groupExecute,
            boolean worldRead, boolean worldWrite, boolean worldExecute) throws IOException {
        
        // Construct the octal permission value
        int permissions = 0;
        if (userRead) permissions |= FTPFile.USER_READ;
        if (userWrite) permissions |= FTPFile.USER_WRITE;
        if (userExecute) permissions |= FTPFile.USER_EXECUTE;
        if (groupRead) permissions |= FTPFile.GROUP_READ;
        if (groupWrite) permissions |= FTPFile.GROUP_WRITE;
        if (groupExecute) permissions |= FTPFile.GROUP_EXECUTE;
        if (worldRead) permissions |= FTPFile.WORLD_READ;
        if (worldWrite) permissions |= FTPFile.WORLD_WRITE;
        if (worldExecute) permissions |= FTPFile.WORLD_EXECUTE;
        
        // Convert to octal string (e.g., 0755)
        String octalPermissions = Integer.toOctalString(permissions);
        // Ensure it's at least 3 digits (pad with leading zeros if needed)
        while (octalPermissions.length() < 3) {
            octalPermissions = "0" + octalPermissions;
        }
        
        return setPermissions(path, octalPermissions);
    }
    
    /**
     * Sets the modification time of a file or directory on the FTP server.
     * Note that not all FTP servers support this command.
     * 
     * @param path the path to the file or directory
     * @param modificationTime the new modification time in format "YYYYMMDDhhmmss"
     * @return true if the modification time was set successfully, false otherwise
     * @throws IOException if an error occurs setting the modification time
     */
    public boolean setModificationTime(String path, String modificationTime) throws IOException {
        if (path == null || modificationTime == null) {
            throw new IllegalArgumentException("Path and modification time cannot be null");
        }
        
        // Verify the modification time format
        if (!modificationTime.matches("^\\d{14}$")) {
            throw new IllegalArgumentException(
                    "Modification time must be in format 'YYYYMMDDhhmmss', e.g., '20240520123045'");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Setting modification time {} for path: {}", 
                    new Object[] { modificationTime, path });
            
            // Use the MDTM command
            boolean success = client.setModificationTime(path, modificationTime);
            
            if (!success) {
                String errorMsg = "Failed to set modification time for path: " + path + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                
                // Check if the server doesn't support the MDTM command
                if (client.getReplyCode() == 500 || client.getReplyCode() == 502) {
                    throw new FTPOperationException(
                            FTPOperationException.ErrorType.ATTRIBUTE_ERROR, 
                            path, 
                            client.getReplyCode(), 
                            "Server does not support MDTM command: " + client.getReplyString());
                }
                
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.ATTRIBUTE_ERROR, 
                        path, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Modification time set successfully for path: {}", new Object[] { path });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.ATTRIBUTE_ERROR, 
                    path, 
                    "Failed to set modification time: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        if (timestamp == null) {
            throw new IllegalArgumentException("Timestamp cannot be null");
        }
        
        // Format the timestamp in the required format: "YYYYMMDDhhmmss"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String formattedTime = sdf.format(timestamp.getTime());
        
        return setModificationTime(path, formattedTime);
    }
    
    /**
     * Sets multiple attributes for a file or directory in one operation.
     * Note that not all attributes may be supported by all FTP servers.
     * 
     * @param path the path to the file or directory
     * @param attributes the attributes to set
     * @return a map of attribute names to boolean success values
     * @throws IOException if an error occurs setting the attributes
     */
    public Map<String, Boolean> setAttributes(String path, Map<String, Object> attributes) 
            throws IOException {
        if (path == null || attributes == null) {
            throw new IllegalArgumentException("Path and attributes cannot be null");
        }
        
        Map<String, Boolean> results = new HashMap<>();
        
        // Handle permissions
        if (attributes.containsKey("permissions")) {
            try {
                Object perms = attributes.get("permissions");
                if (perms instanceof String) {
                    boolean success = setPermissions(path, (String) perms);
                    results.put("permissions", success);
                } else if (perms instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Boolean> permMap = (Map<String, Boolean>) perms;
                    
                    boolean success = setPermissions(path,
                            getOrDefault(permMap, "userRead", false),
                            getOrDefault(permMap, "userWrite", false),
                            getOrDefault(permMap, "userExecute", false),
                            getOrDefault(permMap, "groupRead", false),
                            getOrDefault(permMap, "groupWrite", false),
                            getOrDefault(permMap, "groupExecute", false),
                            getOrDefault(permMap, "worldRead", false),
                            getOrDefault(permMap, "worldWrite", false),
                            getOrDefault(permMap, "worldExecute", false));
                    
                    results.put("permissions", success);
                } else {
                    results.put("permissions", false);
                    logger.warn("Invalid permissions format: {}", new Object[] { perms });
                }
            } catch (IOException e) {
                results.put("permissions", false);
                logger.warn("Failed to set permissions: {}", new Object[] { e.getMessage() });
            }
        }
        
        // Handle modification time
        if (attributes.containsKey("timestamp")) {
            try {
                Object timestamp = attributes.get("timestamp");
                if (timestamp instanceof Calendar) {
                    boolean success = setModificationTime(path, (Calendar) timestamp);
                    results.put("timestamp", success);
                } else if (timestamp instanceof String) {
                    boolean success = setModificationTime(path, (String) timestamp);
                    results.put("timestamp", success);
                } else {
                    results.put("timestamp", false);
                    logger.warn("Invalid timestamp format: {}", new Object[] { timestamp });
                }
            } catch (IOException e) {
                results.put("timestamp", false);
                logger.warn("Failed to set timestamp: {}", new Object[] { e.getMessage() });
            }
        }
        
        return results;
    }
    
    /**
     * Helper method to safely get a boolean value from a map with a default.
     */
    private boolean getOrDefault(Map<String, Boolean> map, String key, boolean defaultValue) {
        Boolean value = map.get(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Checks if the FTP server supports a specific feature or capability.
     * This is useful for determining if attribute operations are supported.
     * 
     * @param featureName the name of the feature to check (e.g., "MDTM", "CHMOD", "SITE")
     * @return true if the feature is supported, false otherwise
     * @throws IOException if an error occurs checking for the feature
     */
    public boolean supportsFeature(String featureName) throws IOException {
        if (featureName == null || featureName.isEmpty()) {
            throw new IllegalArgumentException("Feature name cannot be null or empty");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Checking if server supports feature: {}", new Object[] { featureName });
            
            // Get the list of supported features
            boolean supported = client.hasFeature(featureName);
            
            logger.debug("Server {} feature: {}", 
                    new Object[] { supported ? "supports" : "does not support", featureName });
            
            return supported;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.UNKNOWN_ERROR, 
                    null, 
                    "Failed to check feature support: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Retrieves a list of features supported by the FTP server.
     * 
     * @return a list of supported features
     * @throws IOException if an error occurs retrieving the feature list
     */
    public List<String> getSupportedFeatures() throws IOException {
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Getting list of supported features from server");
            
            // This will force a FEAT request to the server if not already done
            String[] features = client.listHelp();
            
            List<String> result = new ArrayList<>();
            if (features != null) {
                result.addAll(Arrays.asList(features));
            }
            
            logger.debug("Server supports {} features", new Object[] { result.size() });
            
            return result;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.UNKNOWN_ERROR, 
                    null, 
                    "Failed to get supported features: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        if (outputStream == null) {
            throw new IllegalArgumentException("Output stream cannot be null");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Downloading file: {}", new Object[] { remotePath });
            
            // Set binary transfer mode
            client.setFileType(FTP.BINARY_FILE_TYPE);
            
            // Download the file
            boolean success = client.retrieveFile(remotePath, outputStream);
            
            if (!success) {
                String errorMsg = "Failed to download file: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("File downloaded successfully: {}", new Object[] { remotePath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                    remotePath, 
                    "Failed to download file: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        final FTPClient client;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Opening stream to file: {}", new Object[] { remotePath });
            
            // Set binary transfer mode
            client.setFileType(FTP.BINARY_FILE_TYPE);
            
            // Get a stream to the file
            final InputStream ftpStream = client.retrieveFileStream(remotePath);
            
            if (ftpStream == null) {
                String errorMsg = "Failed to open stream to file: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                connectionPool.returnConnection(client);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Opened stream to file: {}", new Object[] { remotePath });
            
            // Wrap the stream to handle connection cleanup when closed
            final AtomicBoolean closed = new AtomicBoolean(false);
            return new FilterInputStream(ftpStream) {
                @Override
                public void close() throws IOException {
                    if (closed.compareAndSet(false, true)) {
                        try {
                            // Close the underlying stream
                            super.close();
                            
                            // Complete the command to avoid leaving the connection in a bad state
                            boolean completed = client.completePendingCommand();
                            
                            if (!completed) {
                                String errorMsg = "Failed to complete file transfer for: " + remotePath + 
                                        ", Reply: " + client.getReplyString();
                                logger.error(errorMsg);
                                connectionPool.invalidateConnection(client);
                                throw new FTPOperationException(
                                        FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                                        remotePath, 
                                        client.getReplyCode(), 
                                        errorMsg);
                            }
                            
                            // Return the connection to the pool
                            connectionPool.returnConnection(client);
                            
                        } catch (IOException e) {
                            // Invalidate the connection if there was an error
                            connectionPool.invalidateConnection(client);
                            throw e;
                        }
                    }
                }
                
                @Override
                protected void finalize() throws Throwable {
                    // Safety net to ensure the connection is returned to the pool
                    // if the user forgets to close the stream
                    if (!closed.get()) {
                        logger.warn("Stream was not closed properly, closing in finalize(): {}", remotePath);
                        try {
                            close();
                        } catch (IOException e) {
                            // Ignore, we're in finalize
                        }
                    }
                    super.finalize();
                }
            };
            
        } catch (IOException e) {
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                    remotePath, 
                    "Failed to open stream to file: " + e.getMessage(), 
                    e);
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
        if (outputStream == null) {
            throw new IllegalArgumentException("Output stream cannot be null");
        }
        
        if (restartOffset < 0) {
            throw new IllegalArgumentException("Restart offset cannot be negative");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Downloading file portion from offset {}: {}", 
                    new Object[] { restartOffset, remotePath });
            
            // Set binary transfer mode
            client.setFileType(FTP.BINARY_FILE_TYPE);
            
            // Set the restart offset
            client.setRestartOffset(restartOffset);
            
            // Download the file
            boolean success = client.retrieveFile(remotePath, outputStream);
            
            if (!success) {
                String errorMsg = "Failed to download file portion: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("File portion downloaded successfully from offset {}: {}", 
                    new Object[] { restartOffset, remotePath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.DOWNLOAD_ERROR, 
                    remotePath, 
                    "Failed to download file portion: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        return storeFile(remotePath, inputStream, FTP.BINARY_FILE_TYPE, null, 0);
    }
    
    /**
     * Uploads a file to the FTP server with a specified file type.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param fileType the file type for the transfer (FTP.BINARY_FILE_TYPE or FTP.ASCII_FILE_TYPE)
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream, int fileType) throws IOException {
        return storeFile(remotePath, inputStream, fileType, null, 0);
    }
    
    /**
     * Interface for tracking upload progress.
     */
    public interface UploadProgressCallback {
        /**
         * Called periodically during upload to report progress.
         * 
         * @param bytesTransferred the number of bytes transferred so far
         * @param totalBytes the total number of bytes to transfer, or -1 if unknown
         */
        void progress(long bytesTransferred, long totalBytes);
    }
    
    /**
     * Uploads a file to the FTP server, resuming from a specific offset if needed.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param restartOffset the offset in the remote file to start from (0 for a new upload)
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFilePartial(String remotePath, InputStream inputStream, long restartOffset) 
            throws IOException {
        return storeFilePartial(remotePath, inputStream, restartOffset, FTP.BINARY_FILE_TYPE, null, -1);
    }
    
    /**
     * Uploads a file to the FTP server, resuming from a specific offset if needed, with progress tracking.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param restartOffset the offset in the remote file to start from (0 for a new upload)
     * @param fileType the file type for the transfer (FTP.BINARY_FILE_TYPE or FTP.ASCII_FILE_TYPE)
     * @param progressCallback callback for tracking upload progress, or null for no tracking
     * @param totalBytes the total number of bytes to be uploaded (including the offset), or -1 if unknown
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFilePartial(String remotePath, InputStream inputStream, long restartOffset,
            int fileType, UploadProgressCallback progressCallback, long totalBytes) 
            throws IOException {
        if (inputStream == null) {
            throw new IllegalArgumentException("Input stream cannot be null");
        }
        
        if (restartOffset < 0) {
            throw new IllegalArgumentException("Restart offset cannot be negative");
        }
        
        if (fileType != FTP.BINARY_FILE_TYPE && fileType != FTP.ASCII_FILE_TYPE) {
            throw new IllegalArgumentException("File type must be either BINARY_FILE_TYPE or ASCII_FILE_TYPE");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Uploading file from offset {}: {} in {} mode", 
                    new Object[] { restartOffset, remotePath, fileType == FTP.BINARY_FILE_TYPE ? "binary" : "ASCII" });
            
            // Set transfer mode
            client.setFileType(fileType);
            
            // Ensure the parent directory exists
            ensureParentDirectoryExists(client, remotePath);
            
            // Check if resuming an existing upload
            if (restartOffset > 0) {
                // Check if the file exists and is large enough
                long fileSize = getFileSize(remotePath);
                if (fileSize < 0) {
                    logger.warn("Cannot resume upload - file does not exist: {}", new Object[] { remotePath });
                    restartOffset = 0; // Start a new upload
                } else if (fileSize < restartOffset) {
                    logger.warn("Cannot resume upload at offset {} - file size is only {}: {}", 
                            new Object[] { restartOffset, fileSize, remotePath });
                    restartOffset = 0; // Start a new upload
                }
            }
            
            // Set the restart offset if needed
            if (restartOffset > 0) {
                client.setRestartOffset(restartOffset);
                logger.debug("Resuming upload at offset {}: {}", new Object[] { restartOffset, remotePath });
                
                // If we have a progress callback, notify it of the initial offset
                if (progressCallback != null) {
                    progressCallback.progress(restartOffset, totalBytes);
                }
            }
            
            // Wrap input stream with progress tracking if requested
            InputStream streamToUse = inputStream;
            if (progressCallback != null) {
                // Adjust progress tracking to account for the restart offset
                streamToUse = new ProgressTrackingInputStream(inputStream, new UploadProgressCallback() {
                    @Override
                    public void progress(long bytesTransferred, long totalBytes) {
                        // Add the restart offset to the bytes transferred
                        progressCallback.progress(bytesTransferred + restartOffset, totalBytes);
                    }
                }, totalBytes > 0 ? totalBytes - restartOffset : -1);
            }
            
            // Upload the file
            boolean success = client.storeFile(remotePath, streamToUse);
            
            if (!success) {
                String errorMsg = "Failed to upload file: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.UPLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("File uploaded successfully from offset {}: {}", 
                    new Object[] { restartOffset, remotePath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.UPLOAD_ERROR, 
                    remotePath, 
                    "Failed to upload file from offset " + restartOffset + ": " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Uploads a file to the FTP server with progress tracking.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param progressCallback callback for tracking upload progress, or null for no tracking
     * @param totalBytes the total number of bytes to be uploaded, or -1 if unknown
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream, UploadProgressCallback progressCallback, long totalBytes) throws IOException {
        return storeFile(remotePath, inputStream, FTP.BINARY_FILE_TYPE, progressCallback, totalBytes);
    }
    
    /**
     * Uploads a file to the FTP server with advanced options.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param fileType the file type for the transfer (FTP.BINARY_FILE_TYPE or FTP.ASCII_FILE_TYPE)
     * @param progressCallback callback for tracking upload progress, or null for no tracking
     * @param totalBytes the total number of bytes to be uploaded, or -1 if unknown
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    /**
     * Input stream wrapper that tracks read progress and reports it via a callback.
     */
    private static class ProgressTrackingInputStream extends FilterInputStream {
        private final UploadProgressCallback callback;
        private final long totalBytes;
        private long bytesRead = 0;
        private long lastReportTime = 0;
        private static final long REPORT_INTERVAL_MS = 500; // Report every 500ms

        public ProgressTrackingInputStream(InputStream in, UploadProgressCallback callback, long totalBytes) {
            super(in);
            this.callback = callback;
            this.totalBytes = totalBytes;
            this.lastReportTime = System.currentTimeMillis();
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) {
                updateProgress(1);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int bytesRead = super.read(b, off, len);
            if (bytesRead > 0) {
                updateProgress(bytesRead);
            }
            return bytesRead;
        }

        private void updateProgress(int bytesReadNow) {
            bytesRead += bytesReadNow;
            
            long now = System.currentTimeMillis();
            if (now - lastReportTime >= REPORT_INTERVAL_MS) {
                callback.progress(bytesRead, totalBytes);
                lastReportTime = now;
            }
        }

        @Override
        public void close() throws IOException {
            // Final progress report with the total bytes read
            callback.progress(bytesRead, totalBytes);
            super.close();
        }
    }
    
    /**
     * Uploads a file to the FTP server with advanced options.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param inputStream the input stream containing the file contents
     * @param fileType the file type for the transfer (FTP.BINARY_FILE_TYPE or FTP.ASCII_FILE_TYPE)
     * @param progressCallback callback for tracking upload progress, or null for no tracking
     * @param totalBytes the total number of bytes to be uploaded, or -1 if unknown
     * @return true if the file was uploaded successfully, false otherwise
     * @throws IOException if an error occurs during the upload
     */
    public boolean storeFile(String remotePath, InputStream inputStream, int fileType, 
            UploadProgressCallback progressCallback, long totalBytes) throws IOException {
        if (inputStream == null) {
            throw new IllegalArgumentException("Input stream cannot be null");
        }
        
        if (fileType != FTP.BINARY_FILE_TYPE && fileType != FTP.ASCII_FILE_TYPE) {
            throw new IllegalArgumentException("File type must be either BINARY_FILE_TYPE or ASCII_FILE_TYPE");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Uploading file: {} in {} mode", 
                    new Object[] { remotePath, fileType == FTP.BINARY_FILE_TYPE ? "binary" : "ASCII" });
            
            // Set transfer mode
            client.setFileType(fileType);
            
            // Ensure the parent directory exists
            ensureParentDirectoryExists(client, remotePath);
            
            // Wrap input stream with progress tracking if requested
            InputStream streamToUse = inputStream;
            if (progressCallback != null) {
                streamToUse = new ProgressTrackingInputStream(inputStream, progressCallback, totalBytes);
            }
            
            // Upload the file
            boolean success = client.storeFile(remotePath, streamToUse);
            
            if (!success) {
                String errorMsg = "Failed to upload file: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.UPLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("File uploaded successfully: {}", new Object[] { remotePath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.UPLOAD_ERROR, 
                    remotePath, 
                    "Failed to upload file: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Opens a stream to upload a file to the FTP server.
     * NOTE: The caller MUST close the returned stream when done to complete the upload and release the connection back to the pool.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @return an output stream to write the file contents to
     * @throws IOException if an error occurs while opening the stream
     */
    public OutputStream storeFileStream(String remotePath) throws IOException {
        return storeFileStream(remotePath, FTP.BINARY_FILE_TYPE, 0, null);
    }
    
    /**
     * Opens a stream to upload a file to the FTP server with a specified file type.
     * NOTE: The caller MUST close the returned stream when done to complete the upload and release the connection back to the pool.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param fileType the file type for the transfer (FTP.BINARY_FILE_TYPE or FTP.ASCII_FILE_TYPE)
     * @return an output stream to write the file contents to
     * @throws IOException if an error occurs while opening the stream
     */
    public OutputStream storeFileStream(String remotePath, int fileType) throws IOException {
        return storeFileStream(remotePath, fileType, 0, null);
    }
    
    /**
     * Wrapper class for tracking progress of uploaded bytes through an OutputStream.
     */
    private static class ProgressTrackingOutputStream extends FilterOutputStream {
        private final UploadProgressCallback callback;
        private final long totalBytes;
        private final long startOffset;
        private long bytesWritten = 0;
        private long lastReportTime = 0;
        private static final long REPORT_INTERVAL_MS = 500; // Report every 500ms
        
        public ProgressTrackingOutputStream(OutputStream out, UploadProgressCallback callback, 
                long totalBytes, long startOffset) {
            super(out);
            this.callback = callback;
            this.totalBytes = totalBytes;
            this.startOffset = startOffset;
            this.lastReportTime = System.currentTimeMillis();
            
            // Report initial progress if resuming
            if (startOffset > 0) {
                callback.progress(startOffset, totalBytes);
            }
        }
        
        @Override
        public void write(int b) throws IOException {
            super.write(b);
            updateProgress(1);
        }
        
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            super.write(b, off, len);
            updateProgress(len);
        }
        
        @Override
        public void write(byte[] b) throws IOException {
            super.write(b);
            updateProgress(b.length);
        }
        
        private void updateProgress(int bytesWrittenNow) {
            bytesWritten += bytesWrittenNow;
            
            long now = System.currentTimeMillis();
            if (now - lastReportTime >= REPORT_INTERVAL_MS) {
                callback.progress(bytesWritten + startOffset, totalBytes);
                lastReportTime = now;
            }
        }
        
        @Override
        public void close() throws IOException {
            // Final progress report
            callback.progress(bytesWritten + startOffset, totalBytes);
            super.close();
        }
    }
    
    /**
     * Opens a stream to upload a file to the FTP server with advanced options.
     * NOTE: The caller MUST close the returned stream when done to complete the upload and release the connection back to the pool.
     *
     * @param remotePath the path to store the file at on the FTP server
     * @param fileType the file type for the transfer (FTP.BINARY_FILE_TYPE or FTP.ASCII_FILE_TYPE)
     * @param restartOffset the offset in the remote file to start from (0 for a new upload)
     * @param progressCallback callback for tracking upload progress, or null for no tracking
     * @return an output stream to write the file contents to
     * @throws IOException if an error occurs while opening the stream
     */
    public OutputStream storeFileStream(String remotePath, int fileType, long restartOffset, 
            UploadProgressCallback progressCallback) throws IOException {
        
        if (fileType != FTP.BINARY_FILE_TYPE && fileType != FTP.ASCII_FILE_TYPE) {
            throw new IllegalArgumentException("File type must be either BINARY_FILE_TYPE or ASCII_FILE_TYPE");
        }
        
        if (restartOffset < 0) {
            throw new IllegalArgumentException("Restart offset cannot be negative");
        }
        
        final FTPClient client;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Opening upload stream to file at offset {}: {} in {} mode", 
                    new Object[] { restartOffset, remotePath, fileType == FTP.BINARY_FILE_TYPE ? "binary" : "ASCII" });
            
            // Set transfer mode
            client.setFileType(fileType);
            
            // Ensure the parent directory exists
            ensureParentDirectoryExists(client, remotePath);
            
            // Check if resuming an existing upload
            if (restartOffset > 0) {
                // Check if the file exists and is large enough
                long fileSize = getFileSize(remotePath);
                if (fileSize < 0) {
                    logger.warn("Cannot resume upload - file does not exist: {}", new Object[] { remotePath });
                    restartOffset = 0; // Start a new upload
                } else if (fileSize < restartOffset) {
                    logger.warn("Cannot resume upload at offset {} - file size is only {}: {}", 
                            new Object[] { restartOffset, fileSize, remotePath });
                    restartOffset = 0; // Start a new upload
                }
            }
            
            // Set the restart offset if needed
            final long actualRestartOffset = restartOffset;
            if (actualRestartOffset > 0) {
                client.setRestartOffset(actualRestartOffset);
                logger.debug("Resuming upload at offset {}: {}", 
                        new Object[] { actualRestartOffset, remotePath });
            }
            
            // Get a stream to write to the file
            final OutputStream ftpStream = client.storeFileStream(remotePath);
            
            if (ftpStream == null) {
                String errorMsg = "Failed to open upload stream to file: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                connectionPool.returnConnection(client);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.UPLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Opened upload stream to file at offset {}: {}", 
                    new Object[] { actualRestartOffset, remotePath });
            
            // Wrap the stream to handle connection cleanup when closed
            final AtomicBoolean closed = new AtomicBoolean(false);
            
            // Base wrapped stream for FTP connection management
            OutputStream wrappedStream = new FilterOutputStream(ftpStream) {
                @Override
                public void close() throws IOException {
                    if (closed.compareAndSet(false, true)) {
                        try {
                            // Close the underlying stream
                            super.close();
                            
                            // Complete the command to avoid leaving the connection in a bad state
                            boolean completed = client.completePendingCommand();
                            
                            if (!completed) {
                                String errorMsg = "Failed to complete file upload for: " + remotePath + 
                                        ", Reply: " + client.getReplyString();
                                logger.error(errorMsg);
                                connectionPool.invalidateConnection(client);
                                throw new FTPOperationException(
                                        FTPOperationException.ErrorType.UPLOAD_ERROR, 
                                        remotePath, 
                                        client.getReplyCode(), 
                                        errorMsg);
                            }
                            
                            // Return the connection to the pool
                            connectionPool.returnConnection(client);
                            
                        } catch (IOException e) {
                            // Invalidate the connection if there was an error
                            connectionPool.invalidateConnection(client);
                            throw e;
                        }
                    }
                }
                
                @Override
                public void write(byte[] b) throws IOException {
                    out.write(b);
                }
                
                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                }
                
                @Override
                protected void finalize() throws Throwable {
                    // Safety net to ensure the connection is returned to the pool
                    // if the user forgets to close the stream
                    if (!closed.get()) {
                        logger.warn("Upload stream was not closed properly, closing in finalize(): {}", remotePath);
                        try {
                            close();
                        } catch (IOException e) {
                            // Ignore, we're in finalize
                        }
                    }
                    super.finalize();
                }
            };
            
            // Add progress tracking if requested
            if (progressCallback != null) {
                wrappedStream = new ProgressTrackingOutputStream(
                        wrappedStream, progressCallback, -1, actualRestartOffset);
            }
            
            return wrappedStream;
            
        } catch (IOException e) {
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.UPLOAD_ERROR, 
                    remotePath, 
                    "Failed to open upload stream to file: " + e.getMessage(), 
                    e);
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
        if (inputStream == null) {
            throw new IllegalArgumentException("Input stream cannot be null");
        }
        
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Appending to file: {}", new Object[] { remotePath });
            
            // Set binary transfer mode
            client.setFileType(FTP.BINARY_FILE_TYPE);
            
            // Ensure the parent directory exists
            ensureParentDirectoryExists(client, remotePath);
            
            // Append to the file
            boolean success = client.appendFile(remotePath, inputStream);
            
            if (!success) {
                String errorMsg = "Failed to append to file: " + remotePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.UPLOAD_ERROR, 
                        remotePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("File appended successfully: {}", new Object[] { remotePath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.UPLOAD_ERROR, 
                    remotePath, 
                    "Failed to append to file: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Ensures that the parent directory of a file exists, creating it if necessary.
     * 
     * @param client the FTP client to use
     * @param filePath the path to the file
     * @throws IOException if an error occurs creating the directory
     */
    private void ensureParentDirectoryExists(FTPClient client, String filePath) throws IOException {
        // Get the parent directory
        String parentDir = new File(filePath).getParent();
        
        // If there's no parent directory or it's just the root, nothing to do
        if (parentDir == null || parentDir.equals("/") || parentDir.isEmpty()) {
            return;
        }
        
        // Normalize the path
        if (!parentDir.startsWith("/")) {
            parentDir = "/" + parentDir;
        }
        
        // Check if the directory exists
        client.changeWorkingDirectory(parentDir);
        if (client.getReplyCode() == 550) { // Directory doesn't exist
            // Create the directory and all parent directories
            createDirectoryTree(client, parentDir);
        }
        
        // Change back to the root directory
        client.changeWorkingDirectory("/");
    }
    
    /**
     * Creates a directory tree on the FTP server.
     * 
     * @param client the FTP client to use
     * @param dirPath the directory path to create
     * @throws IOException if an error occurs creating the directories
     */
    private void createDirectoryTree(FTPClient client, String dirPath) throws IOException {
        String[] pathElements = dirPath.split("/");
        
        // Start from the root
        client.changeWorkingDirectory("/");
        
        // Create each directory in the path
        StringBuilder currentPath = new StringBuilder();
        for (String pathElement : pathElements) {
            if (pathElement.isEmpty()) {
                continue;
            }
            
            currentPath.append("/").append(pathElement);
            
            // Try to change to the directory
            boolean exists = client.changeWorkingDirectory(currentPath.toString());
            
            if (!exists) {
                // Create the directory if it doesn't exist
                boolean created = client.makeDirectory(currentPath.toString());
                if (!created) {
                    throw new FTPOperationException(
                            FTPOperationException.ErrorType.MKDIR_ERROR,
                            currentPath.toString(),
                            client.getReplyCode(),
                            "Failed to create directory: " + currentPath.toString() + 
                            ", Reply: " + client.getReplyString());
                }
                
                client.changeWorkingDirectory(currentPath.toString());
            }
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Creating directory: {}", new Object[] { directoryPath });
            
            // Ensure the parent directory exists
            String parentDir = new File(directoryPath).getParent();
            if (parentDir != null && !parentDir.equals("/") && !parentDir.isEmpty()) {
                ensureParentDirectoryExists(client, directoryPath);
            }
            
            // Create the directory
            boolean success = client.makeDirectory(directoryPath);
            
            if (!success) {
                String errorMsg = "Failed to create directory: " + directoryPath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                
                // If the directory already exists, that's not an error
                if (client.getReplyCode() == 550 && client.getReplyString().contains("already exists")) {
                    logger.debug("Directory already exists: {}", new Object[] { directoryPath });
                    return true;
                }
                
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.MKDIR_ERROR, 
                        directoryPath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Directory created successfully: {}", new Object[] { directoryPath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.MKDIR_ERROR, 
                    directoryPath, 
                    "Failed to create directory: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        if (directoryPaths == null || directoryPaths.isEmpty()) {
            return new HashMap<>();
        }
        
        Map<String, Boolean> results = new HashMap<>();
        
        for (String directoryPath : directoryPaths) {
            try {
                boolean success = makeDirectory(directoryPath);
                results.put(directoryPath, success);
            } catch (IOException e) {
                results.put(directoryPath, false);
                if (!continueOnError) {
                    throw e;
                }
                logger.warn("Failed to create directory: {}. Continuing with next directory.", 
                        new Object[] { directoryPath });
            }
        }
        
        return results;
    }
    
    /**
     * Creates a directory and all parent directories if they don't exist.
     * This method is similar to 'mkdir -p' in Linux.
     * 
     * @param directoryPath the path of the directory to create
     * @return true if the directory was created successfully, false otherwise
     * @throws IOException if an error occurs creating the directory
     */
    public boolean makeDirectories(String directoryPath) throws IOException {
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Creating directory tree: {}", new Object[] { directoryPath });
            
            // Normalize the path
            if (!directoryPath.startsWith("/")) {
                directoryPath = "/" + directoryPath;
            }
            
            // Create the directory and all parent directories
            createDirectoryTree(client, directoryPath);
            
            logger.debug("Directory tree created successfully: {}", new Object[] { directoryPath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.MKDIR_ERROR, 
                    directoryPath, 
                    "Failed to create directory tree: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Deleting file: {}", new Object[] { filePath });
            
            // Delete the file
            boolean success = client.deleteFile(filePath);
            
            if (!success) {
                String errorMsg = "Failed to delete file: " + filePath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.DELETION_ERROR, 
                        filePath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("File deleted successfully: {}", new Object[] { filePath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.DELETION_ERROR, 
                    filePath, 
                    "Failed to delete file: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Copies a directory structure from one location to another on the FTP server.
     * 
     * @param sourceDir the source directory path
     * @param destDir the destination directory path
     * @param createDestDir whether to create the destination directory if it doesn't exist
     * @param includeFiles whether to copy files in addition to the directory structure
     * @return true if the directory structure was copied successfully, false otherwise
     * @throws IOException if an error occurs copying the directory structure
     */
    public boolean copyDirectoryStructure(String sourceDir, String destDir, 
            boolean createDestDir, boolean includeFiles) throws IOException {
        
        // Normalize paths
        if (!sourceDir.startsWith("/")) {
            sourceDir = "/" + sourceDir;
        }
        if (!destDir.startsWith("/")) {
            destDir = "/" + destDir;
        }
        
        // Remove trailing slashes
        if (sourceDir.endsWith("/") && !sourceDir.equals("/")) {
            sourceDir = sourceDir.substring(0, sourceDir.length() - 1);
        }
        if (destDir.endsWith("/") && !destDir.equals("/")) {
            destDir = destDir.substring(0, destDir.length() - 1);
        }
        
        // Check if source directory exists
        if (!directoryExists(sourceDir)) {
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.COPY_ERROR, 
                    sourceDir, 
                    "Source directory does not exist: " + sourceDir);
        }
        
        // Create destination directory if needed
        if (createDestDir && !directoryExists(destDir)) {
            makeDirectories(destDir);
        }
        
        // List directories in the source directory
        List<FTPFile> directories = listDirectories(sourceDir);
        
        // Create corresponding directories in the destination
        for (FTPFile dir : directories) {
            String sourcePath = sourceDir + "/" + dir.getName();
            String destPath = destDir + "/" + dir.getName();
            
            // Create the directory
            makeDirectory(destPath);
            
            // Recursively copy subdirectories
            copyDirectoryStructure(sourcePath, destPath, false, includeFiles);
        }
        
        // Copy files if requested
        if (includeFiles) {
            // List files in the source directory
            List<FTPFile> files = listFiles(sourceDir);
            
            // Copy each file
            for (FTPFile file : files) {
                if (!file.isDirectory()) {
                    String sourcePath = sourceDir + "/" + file.getName();
                    String destPath = destDir + "/" + file.getName();
                    
                    // Create an input stream for the source file
                    try (InputStream inputStream = retrieveFileStream(sourcePath)) {
                        // Copy the file to the destination
                        storeFile(destPath, inputStream);
                    }
                }
            }
        }
        
        return true;
    }
    
    /**
     * Calculates the total size of a directory, including all files and subdirectories.
     * 
     * @param directoryPath the path of the directory to calculate the size of
     * @return the total size of the directory in bytes
     * @throws IOException if an error occurs calculating the size
     */
    public long getDirectorySize(String directoryPath) throws IOException {
        return getDirectorySize(directoryPath, -1); // No depth limit
    }
    
    /**
     * Calculates the total size of a directory, including all files and subdirectories,
     * up to a specified depth.
     * 
     * @param directoryPath the path of the directory to calculate the size of
     * @param maxDepth the maximum depth to recurse (-1 for unlimited)
     * @return the total size of the directory in bytes
     * @throws IOException if an error occurs calculating the size
     */
    public long getDirectorySize(String directoryPath, int maxDepth) throws IOException {
        if (!directoryExists(directoryPath)) {
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.LISTING_ERROR, 
                    directoryPath, 
                    "Directory does not exist: " + directoryPath);
        }
        
        long totalSize = 0;
        
        // List files in the directory
        List<FTPFile> files = listFiles(directoryPath);
        
        // Sum the size of all files
        for (FTPFile file : files) {
            if (file.isFile()) {
                totalSize += file.getSize();
            } else if (file.isDirectory() && (maxDepth > 0 || maxDepth == -1)) {
                // Recursively get size of subdirectories
                String subDirPath = directoryPath;
                if (!subDirPath.endsWith("/")) {
                    subDirPath += "/";
                }
                subDirPath += file.getName();
                
                int newDepth = maxDepth == -1 ? -1 : maxDepth - 1;
                totalSize += getDirectorySize(subDirPath, newDepth);
            }
        }
        
        return totalSize;
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Deleting directory{}: {}", 
                    new Object[] { recursive ? " recursively" : "", directoryPath });
            
            if (recursive) {
                // Delete the directory and all its contents
                return deleteDirectoryRecursive(client, directoryPath);
            } else {
                // Delete just the directory (must be empty)
                boolean success = client.removeDirectory(directoryPath);
                
                if (!success) {
                    String errorMsg = "Failed to delete directory: " + directoryPath + 
                            ", Reply: " + client.getReplyString();
                    logger.error(errorMsg);
                    throw new FTPOperationException(
                            FTPOperationException.ErrorType.DELETION_ERROR, 
                            directoryPath, 
                            client.getReplyCode(), 
                            errorMsg);
                }
                
                logger.debug("Directory deleted successfully: {}", new Object[] { directoryPath });
                return true;
            }
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.DELETION_ERROR, 
                    directoryPath, 
                    "Failed to delete directory: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Deletes a directory and all its contents recursively.
     * 
     * @param client the FTP client to use
     * @param directoryPath the path of the directory to delete
     * @return true if the directory was deleted successfully, false otherwise
     * @throws IOException if an error occurs deleting the directory
     */
    private boolean deleteDirectoryRecursive(FTPClient client, String directoryPath) throws IOException {
        // Normalize the path
        if (!directoryPath.startsWith("/")) {
            directoryPath = "/" + directoryPath;
        }
        if (directoryPath.endsWith("/")) {
            directoryPath = directoryPath.substring(0, directoryPath.length() - 1);
        }
        
        // List files in the directory
        FTPFile[] files = client.listFiles(directoryPath);
        if (files == null) {
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.LISTING_ERROR,
                    directoryPath,
                    client.getReplyCode(),
                    "Failed to list files in directory: " + directoryPath + 
                    ", Reply: " + client.getReplyString());
        }
        
        // Delete each file/directory in the directory
        for (FTPFile file : files) {
            String name = file.getName();
            if (name.equals(".") || name.equals("..")) {
                continue;
            }
            
            String filePath = directoryPath + "/" + name;
            
            if (file.isDirectory()) {
                // Recursively delete subdirectories
                deleteDirectoryRecursive(client, filePath);
            } else {
                // Delete file
                boolean deleted = client.deleteFile(filePath);
                if (!deleted) {
                    logger.warn("Failed to delete file: {} (continuing with directory deletion)", filePath);
                }
            }
        }
        
        // Delete the now-empty directory
        boolean success = client.removeDirectory(directoryPath);
        
        if (!success) {
            String errorMsg = "Failed to delete directory: " + directoryPath + 
                    ", Reply: " + client.getReplyString();
            logger.error(errorMsg);
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.DELETION_ERROR, 
                    directoryPath, 
                    client.getReplyCode(), 
                    errorMsg);
        }
        
        logger.debug("Directory deleted recursively: {}", new Object[] { directoryPath });
        return true;
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
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Renaming from {} to {}", new Object[] { fromPath, toPath });
            
            // Ensure the parent directory exists for the destination
            ensureParentDirectoryExists(client, toPath);
            
            // Rename the file or directory
            boolean success = client.rename(fromPath, toPath);
            
            if (!success) {
                String errorMsg = "Failed to rename from " + fromPath + " to " + toPath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.RENAME_ERROR, 
                        fromPath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Renamed successfully from {} to {}", new Object[] { fromPath, toPath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.RENAME_ERROR, 
                    fromPath, 
                    "Failed to rename from " + fromPath + " to " + toPath + ": " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Gets the current working directory on the FTP server.
     * 
     * @return the current working directory
     * @throws IOException if an error occurs getting the current working directory
     */
    public String printWorkingDirectory() throws IOException {
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Getting current working directory");
            
            // Get the current working directory
            String pwd = client.printWorkingDirectory();
            
            if (pwd == null) {
                String errorMsg = "Failed to get current working directory, Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.PWD_ERROR, 
                        null, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Current working directory: {}", new Object[] { pwd });
            return pwd;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.PWD_ERROR, 
                    null, 
                    "Failed to get current working directory: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
    
    /**
     * Changes the current working directory on the FTP server.
     * 
     * @param directoryPath the path to change to
     * @return true if the directory was changed successfully, false otherwise
     * @throws IOException if an error occurs changing the directory
     */
    public boolean changeWorkingDirectory(String directoryPath) throws IOException {
        FTPClient client = null;
        try {
            // Borrow a connection from the pool
            client = connectionPool.borrowConnection();
            
            logger.debug("Changing working directory to: {}", new Object[] { directoryPath });
            
            // Change the working directory
            boolean success = client.changeWorkingDirectory(directoryPath);
            
            if (!success) {
                String errorMsg = "Failed to change working directory to: " + directoryPath + 
                        ", Reply: " + client.getReplyString();
                logger.error(errorMsg);
                throw new FTPOperationException(
                        FTPOperationException.ErrorType.PWD_ERROR, 
                        directoryPath, 
                        client.getReplyCode(), 
                        errorMsg);
            }
            
            logger.debug("Changed working directory to: {}", new Object[] { directoryPath });
            return true;
            
        } catch (IOException e) {
            // Invalidate the connection if there was an error
            if (client != null) {
                logger.debug("Invalidating connection due to error: {}", new Object[] { e.getMessage() });
                connectionPool.invalidateConnection(client);
                client = null;
            }
            
            throw new FTPOperationException(
                    FTPOperationException.ErrorType.PWD_ERROR, 
                    directoryPath, 
                    "Failed to change working directory: " + e.getMessage(), 
                    e);
            
        } finally {
            // Return the connection to the pool if it wasn't invalidated
            if (client != null) {
                connectionPool.returnConnection(client);
            }
        }
    }
}