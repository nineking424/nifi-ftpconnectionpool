package org.apache.nifi.controllers.ftp;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a file on an FTP server with extended attribute support.
 * This model enhances the Apache Commons Net FTPFile class with additional
 * attribute handling capabilities.
 */
public class FTPFile {
    private String name;
    private String path;
    private long size;
    private Date timestamp;
    private String permissions;
    private String owner;
    private String group;
    private FTPFileType type;
    private Map<String, Object> attributes;

    /**
     * Creates a new FTPFile instance.
     */
    public FTPFile() {
        this.attributes = new HashMap<>();
    }

    /**
     * Gets the name of the file.
     *
     * @return The file name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the file.
     *
     * @param name The file name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the path of the file on the FTP server.
     *
     * @return The file path
     */
    public String getPath() {
        return path;
    }

    /**
     * Sets the path of the file on the FTP server.
     *
     * @param path The file path
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Gets the size of the file in bytes.
     *
     * @return The file size
     */
    public long getSize() {
        return size;
    }

    /**
     * Sets the size of the file in bytes.
     *
     * @param size The file size
     */
    public void setSize(long size) {
        this.size = size;
    }

    /**
     * Gets the timestamp of the file.
     *
     * @return The file timestamp
     */
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp of the file.
     *
     * @param timestamp The file timestamp
     */
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the permissions of the file in Unix-style format (e.g., "rwxr-xr--").
     *
     * @return The file permissions
     */
    public String getPermissions() {
        return permissions;
    }

    /**
     * Sets the permissions of the file.
     *
     * @param permissions The file permissions
     */
    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    /**
     * Gets the owner of the file.
     *
     * @return The file owner
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Sets the owner of the file.
     *
     * @param owner The file owner
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * Gets the group of the file.
     *
     * @return The file group
     */
    public String getGroup() {
        return group;
    }

    /**
     * Sets the group of the file.
     *
     * @param group The file group
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Gets the type of the file.
     *
     * @return The file type
     */
    public FTPFileType getType() {
        return type;
    }

    /**
     * Sets the type of the file.
     *
     * @param type The file type
     */
    public void setType(FTPFileType type) {
        this.type = type;
    }

    /**
     * Gets all additional attributes of the file.
     *
     * @return A map of attribute names to values
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /**
     * Sets all additional attributes of the file.
     *
     * @param attributes A map of attribute names to values
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    /**
     * Gets a specific attribute value.
     *
     * @param name The attribute name
     * @return The attribute value, or null if not present
     */
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    /**
     * Sets a specific attribute value.
     *
     * @param name The attribute name
     * @param value The attribute value
     */
    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    /**
     * Checks if the file is a directory.
     *
     * @return true if the file is a directory, false otherwise
     */
    public boolean isDirectory() {
        return type == FTPFileType.DIRECTORY;
    }

    /**
     * Checks if the file is a regular file.
     *
     * @return true if the file is a regular file, false otherwise
     */
    public boolean isFile() {
        return type == FTPFileType.FILE;
    }

    /**
     * Checks if the file is a symbolic link.
     *
     * @return true if the file is a symbolic link, false otherwise
     */
    public boolean isSymbolicLink() {
        return type == FTPFileType.SYMBOLIC_LINK;
    }
}