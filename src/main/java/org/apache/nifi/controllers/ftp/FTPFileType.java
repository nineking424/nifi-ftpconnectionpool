package org.apache.nifi.controllers.ftp;

/**
 * Enumeration of FTP file types.
 */
public enum FTPFileType {
    /** A regular file. */
    FILE,
    
    /** A directory. */
    DIRECTORY,
    
    /** A symbolic link. */
    SYMBOLIC_LINK,
    
    /** An unknown file type. */
    UNKNOWN
}