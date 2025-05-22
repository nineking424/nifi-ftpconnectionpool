package org.apache.nifi.controllers.ftp;

/**
 * Enumeration for tracking the state of an FTP connection.
 */
public enum FTPConnectionState {
    /**
     * The connection has not been established yet.
     */
    DISCONNECTED,
    
    /**
     * The connection is in the process of being established.
     */
    CONNECTING,
    
    /**
     * The connection has been established and is ready for use.
     */
    CONNECTED,
    
    /**
     * The connection is currently being used for an operation.
     */
    BUSY,
    
    /**
     * The connection is valid but is currently idle.
     */
    IDLE,
    
    /**
     * The connection is in the process of being closed.
     */
    DISCONNECTING,
    
    /**
     * The connection has failed and needs to be reestablished.
     */
    FAILED,
    
    /**
     * The connection is currently being reconnected after a failure.
     */
    RECONNECTING;
    
    /**
     * Checks if the connection is in a state where it can be used.
     * 
     * @return true if the connection is in CONNECTED or IDLE state
     */
    public boolean isUsable() {
        return this == CONNECTED || this == IDLE;
    }
    
    /**
     * Checks if the connection is in a transitional state.
     * 
     * @return true if the connection is in CONNECTING, DISCONNECTING, or RECONNECTING state
     */
    public boolean isTransitional() {
        return this == CONNECTING || this == DISCONNECTING || this == RECONNECTING;
    }
    
    /**
     * Checks if the connection is in a failed state.
     * 
     * @return true if the connection is in FAILED state
     */
    public boolean isFailed() {
        return this == FAILED;
    }
    
    /**
     * Checks if the connection is busy.
     * 
     * @return true if the connection is in BUSY state
     */
    public boolean isBusy() {
        return this == BUSY;
    }
    
    /**
     * Checks if the connection is disconnected.
     * 
     * @return true if the connection is in DISCONNECTED state
     */
    public boolean isDisconnected() {
        return this == DISCONNECTED;
    }
}