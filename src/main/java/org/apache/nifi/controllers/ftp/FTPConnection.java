package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents an FTP connection with its associated state and metadata.
 */
public class FTPConnection {
    private final String id;
    private final FTPConnectionConfig config;
    private final ComponentLog logger;
    private final AtomicReference<FTPConnectionState> state;
    private FTPClient client;
    
    private Instant createdAt;
    private Instant lastUsedAt;
    private Instant lastTestedAt;
    private String currentDirectory;
    private int reconnectAttempts;
    private String lastErrorMessage;
    
    /**
     * Creates a new FTPConnection with the given configuration and logger.
     * The connection starts in the DISCONNECTED state.
     * 
     * @param config the FTP connection configuration
     * @param logger the logger to use
     */
    public FTPConnection(FTPConnectionConfig config, ComponentLog logger) {
        this.id = UUID.randomUUID().toString();
        this.config = config;
        this.logger = logger;
        this.state = new AtomicReference<>(FTPConnectionState.DISCONNECTED);
        this.createdAt = Instant.now();
        this.reconnectAttempts = 0;
        this.currentDirectory = "/";
    }
    
    /**
     * Gets the unique ID of this connection.
     * 
     * @return the connection ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the current state of the connection.
     * 
     * @return the connection state
     */
    public FTPConnectionState getState() {
        return state.get();
    }
    
    /**
     * Sets the connection state.
     * 
     * @param newState the new connection state
     * @return the previous state
     */
    public FTPConnectionState setState(FTPConnectionState newState) {
        FTPConnectionState previousState = state.getAndSet(newState);
        logger.debug("Connection {} state changed from {} to {}", new Object[] { id, previousState, newState });
        return previousState;
    }
    
    /**
     * Atomically sets the state to newState if the current state is expectedState.
     * 
     * @param expectedState the expected current state
     * @param newState the new state to set
     * @return true if successful, false otherwise
     */
    public boolean compareAndSetState(FTPConnectionState expectedState, FTPConnectionState newState) {
        boolean updated = state.compareAndSet(expectedState, newState);
        if (updated) {
            logger.debug("Connection {} state changed from {} to {}", new Object[] { id, expectedState, newState });
        }
        return updated;
    }
    
    /**
     * Gets the FTP client associated with this connection.
     * 
     * @return the FTP client
     */
    public FTPClient getClient() {
        return client;
    }
    
    /**
     * Sets the FTP client for this connection.
     * 
     * @param client the FTP client to set
     */
    public void setClient(FTPClient client) {
        this.client = client;
    }
    
    /**
     * Updates the last used timestamp to now.
     */
    public void markAsUsed() {
        this.lastUsedAt = Instant.now();
    }
    
    /**
     * Updates the last tested timestamp to now.
     */
    public void markAsTested() {
        this.lastTestedAt = Instant.now();
    }
    
    /**
     * Gets the time when this connection was created.
     * 
     * @return the creation time
     */
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    /**
     * Gets the time when this connection was last used.
     * 
     * @return the last used time, or null if never used
     */
    public Instant getLastUsedAt() {
        return lastUsedAt;
    }
    
    /**
     * Gets the time when this connection was last tested.
     * 
     * @return the last tested time, or null if never tested
     */
    public Instant getLastTestedAt() {
        return lastTestedAt;
    }
    
    /**
     * Gets the current working directory on the FTP server.
     * 
     * @return the current directory
     */
    public String getCurrentDirectory() {
        return currentDirectory;
    }
    
    /**
     * Sets the current working directory.
     * 
     * @param currentDirectory the current directory to set
     */
    public void setCurrentDirectory(String currentDirectory) {
        this.currentDirectory = currentDirectory;
    }
    
    /**
     * Gets the number of reconnection attempts made.
     * 
     * @return the reconnect attempts count
     */
    public int getReconnectAttempts() {
        return reconnectAttempts;
    }
    
    /**
     * Increments the reconnection attempts count.
     * 
     * @return the new count
     */
    public int incrementReconnectAttempts() {
        return ++reconnectAttempts;
    }
    
    /**
     * Resets the reconnection attempts count to zero.
     */
    public void resetReconnectAttempts() {
        this.reconnectAttempts = 0;
    }
    
    /**
     * Gets the last error message.
     * 
     * @return the last error message, or null if no error
     */
    public String getLastErrorMessage() {
        return lastErrorMessage;
    }
    
    /**
     * Sets the last error message.
     * 
     * @param lastErrorMessage the error message to set
     */
    public void setLastErrorMessage(String lastErrorMessage) {
        this.lastErrorMessage = lastErrorMessage;
    }
    
    /**
     * Gets the configuration for this connection.
     * 
     * @return the connection configuration
     */
    public FTPConnectionConfig getConfig() {
        return config;
    }
    
    /**
     * Gets a summary of the connection for logging purposes.
     * 
     * @return a summary string
     */
    public String getSummary() {
        return String.format("FTPConnection[id=%s, host=%s:%d, state=%s, createdAt=%s, lastUsedAt=%s]",
                id, config.getHostname(), config.getPort(), state.get(), createdAt,
                lastUsedAt == null ? "never" : lastUsedAt);
    }
    
    /**
     * Updates the working directory on the FTP server.
     * 
     * @param directory the directory to change to
     * @return true if successful, false otherwise
     */
    public boolean changeWorkingDirectory(String directory) {
        if (client == null || !getState().isUsable()) {
            return false;
        }
        
        try {
            boolean success = client.changeWorkingDirectory(directory);
            if (success) {
                setCurrentDirectory(directory);
            }
            return success;
        } catch (IOException e) {
            logger.error("Failed to change working directory to {}: {}", new Object[] { directory, e.getMessage() });
            return false;
        }
    }
    
    @Override
    public String toString() {
        return getSummary();
    }
}