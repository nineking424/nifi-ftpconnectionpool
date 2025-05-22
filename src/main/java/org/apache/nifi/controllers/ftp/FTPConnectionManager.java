package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ProxyClient;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages FTP connection creation, validation, and closing with enhanced state tracking.
 */
public class FTPConnectionManager {
    private final FTPConnectionConfig config;
    private final ComponentLog logger;
    private final SecureCredentialHandler credentialHandler;
    
    // Connection tracking
    private final ConcurrentHashMap<String, FTPConnection> connections = new ConcurrentHashMap<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    
    // Scheduler for connection maintenance tasks
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> connectionMaintenanceTask;
    
    // Reconnection settings
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private static final long[] RECONNECT_BACKOFF_MS = {
        1000,  // 1 second
        5000,  // 5 seconds
        15000, // 15 seconds
        30000, // 30 seconds
        60000  // 60 seconds
    };

    /**
     * Creates a new FTPConnectionManager with the given configuration and logger.
     *
     * @param config the FTP connection configuration
     * @param logger the logger to use
     */
    public FTPConnectionManager(FTPConnectionConfig config, ComponentLog logger) {
        this.config = config;
        this.logger = logger;
        
        // Create a secure credential handler for this connection manager
        this.credentialHandler = new SecureCredentialHandler(logger);
        
        // Store password securely
        if (config.getPassword() != null) {
            this.credentialHandler.storeCredential(SecureCredentialHandler.CredentialType.PASSWORD, config.getPassword());
        }
        
        // Store proxy password securely if available
        if (config.getProxyPassword() != null) {
            this.credentialHandler.storeCredential(SecureCredentialHandler.CredentialType.PROXY_PASSWORD, config.getProxyPassword());
        }
        
        // Initialize the connection manager
        initialize();
    }
    
    /**
     * Initializes the connection manager.
     */
    private void initialize() {
        if (initialized.compareAndSet(false, true)) {
            logger.debug("Initializing FTP connection manager");
            
            // Create a scheduler for maintenance tasks
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("FTP-Connection-Manager-" + config.getHostname());
                t.setDaemon(true);
                return t;
            });
            
            // Schedule connection maintenance task if idle timeout or keep-alive is set
            if (config.getConnectionIdleTimeout() > 0 || config.getKeepAliveInterval() > 0) {
                long maintenanceInterval = Math.min(
                    config.getConnectionIdleTimeout() > 0 ? config.getConnectionIdleTimeout() / 2 : Long.MAX_VALUE,
                    config.getKeepAliveInterval() > 0 ? config.getKeepAliveInterval() : Long.MAX_VALUE
                );
                
                maintenanceInterval = Math.max(1000, maintenanceInterval); // At least 1 second
                
                this.connectionMaintenanceTask = scheduler.scheduleAtFixedRate(
                    this::performConnectionMaintenance,
                    maintenanceInterval,
                    maintenanceInterval,
                    TimeUnit.MILLISECONDS
                );
                
                logger.debug("Scheduled connection maintenance task with interval: {} ms", maintenanceInterval);
            }
        }
    }
    
    /**
     * Performs maintenance on all managed connections.
     * This includes closing idle connections and sending keep-alive messages.
     */
    private void performConnectionMaintenance() {
        if (shutdown.get()) {
            return;
        }
        
        try {
            logger.debug("Performing connection maintenance");
            Instant now = Instant.now();
            
            for (FTPConnection connection : connections.values()) {
                // Skip connections that are not in a usable state
                if (!connection.getState().isUsable()) {
                    continue;
                }
                
                // Check if the connection has been idle for too long
                if (config.getConnectionIdleTimeout() > 0 && 
                    connection.getLastUsedAt() != null &&
                    Duration.between(connection.getLastUsedAt(), now).toMillis() > config.getConnectionIdleTimeout()) {
                    
                    logger.debug("Closing idle connection: {}", connection.getId());
                    closeConnection(connection);
                    continue;
                }
                
                // Send keep-alive if needed
                if (config.getKeepAliveInterval() > 0 &&
                    (connection.getLastTestedAt() == null ||
                     Duration.between(connection.getLastTestedAt(), now).toMillis() > config.getKeepAliveInterval())) {
                    
                    logger.debug("Sending keep-alive to connection: {}", connection.getId());
                    validateConnection(connection);
                }
            }
        } catch (Exception e) {
            logger.error("Error during connection maintenance: {}", new Object[] { e.getMessage() }, e);
        }
    }

    /**
     * Creates a new FTP connection with state tracking.
     *
     * @return a new FTPConnection in the CONNECTED state
     * @throws FTPConnectionException if an error occurs while connecting
     */
    public FTPConnection createManagedConnection() throws FTPConnectionException {
        if (shutdown.get()) {
            throw new FTPConnectionException(
                    FTPConnectionException.ErrorType.POOL_ERROR,
                    "FTP connection manager has been shut down");
        }
        
        // Create a new connection object
        FTPConnection connection = new FTPConnection(config, logger);
        connection.setState(FTPConnectionState.CONNECTING);
        
        try {
            // Establish the actual FTP connection
            FTPClient client = createAndConnectClient(connection);
            connection.setClient(client);
            
            // Update connection state and metadata
            connection.setState(FTPConnectionState.CONNECTED);
            connection.markAsUsed();
            connection.markAsTested();
            
            // Add to managed connections
            connections.put(connection.getId(), connection);
            
            logger.debug("Created managed connection: {}", connection.getId());
            return connection;
            
        } catch (IOException e) {
            connection.setState(FTPConnectionState.FAILED);
            connection.setLastErrorMessage(e.getMessage());
            
            // Wrap the exception with more context
            FTPConnectionException.ErrorType errorType;
            
            if (e.getMessage().contains("refused")) {
                errorType = FTPConnectionException.ErrorType.CONNECTION_ERROR;
            } else if (e.getMessage().contains("login") || e.getMessage().contains("authentication")) {
                errorType = FTPConnectionException.ErrorType.AUTHENTICATION_ERROR;
            } else if (e.getMessage().contains("timed out") || e.getMessage().contains("timeout")) {
                errorType = FTPConnectionException.ErrorType.TIMEOUT_ERROR;
            } else {
                errorType = FTPConnectionException.ErrorType.CONNECTION_ERROR;
            }
            
            throw new FTPConnectionException(
                    errorType,
                    connection,
                    "Failed to establish FTP connection: " + e.getMessage(),
                    e);
        }
    }
    
    /**
     * Creates and connects an FTPClient for the given connection.
     *
     * @param connection the connection to establish
     * @return a connected FTPClient
     * @throws IOException if an error occurs while connecting
     */
    private FTPClient createAndConnectClient(FTPConnection connection) throws IOException {
        // Determine if we need an FTPS client based on SSL settings
        FTPClient client = createAppropriateClient();
        
        try {
            // Set timeouts and encoding
            client.setConnectTimeout(config.getConnectionTimeout());
            client.setDataTimeout(config.getDataTimeout());
            client.setControlEncoding(config.getControlEncoding());
            
            // Set control channel timeout if specified
            if (config.getControlTimeout() > 0) {
                client.setControlKeepAliveTimeout(config.getControlTimeout() / 1000);
            }
            
            // Configure buffer size if specified
            if (config.getBufferSize() > 0) {
                client.setBufferSize(config.getBufferSize());
            }
            
            // Handle proxy if configured
            Socket connectionSocket = null;
            if (shouldUseProxy()) {
                connectionSocket = createProxyConnection();
                if (connectionSocket != null) {
                    // Use the socket already connected through the proxy
                    client.setSocketFactory(() -> connectionSocket);
                }
            }
            
            // Connect to server
            logger.debug("Connecting to FTP server {}:{}", new Object[] { config.getHostname(), config.getPort() });
            
            if (connectionSocket != null) {
                // If we're using a proxy socket, we already connected
                client.connect("localhost", 0); // This will use the pre-connected socket
            } else {
                // Standard connection
                client.connect(config.getHostname(), config.getPort());
            }
            
            // Check reply code to make sure connection was successful
            int reply = client.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                client.disconnect();
                throw new IOException("FTP server refused connection with code " + reply);
            }
            
            // Login with credentials
            login(client);
            
            // Configure connection mode
            if (config.isActiveMode()) {
                logger.debug("Using active mode");
                
                // Handle active mode configuration
                if (config.isLocalActivePortRange() && 
                    config.getActivePortRangeStart() > 0 && 
                    config.getActivePortRangeEnd() > config.getActivePortRangeStart()) {
                    
                    logger.debug("Setting active port range: {} - {}", 
                        config.getActivePortRangeStart(), config.getActivePortRangeEnd());
                    client.setActivePortRange(config.getActivePortRangeStart(), config.getActivePortRangeEnd());
                }
                
                // Set external IP address for active mode if configured
                if (config.getActiveExternalIPAddress() != null && !config.getActiveExternalIPAddress().isEmpty()) {
                    logger.debug("Setting active external IP address: {}", config.getActiveExternalIPAddress());
                    client.setActiveExternalIPAddress(config.getActiveExternalIPAddress());
                }
                
                client.enterLocalActiveMode();
            } else {
                logger.debug("Using passive mode");
                client.enterLocalPassiveMode();
            }
            
            // Send keep-alive if configured
            if (config.getKeepAliveInterval() > 0) {
                client.setControlKeepAliveTimeout(config.getKeepAliveInterval() / 1000);
            }
            
            // Set transfer mode if configured
            if (config.getTransferMode() != null) {
                logger.debug("Setting transfer mode to: {}", config.getTransferMode());
                if ("ASCII".equalsIgnoreCase(config.getTransferMode())) {
                    client.setFileType(FTPClient.ASCII_FILE_TYPE);
                } else {
                    // Default to binary
                    client.setFileType(FTPClient.BINARY_FILE_TYPE);
                }
            }
            
            logger.debug("FTP connection established successfully");
            return client;
            
        } catch (IOException e) {
            closeClient(client);
            throw e;
        }
    }

    /**
     * Creates the appropriate FTP client based on the configuration.
     * 
     * @return an FTPClient or FTPSClient instance
     */
    private FTPClient createAppropriateClient() {
        // Check if we need to use FTPS (either implicit or explicit)
        if (config.isUseImplicitSSL() || config.isUseExplicitSSL()) {
            boolean useImplicit = config.isUseImplicitSSL();
            logger.debug("Creating FTPS client with {} SSL", useImplicit ? "implicit" : "explicit");
            
            // Create the appropriate FTPS client
            FTPSClient ftpsClient = new FTPSClient(useImplicit);
            
            // Configure SSL settings
            ftpsClient.setUseClientMode(config.isUseClientMode());
            
            // Configure enabled protocols if specified
            if (config.getEnabledProtocols() != null && config.getEnabledProtocols().length > 0) {
                logger.debug("Setting enabled SSL/TLS protocols");
                ftpsClient.setEnabledProtocols(config.getEnabledProtocols());
            }
            
            // Configure enabled cipher suites if specified
            if (config.getEnabledCipherSuites() != null && config.getEnabledCipherSuites().length > 0) {
                logger.debug("Setting enabled SSL/TLS cipher suites");
                ftpsClient.setEnabledCipherSuites(config.getEnabledCipherSuites());
            }
            
            // Configure certificate validation
            if (!config.isValidateServerCertificate()) {
                logger.warn("Server certificate validation is disabled - this is not recommended for production use");
                ftpsClient.setTrustManager(TrustManagerUtils.getAcceptAllTrustManager());
            } else if (config.getTrustStorePath() != null && !config.getTrustStorePath().isEmpty()) {
                // Use custom truststore if provided
                logger.debug("Using custom truststore for certificate validation: {}", config.getTrustStorePath());
                try {
                    // Load the truststore
                    KeyStore trustStore = KeyStore.getInstance(config.getTrustStoreType());
                    String trustStorePassword = config.getTrustStorePassword();
                    if (trustStorePassword == null) {
                        // If password is not available from config, try to get it from credential handler
                        trustStorePassword = credentialHandler.retrieveCredential(
                                SecureCredentialHandler.CredentialType.TRUST_STORE_PASSWORD);
                    }
                    
                    // Load the truststore with password
                    try (FileInputStream fis = new FileInputStream(config.getTrustStorePath())) {
                        trustStore.load(fis, trustStorePassword != null ? trustStorePassword.toCharArray() : null);
                    }
                    
                    // Create and set the trust manager
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);
                    ftpsClient.setTrustManager(tmf.getTrustManagers()[0]);
                    
                } catch (Exception e) {
                    logger.error("Failed to initialize truststore. Falling back to default trust manager: {}", e.getMessage(), e);
                    // Fall back to system default trust manager
                }
            }
            
            return ftpsClient;
        } else {
            // Use standard FTP client
            return new FTPClient();
        }
    }
    
    /**
     * Determines whether a proxy should be used for the connection.
     * 
     * @return true if a proxy should be used, false otherwise
     */
    private boolean shouldUseProxy() {
        return config.getProxyHost() != null && !config.getProxyHost().isEmpty() && 
               config.getProxyPort() != null;
    }
    
    /**
     * Creates a socket connection through a proxy if configured.
     * 
     * @return a connected socket, or null if no proxy is configured
     * @throws IOException if an error occurs connecting through the proxy
     */
    private Socket createProxyConnection() throws IOException {
        if (!shouldUseProxy()) {
            return null;
        }
        
        try {
            logger.debug("Connecting through proxy {}:{} (type: {})", 
                    new Object[] { config.getProxyHost(), config.getProxyPort(), config.getProxyType() });
            
            ProxyClient proxyClient = new ProxyClient();
            
            // Set proxy credentials if available
            if (config.getProxyUsername() != null && !config.getProxyUsername().isEmpty()) {
                String proxyPassword = credentialHandler.retrieveCredential(
                        SecureCredentialHandler.CredentialType.PROXY_PASSWORD);
                proxyClient.setProxyCredentials(config.getProxyUsername(), proxyPassword);
                logger.debug("Using proxy authentication with username: {}", config.getProxyUsername());
            }
            
            // Set proxy type if configured
            if (config.getProxyType() != null && !config.getProxyType().isEmpty()) {
                logger.debug("Using proxy type: {}", config.getProxyType());
                
                // Determine proxy type to use
                ProxyClient.Type proxyType;
                switch (config.getProxyType().toUpperCase()) {
                    case "HTTP":
                        proxyType = ProxyClient.Type.HTTP;
                        break;
                    case "SOCKS":
                    case "SOCKS4":
                    case "SOCKS4A":
                        proxyType = ProxyClient.Type.SOCKS_V4;
                        break;
                    case "SOCKS5":
                        proxyType = ProxyClient.Type.SOCKS_V5;
                        break;
                    default:
                        logger.warn("Unknown proxy type: {}. Defaulting to SOCKS V5", config.getProxyType());
                        proxyType = ProxyClient.Type.SOCKS_V5;
                        break;
                }
                
                // Connect through proxy to the target server with specified type
                return proxyClient.connectForDataTransferProxy(
                        proxyType,
                        config.getProxyHost(), 
                        config.getProxyPort(),
                        config.getHostname(), 
                        config.getPort(), 
                        config.getConnectionTimeout());
            } else {
                // Connect through proxy to the target server using default proxy type
                return proxyClient.connect(config.getHostname(), config.getPort(),
                        config.getProxyHost(), config.getProxyPort());
            }
            
        } catch (IOException e) {
            logger.error("Failed to connect through proxy: {}", new Object[] { e.getMessage() });
            throw e;
        }
    }
    
    /**
     * Logs in to the FTP server using the configured credentials.
     * 
     * @param client the FTP client to log in with
     * @throws IOException if login fails
     */
    private void login(FTPClient client) throws IOException {
        try {
            logger.debug("Logging in as user {}", new Object[] { config.getUsername() });
            
            // Retrieve password securely
            String password = credentialHandler.retrieveCredential(
                    SecureCredentialHandler.CredentialType.PASSWORD);
            
            if (password == null) {
                // Fall back to config if credential handler doesn't have it
                password = config.getPassword();
            }
            
            if (!client.login(config.getUsername(), password)) {
                client.disconnect();
                throw new IOException("Failed to login to FTP server as user " + config.getUsername());
            }
            
        } catch (IOException e) {
            logger.error("Login failed: {}", new Object[] { e.getMessage() });
            throw e;
        }
    }

    /**
     * Validates that a connection is still active and usable.
     *
     * @param connection the connection to validate
     * @return true if the connection is valid, false otherwise
     * @throws FTPConnectionException if a serious error occurs during validation
     */
    public boolean validateConnection(FTPConnection connection) throws FTPConnectionException {
        if (connection == null) {
            throw new FTPConnectionException(
                    FTPConnectionException.ErrorType.VALIDATION_ERROR,
                    "Cannot validate null connection");
        }
        
        if (connection.getClient() == null) {
            connection.setState(FTPConnectionState.FAILED);
            connection.setLastErrorMessage("Connection has no client");
            logger.debug("Connection {} validation failed: no client", connection.getId());
            return false;
        }
        
        // Check if the connection is in a state that can be validated
        if (!connection.getState().isUsable() && !connection.getState().isFailed()) {
            logger.debug("Connection {} cannot be validated in state: {}", 
                    new Object[] { connection.getId(), connection.getState() });
            
            if (connection.getState().isTransitional()) {
                // If the connection is in a transitional state, wait for it to complete
                logger.debug("Connection {} is in transitional state {}, skipping validation", 
                        connection.getId(), connection.getState());
                return false;
            } else if (connection.getState().isDisconnected()) {
                // If the connection is already disconnected, no need to validate
                logger.debug("Connection {} is disconnected, skipping validation", connection.getId());
                return false;
            }
        }
        
        try {
            FTPClient client = connection.getClient();
            
            // Check if client is connected
            if (!client.isConnected()) {
                connection.setState(FTPConnectionState.FAILED);
                connection.setLastErrorMessage("Client is not connected");
                logger.debug("Connection {} validation failed: client not connected", connection.getId());
                return false;
            }
            
            // Send NOOP command to verify connection is still alive
            boolean valid = client.sendNoOp();
            int replyCode = client.getReplyCode();
            
            if (valid) {
                // Update connection state if it was previously failed
                if (connection.getState() == FTPConnectionState.FAILED) {
                    connection.setState(FTPConnectionState.CONNECTED);
                }
                
                // Update last tested timestamp
                connection.markAsTested();
                logger.debug("Connection {} validated successfully (reply code: {})", 
                        connection.getId(), replyCode);
                return true;
            } else {
                // Handle specific reply codes
                connection.setState(FTPConnectionState.FAILED);
                String errorMessage = "NOOP command failed with reply code: " + replyCode;
                connection.setLastErrorMessage(errorMessage);
                
                logger.debug("Connection {} validation failed: {}", connection.getId(), errorMessage);
                
                // For certain reply codes, we might want to take specific actions
                if (replyCode >= 500 && replyCode < 600) {
                    // 5xx codes indicate permanent negative completion
                    logger.warn("Server returned permanent error code: {}", replyCode);
                }
                
                return false;
            }
            
        } catch (IOException e) {
            // Update connection state
            connection.setState(FTPConnectionState.FAILED);
            String errorMessage = "Error validating connection: " + e.getMessage();
            connection.setLastErrorMessage(errorMessage);
            
            logger.debug("Error validating connection {}: {}", connection.getId(), e.getMessage());
            
            // Determine appropriate error type
            FTPConnectionException.ErrorType errorType;
            if (e.getMessage().contains("timed out") || e.getMessage().contains("timeout")) {
                errorType = FTPConnectionException.ErrorType.TIMEOUT_ERROR;
            } else if (e.getMessage().contains("connection is not open")) {
                errorType = FTPConnectionException.ErrorType.CONNECTION_CLOSED;
            } else {
                errorType = FTPConnectionException.ErrorType.VALIDATION_ERROR;
            }
            
            // For most validation errors, we just return false rather than throwing
            // This allows the caller to decide how to handle the failure
            return false;
        }
    }
    
    /**
     * For backward compatibility, validates a raw FTPClient.
     *
     * @param client the FTPClient to validate
     * @return true if the connection is valid, false otherwise
     */
    public boolean validateConnection(FTPClient client) {
        if (client == null) {
            return false;
        }
        
        try {
            // Check if client is connected
            if (!client.isConnected()) {
                logger.debug("Client is not connected");
                return false;
            }
            
            // Send NOOP command to verify connection is still alive
            boolean valid = client.sendNoOp();
            if (!valid) {
                logger.debug("NOOP command failed");
            }
            return valid;
            
        } catch (IOException e) {
            logger.debug("Error validating FTP connection: {}", new Object[] { e.getMessage() });
            return false;
        }
    }
    
    /**
     * Attempts to reconnect a failed connection.
     *
     * @param connection the connection to reconnect
     * @return true if reconnection was successful, false otherwise
     * @throws FTPConnectionException if a critical error occurs during reconnection
     */
    public boolean reconnect(FTPConnection connection) throws FTPConnectionException {
        if (connection == null) {
            throw new FTPConnectionException(
                    FTPConnectionException.ErrorType.VALIDATION_ERROR,
                    "Cannot reconnect null connection");
        }
        
        // Only reconnect connections that are in FAILED or DISCONNECTED state
        if (connection.getState() != FTPConnectionState.FAILED && 
            connection.getState() != FTPConnectionState.DISCONNECTED) {
            logger.debug("Cannot reconnect connection {} in state: {}", 
                    new Object[] { connection.getId(), connection.getState() });
            return false;
        }
        
        // Check if we've reached the maximum reconnect attempts
        if (connection.getReconnectAttempts() >= MAX_RECONNECT_ATTEMPTS) {
            logger.warn("Maximum reconnect attempts ({}) reached for connection {}",
                    new Object[] { MAX_RECONNECT_ATTEMPTS, connection.getId() });
            
            // Throw an exception to indicate a terminal failure condition
            throw new FTPConnectionException(
                    FTPConnectionException.ErrorType.CONNECTION_ERROR,
                    connection,
                    "Maximum reconnection attempts (" + MAX_RECONNECT_ATTEMPTS + ") exceeded");
        }
        
        // Set state to reconnecting
        connection.setState(FTPConnectionState.RECONNECTING);
        
        try {
            // Close any existing client
            if (connection.getClient() != null) {
                closeClient(connection.getClient());
                connection.setClient(null);
            }
            
            // Calculate backoff time based on attempt count
            int attemptCount = connection.incrementReconnectAttempts();
            long backoffMs = RECONNECT_BACKOFF_MS[Math.min(attemptCount - 1, RECONNECT_BACKOFF_MS.length - 1)];
            
            logger.debug("Reconnect attempt {} for connection {} with backoff: {} ms",
                    new Object[] { attemptCount, connection.getId(), backoffMs });
            
            // Wait for backoff time
            if (backoffMs > 0) {
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Reconnection interrupted for connection {}", connection.getId());
                    connection.setState(FTPConnectionState.FAILED);
                    return false;
                }
            }
            
            // Establish a new FTP connection
            FTPClient client;
            try {
                client = createAndConnectClient(connection);
            } catch (IOException e) {
                // Update state and record the error
                connection.setState(FTPConnectionState.FAILED);
                connection.setLastErrorMessage("Reconnection failed: " + e.getMessage());
                
                logger.error("Failed to reconnect connection {}: {}", 
                        new Object[] { connection.getId(), e.getMessage() });
                
                // For timeout and connection refused errors, we want to allow retries
                if (attemptCount < MAX_RECONNECT_ATTEMPTS && 
                        (e.getMessage().contains("timed out") || 
                         e.getMessage().contains("refused") ||
                         e.getMessage().contains("reset"))) {
                    logger.debug("Temporary connection error, will retry later");
                    return false;
                }
                
                // For more serious errors, throw an exception
                FTPConnectionException.ErrorType errorType;
                if (e.getMessage().contains("login") || e.getMessage().contains("authentication")) {
                    errorType = FTPConnectionException.ErrorType.AUTHENTICATION_ERROR;
                } else if (e.getMessage().contains("timed out") || e.getMessage().contains("timeout")) {
                    errorType = FTPConnectionException.ErrorType.TIMEOUT_ERROR;
                } else {
                    errorType = FTPConnectionException.ErrorType.CONNECTION_ERROR;
                }
                
                throw new FTPConnectionException(
                        errorType,
                        connection,
                        "Failed to reconnect: " + e.getMessage(),
                        e);
            }
            
            connection.setClient(client);
            
            // Update connection state and metadata
            connection.setState(FTPConnectionState.CONNECTED);
            connection.markAsUsed();
            connection.markAsTested();
            
            // Reset reconnect attempts on successful connection
            connection.resetReconnectAttempts();
            
            logger.info("Successfully reconnected connection: {}", connection.getId());
            return true;
            
        } catch (IOException e) {
            // This catch handles createAndConnectClient exceptions not caught in the nested try/catch
            connection.setState(FTPConnectionState.FAILED);
            connection.setLastErrorMessage("Reconnection failed: " + e.getMessage());
            
            logger.error("Failed to reconnect connection {}: {}", 
                    new Object[] { connection.getId(), e.getMessage() });
            
            FTPConnectionException.ErrorType errorType = FTPConnectionException.ErrorType.CONNECTION_ERROR;
            throw new FTPConnectionException(
                    errorType,
                    connection,
                    "Failed to reconnect: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Closes a managed connection, updating its state and removing it from management.
     *
     * @param connection the connection to close
     */
    public void closeConnection(FTPConnection connection) {
        if (connection == null) {
            return;
        }
        
        // Update connection state
        connection.setState(FTPConnectionState.DISCONNECTING);
        
        try {
            // Close the FTP client
            closeClient(connection.getClient());
            
            // Update state and remove from managed connections
            connection.setState(FTPConnectionState.DISCONNECTED);
            connections.remove(connection.getId());
            
            logger.debug("Closed and removed managed connection: {}", connection.getId());
            
        } catch (Exception e) {
            logger.warn("Error closing connection {}: {}", 
                    new Object[] { connection.getId(), e.getMessage() });
            connection.setState(FTPConnectionState.FAILED);
            connection.setLastErrorMessage("Error during close: " + e.getMessage());
        }
    }
    
    /**
     * Closes an FTP client, handling all cleanup tasks.
     *
     * @param client the FTPClient to close
     */
    private void closeClient(FTPClient client) {
        if (client == null) {
            return;
        }
        
        try {
            if (client.isConnected()) {
                try {
                    client.logout();
                } catch (IOException e) {
                    logger.debug("Error during FTP logout: {}", new Object[] { e.getMessage() });
                }
                try {
                    client.disconnect();
                } catch (IOException e) {
                    logger.debug("Error during FTP disconnect: {}", new Object[] { e.getMessage() });
                }
            }
        } catch (Exception e) {
            logger.warn("Error closing FTP client: {}", new Object[] { e.getMessage() });
        }
    }
    
    /**
     * For backward compatibility, closes a raw FTPClient.
     *
     * @param client the FTPClient to close
     */
    public void closeConnection(FTPClient client) {
        closeClient(client);
    }

    /**
     * Gets a connection by ID.
     *
     * @param connectionId the connection ID
     * @return the connection, or null if not found
     */
    public FTPConnection getConnection(String connectionId) {
        return connections.get(connectionId);
    }
    
    /**
     * Gets the number of managed connections.
     *
     * @return the connection count
     */
    public int getConnectionCount() {
        return connections.size();
    }
    
    /**
     * Gets the number of connections in a specific state.
     *
     * @param state the state to count
     * @return the number of connections in that state
     */
    public int getConnectionCount(FTPConnectionState state) {
        return (int) connections.values().stream()
                .filter(conn -> conn.getState() == state)
                .count();
    }
    
    /**
     * For backward compatibility, creates a new FTP connection.
     *
     * @return a new FTPClient that has been connected and configured
     * @throws IOException if an error occurs while connecting
     */
    public FTPClient createConnection() throws IOException {
        try {
            FTPConnection connection = createManagedConnection();
            return connection.getClient();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Failed to create FTP connection", e);
        }
    }

    /**
     * Shuts down the connection manager and cleans up resources.
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            logger.info("Shutting down FTP connection manager");
            
            // Cancel scheduled tasks
            if (connectionMaintenanceTask != null) {
                connectionMaintenanceTask.cancel(true);
                connectionMaintenanceTask = null;
            }
            
            // Shutdown scheduler
            if (scheduler != null) {
                try {
                    scheduler.shutdown();
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    scheduler.shutdownNow();
                } finally {
                    scheduler = null;
                }
            }
            
            // Close all managed connections
            for (FTPConnection connection : connections.values()) {
                try {
                    closeConnection(connection);
                } catch (Exception e) {
                    logger.warn("Error closing connection during shutdown: {}", 
                            new Object[] { connection.getId() });
                }
            }
            
            connections.clear();
            
            // Clear any sensitive information
            if (this.credentialHandler != null) {
                this.credentialHandler.clearCredentials();
            }
            
            initialized.set(false);
        }
    }
    
    /**
     * Gets the secure credential handler for this connection manager.
     * 
     * @return the credential handler
     */
    protected SecureCredentialHandler getCredentialHandler() {
        return this.credentialHandler;
    }
    
    /**
     * Gets the connection configuration.
     * 
     * @return the configuration
     */
    public FTPConnectionConfig getConfig() {
        return config;
    }
}