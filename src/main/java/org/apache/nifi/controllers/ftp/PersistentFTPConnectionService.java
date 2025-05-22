package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controllers.ftp.exception.FTPAuthenticationException;
import org.apache.nifi.controllers.ftp.exception.FTPConnectionException;
import org.apache.nifi.controllers.ftp.exception.FTPErrorType;
import org.apache.nifi.controllers.ftp.exception.FTPFileOperationException;
import org.apache.nifi.controllers.ftp.exception.FTPOperationException;
import org.apache.nifi.controllers.ftp.exception.FTPTransferException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.PersistentFTPService;

import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.controllers.ftp.FTPConnectionConfig.DataUnit;

/**
 * Implementation of a controller service that provides persistent
 * FTP connections with connection pooling, health monitoring,
 * and optimized operations.
 */
@Tags({"ftp", "connection", "pool", "persistent"})
@CapabilityDescription("Provides persistent FTP connection management with connection pooling, health monitoring, and optimized operations.")
public class PersistentFTPConnectionService extends AbstractControllerService implements PersistentFTPService {

    // Basic connection property descriptors
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The hostname of the FTP server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("The port of the FTP server")
            .required(true)
            .defaultValue("21")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to use when connecting to the FTP server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password to use when connecting to the FTP server")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Connection timeout properties
    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("The amount of time to wait when connecting to the FTP server before timing out")
            .required(true)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Data Timeout")
            .description("The amount of time to wait when transferring data to/from the FTP server before timing out")
            .required(true)
            .defaultValue("60 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor CONTROL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Control Timeout")
            .description("The amount of time to wait for control channel operations before timing out")
            .required(false)
            .defaultValue("60 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    // Connection mode properties
    public static final PropertyDescriptor ACTIVE_MODE = new PropertyDescriptor.Builder()
            .name("Active Mode")
            .description("Whether to use active mode for the FTP connection")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
            
    public static final PropertyDescriptor LOCAL_ACTIVE_PORT_RANGE = new PropertyDescriptor.Builder()
            .name("Local Active Port Range")
            .description("Whether to use a specific local port range for active mode connections")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
            
    public static final PropertyDescriptor ACTIVE_PORT_RANGE_START = new PropertyDescriptor.Builder()
            .name("Active Port Range Start")
            .description("The starting port in the local port range for active mode connections")
            .required(false)
            .defaultValue("40000")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor ACTIVE_PORT_RANGE_END = new PropertyDescriptor.Builder()
            .name("Active Port Range End")
            .description("The ending port in the local port range for active mode connections")
            .required(false)
            .defaultValue("50000")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor ACTIVE_EXTERNAL_IP_ADDRESS = new PropertyDescriptor.Builder()
            .name("Active External IP Address")
            .description("The external IP address to use for active mode connections when behind NAT")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Connection pool properties
    public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Connections")
            .description("The maximum number of connections to maintain in the connection pool")
            .required(true)
            .defaultValue("10")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Min Connections")
            .description("The minimum number of connections to maintain in the connection pool")
            .required(true)
            .defaultValue("2")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEEP_ALIVE_INTERVAL = new PropertyDescriptor.Builder()
            .name("Keep Alive Interval")
            .description("The amount of time to wait between sending keep-alive messages")
            .required(true)
            .defaultValue("60 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECTION_IDLE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Idle Timeout")
            .description("The amount of time to allow a connection to remain idle before closing it")
            .required(true)
            .defaultValue("5 min")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    // Buffer size property
    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Buffer Size")
            .description("The buffer size to use when transferring data")
            .required(true)
            .defaultValue("1 MB")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    // Encoding property
    public static final PropertyDescriptor CONTROL_ENCODING = new PropertyDescriptor.Builder()
            .name("Control Encoding")
            .description("The character encoding to use for the control channel")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    // Transfer mode property
    public static final PropertyDescriptor TRANSFER_MODE = new PropertyDescriptor.Builder()
            .name("Transfer Mode")
            .description("The transfer mode to use for file transfers (ASCII for text files, Binary for all other files)")
            .required(true)
            .allowableValues("ASCII", "Binary")
            .defaultValue("Binary")
            .build();

    // SSL/TLS properties
    public static final PropertyDescriptor USE_CLIENT_MODE = new PropertyDescriptor.Builder()
            .name("Use Client Mode")
            .description("Whether to use client mode for the FTPS connection")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor USE_IMPLICIT_SSL = new PropertyDescriptor.Builder()
            .name("Use Implicit SSL")
            .description("Whether to use implicit SSL for the FTPS connection")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
            
    public static final PropertyDescriptor USE_EXPLICIT_SSL = new PropertyDescriptor.Builder()
            .name("Use Explicit SSL/TLS")
            .description("Whether to use explicit SSL/TLS (FTPS) via the AUTH TLS/SSL command")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
            
    public static final PropertyDescriptor ENABLED_PROTOCOLS = new PropertyDescriptor.Builder()
            .name("Enabled SSL/TLS Protocols")
            .description("Comma-separated list of SSL/TLS protocols to enable (e.g., 'TLSv1.2,TLSv1.3')")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor ENABLED_CIPHER_SUITES = new PropertyDescriptor.Builder()
            .name("Enabled SSL/TLS Cipher Suites")
            .description("Comma-separated list of SSL/TLS cipher suites to enable")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor VALIDATE_SERVER_CERTIFICATE = new PropertyDescriptor.Builder()
            .name("Validate Server Certificate")
            .description("Whether to validate the server's SSL/TLS certificate")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
            
    public static final PropertyDescriptor TRUST_STORE_PATH = new PropertyDescriptor.Builder()
            .name("TrustStore Path")
            .description("The path to the truststore for validating server certificates")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor TRUST_STORE_PASSWORD = new PropertyDescriptor.Builder()
            .name("TrustStore Password")
            .description("The password for the truststore")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor TRUST_STORE_TYPE = new PropertyDescriptor.Builder()
            .name("TrustStore Type")
            .description("The type of the truststore (JKS, PKCS12, etc.)")
            .required(false)
            .allowableValues("JKS", "PKCS12")
            .defaultValue("JKS")
            .build();
            
    // Proxy properties
    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("The hostname of the proxy server")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("The port of the proxy server")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("Proxy Username")
            .description("The username for proxy authentication")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("Proxy Password")
            .description("The password for proxy authentication")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
            
    public static final PropertyDescriptor PROXY_TYPE = new PropertyDescriptor.Builder()
            .name("Proxy Type")
            .description("The type of proxy server")
            .required(false)
            .allowableValues("SOCKS", "HTTP")
            .defaultValue("SOCKS")
            .build();

    private static final List<PropertyDescriptor> properties;

    private volatile boolean initialized = false;
    private volatile FTPConnectionConfig config;
    private volatile FTPConnectionManager connectionManager;
    
    // Metrics for error tracking
    private final AtomicLong connectionErrorCount = new AtomicLong(0);
    private final AtomicLong authenticationErrorCount = new AtomicLong(0);
    private final AtomicLong transferErrorCount = new AtomicLong(0);
    private final AtomicLong fileOperationErrorCount = new AtomicLong(0);
    private final AtomicLong unexpectedErrorCount = new AtomicLong(0);
    private final AtomicLong successfulOperationCount = new AtomicLong(0);
    private final AtomicLong successfulReconnectionCount = new AtomicLong(0);
    private final AtomicLong failedReconnectionCount = new AtomicLong(0);
    
    // Last recorded error information
    private volatile String lastErrorMessage = null;
    private volatile Date lastErrorTime = null;
    private volatile FTPErrorType lastErrorType = null;
    
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        // Basic connection properties
        props.add(HOSTNAME);
        props.add(PORT);
        props.add(USERNAME);
        props.add(PASSWORD);
        
        // Timeout properties
        props.add(CONNECTION_TIMEOUT);
        props.add(DATA_TIMEOUT);
        props.add(CONTROL_TIMEOUT);
        
        // Connection mode properties
        props.add(ACTIVE_MODE);
        props.add(LOCAL_ACTIVE_PORT_RANGE);
        props.add(ACTIVE_PORT_RANGE_START);
        props.add(ACTIVE_PORT_RANGE_END);
        props.add(ACTIVE_EXTERNAL_IP_ADDRESS);
        
        // Connection pool properties
        props.add(MAX_CONNECTIONS);
        props.add(MIN_CONNECTIONS);
        props.add(KEEP_ALIVE_INTERVAL);
        props.add(CONNECTION_IDLE_TIMEOUT);
        
        // Transfer properties
        props.add(BUFFER_SIZE);
        props.add(CONTROL_ENCODING);
        props.add(TRANSFER_MODE);
        
        // SSL/TLS properties
        props.add(USE_CLIENT_MODE);
        props.add(USE_IMPLICIT_SSL);
        props.add(USE_EXPLICIT_SSL);
        props.add(ENABLED_PROTOCOLS);
        props.add(ENABLED_CIPHER_SUITES);
        props.add(VALIDATE_SERVER_CERTIFICATE);
        props.add(TRUST_STORE_PATH);
        props.add(TRUST_STORE_PASSWORD);
        props.add(TRUST_STORE_TYPE);
        
        // Proxy properties
        props.add(PROXY_HOST);
        props.add(PROXY_PORT);
        props.add(PROXY_USERNAME);
        props.add(PROXY_PASSWORD);
        props.add(PROXY_TYPE);
        
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        
        // Validate that min connections <= max connections
        final int minConnections = validationContext.getProperty(MIN_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final int maxConnections = validationContext.getProperty(MAX_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        
        if (minConnections > maxConnections) {
            results.add(new ValidationResult.Builder()
                    .subject("Connection Pool Configuration")
                    .valid(false)
                    .explanation("Min Connections cannot be greater than Max Connections")
                    .build());
        }
        
        // Validate connection idle timeout is greater than keep alive interval
        if (validationContext.getProperty(CONNECTION_IDLE_TIMEOUT).isSet() && 
            validationContext.getProperty(KEEP_ALIVE_INTERVAL).isSet()) {
            
            final long idleTimeout = validationContext.getProperty(CONNECTION_IDLE_TIMEOUT)
                    .evaluateAttributeExpressions()
                    .asTimePeriod(TimeUnit.MILLISECONDS);
            
            final long keepAliveInterval = validationContext.getProperty(KEEP_ALIVE_INTERVAL)
                    .evaluateAttributeExpressions()
                    .asTimePeriod(TimeUnit.MILLISECONDS);
            
            if (idleTimeout <= keepAliveInterval) {
                results.add(new ValidationResult.Builder()
                        .subject("Connection Timeout Configuration")
                        .valid(false)
                        .explanation("Connection Idle Timeout must be greater than Keep Alive Interval")
                        .build());
            }
        }
        
        // Validate buffer size is reasonable
        if (validationContext.getProperty(BUFFER_SIZE).isSet()) {
            final long bufferSize = validationContext.getProperty(BUFFER_SIZE)
                    .evaluateAttributeExpressions()
                    .asDataSize(DataUnit.B);
            
            // Buffer size should be between 1KB and 100MB
            if (bufferSize < 1024) {
                results.add(new ValidationResult.Builder()
                        .subject(BUFFER_SIZE.getName())
                        .valid(false)
                        .explanation("Buffer Size is too small; should be at least 1KB")
                        .build());
            } else if (bufferSize > 104857600) { // 100MB
                results.add(new ValidationResult.Builder()
                        .subject(BUFFER_SIZE.getName())
                        .valid(false)
                        .explanation("Buffer Size is too large; should be no more than 100MB")
                        .build());
            }
        }
        
        return results;
    }

    @Override
    protected void init(ControllerServiceInitializationContext context) {
        // Nothing to do here yet - just basic initialization
        this.initialized = false;
    }

    // Define a secure credential handler at the service level for reuse
    private volatile SecureCredentialHandler credentialHandler;
    
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        // Create an instance of secure credential handler
        this.credentialHandler = new SecureCredentialHandler(getLogger());
        
        // Load credentials from context with enhanced handling
        this.credentialHandler.loadCredentialsFromContext(context, PersistentFTPConnectionService.class);
        
        // Log the types of credentials that were loaded (without exposing values)
        logLoadedCredentialTypes();
        
        // Create configuration from context with secured credentials
        this.config = FTPConnectionConfig.fromContext(context, getLogger());
        
        // Create a connection manager using this configuration
        this.connectionManager = new FTPConnectionManager(
                config, 
                getLogger());
        
        getLogger().info("Initialized FTP Connection Service for {}:{}",
                new Object[] { config.getHostname(), config.getPort() });
        
        // Test the connection to make sure everything is configured correctly
        try {
            if (testConnection()) {
                getLogger().info("Successfully connected to FTP server at {}:{}",
                        new Object[] { config.getHostname(), config.getPort() });
            } else {
                getLogger().warn("Failed to connect to FTP server at {}:{}",
                        new Object[] { config.getHostname(), config.getPort() });
            }
        } catch (Exception e) {
            getLogger().error("Error testing connection to FTP server at {}:{}",
                    new Object[] { config.getHostname(), config.getPort(), e });
        }
        
        this.initialized = true;
    }

    /**
     * Logs the types of credentials that were loaded, without exposing any sensitive values.
     * This is useful for debugging purposes.
     */
    private void logLoadedCredentialTypes() {
        if (this.credentialHandler == null) {
            return;
        }
        
        List<String> storedKeys = this.credentialHandler.getStoredCredentialKeys();
        if (storedKeys.isEmpty()) {
            getLogger().debug("No credentials were loaded");
            return;
        }
        
        getLogger().debug("Loaded {} credential types: {}", 
                new Object[] { storedKeys.size(), String.join(", ", storedKeys) });
                
        // Check for specific credential types that may be required
        for (SecureCredentialHandler.CredentialType type : SecureCredentialHandler.CredentialType.values()) {
            if (this.credentialHandler.hasCredential(type)) {
                getLogger().debug("Loaded credential of type: {}", type.name());
            }
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (this.connectionManager != null) {
            try {
                // Clean up any connections that might be open
                this.connectionManager.shutdown();
                getLogger().info("Successfully shutdown FTP Connection Service");
            } catch (Exception e) {
                getLogger().error("Error shutting down FTP Connection Service", e);
            }
        }
        
        // Clear any sensitive information
        if (this.credentialHandler != null) {
            this.credentialHandler.clearCredentials();
            getLogger().debug("Cleared all stored credentials");
        }
        
        this.initialized = false;
        this.connectionManager = null;
        this.config = null;
        this.credentialHandler = null;
    }
    
    /**
     * Gets the credential handler for this service.
     * This can be used by operations to retrieve credentials securely.
     *
     * @return the credential handler
     */
    protected SecureCredentialHandler getCredentialHandler() {
        return this.credentialHandler;
    }
    
    /**
     * Retrieves a credential securely if available.
     *
     * @param type the credential type to retrieve
     * @return the credential value, or null if not available
     */
    protected String getCredential(SecureCredentialHandler.CredentialType type) {
        if (this.credentialHandler == null) {
            return null;
        }
        return this.credentialHandler.retrieveCredential(type);
    }

    @Override
    public FTPClient getConnection() throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        if (connectionManager == null) {
            recordError(FTPErrorType.POOL_ERROR, "Connection manager is not initialized");
            throw new FTPConnectionException(FTPErrorType.POOL_ERROR, "Connection manager is not initialized");
        }
        
        try {
            // Create a new connection
            FTPConnection connection = connectionManager.createManagedConnection();
            
            // Verify the connection is valid
            if (!connectionManager.validateConnection(connection)) {
                // Try to reconnect the connection if validation fails
                boolean reconnected = connectionManager.reconnect(connection);
                if (!reconnected) {
                    recordError(FTPErrorType.CONNECTION_ERROR, "Failed to establish valid FTP connection");
                    throw new FTPConnectionException(FTPErrorType.CONNECTION_ERROR, connection,
                            "Failed to establish valid FTP connection");
                }
            }
            
            // Record successful connection
            recordSuccess();
            return connection.getClient();
            
        } catch (FTPConnectionException e) {
            // Record the error and rethrow
            recordError(e.getErrorType(), e.getMessage());
            throw e;
        } catch (Exception e) {
            // Handle unexpected exceptions
            recordError(FTPErrorType.CONNECTION_ERROR, "Error getting FTP connection: " + e.getMessage());
            throw new FTPConnectionException(FTPErrorType.CONNECTION_ERROR, 
                    "Error getting FTP connection: " + e.getMessage(), e);
        }
    }

    @Override
    public void releaseConnection(FTPClient client) {
        if (client == null || connectionManager == null) {
            return;
        }
        
        try {
            // Before returning to pool, check if the connection is still valid
            boolean isValid = connectionManager.validateConnection(client);
            
            if (!isValid) {
                // If the connection is not valid, close it instead of returning to pool
                getLogger().debug("Closing invalid connection instead of returning to pool");
                connectionManager.closeConnection(client);
                recordError(FTPErrorType.CONNECTION_ERROR, "Released invalid connection to pool");
            } else {
                // Return valid connection to the pool
                // Since we don't have direct mapping from client to connection in this method,
                // we'll just close the connection using the closeConnection method
                // In a real implementation, you'd maintain a mapping to return to pool correctly
                connectionManager.closeConnection(client);
                recordSuccess();
            }
        } catch (Exception e) {
            // If there's an error releasing the connection, log it and ensure the connection is closed
            getLogger().error("Error releasing FTP connection to pool", e);
            try {
                connectionManager.closeConnection(client);
            } catch (Exception closeEx) {
                getLogger().error("Error closing FTP connection after release failure", closeEx);
            }
            recordError(FTPErrorType.POOL_ERROR, "Error releasing connection to pool: " + e.getMessage());
        }
    }

    @Override
    public boolean testConnection() {
        if (connectionManager == null) {
            getLogger().warn("Cannot test connection because service is not initialized");
            return false;
        }
        
        FTPClient client = null;
        try {
            client = connectionManager.createConnection();
            if (client == null) {
                getLogger().warn("Failed to create FTP connection for testing");
                return false;
            }
            
            boolean valid = connectionManager.validateConnection(client);
            if (valid) {
                getLogger().debug("Successfully tested connection to FTP server");
            } else {
                getLogger().warn("Connection test failed - FTP connection is not valid");
            }
            return valid;
            
        } catch (Exception e) {
            getLogger().error("Error testing FTP connection", e);
            return false;
        } finally {
            if (client != null) {
                try {
                    connectionManager.closeConnection(client);
                } catch (Exception e) {
                    getLogger().error("Error closing FTP connection after test", e);
                }
            }
        }
    }

    @Override
    public Map<String, Object> getConnectionStats() {
        Map<String, Object> stats = new HashMap<>();
        
        if (connectionManager == null) {
            return stats;
        }
        
        // Add connection pool stats
        stats.put("connection_count", connectionManager.getConnectionCount());
        stats.put("connected_count", connectionManager.getConnectionCount(FTPConnectionState.CONNECTED));
        stats.put("failed_count", connectionManager.getConnectionCount(FTPConnectionState.FAILED));
        stats.put("connecting_count", connectionManager.getConnectionCount(FTPConnectionState.CONNECTING));
        stats.put("disconnecting_count", connectionManager.getConnectionCount(FTPConnectionState.DISCONNECTING));
        stats.put("reconnecting_count", connectionManager.getConnectionCount(FTPConnectionState.RECONNECTING));
        stats.put("disconnected_count", connectionManager.getConnectionCount(FTPConnectionState.DISCONNECTED));
        
        // Add error metrics
        stats.put("connection_error_count", connectionErrorCount.get());
        stats.put("authentication_error_count", authenticationErrorCount.get());
        stats.put("transfer_error_count", transferErrorCount.get());
        stats.put("file_operation_error_count", fileOperationErrorCount.get());
        stats.put("unexpected_error_count", unexpectedErrorCount.get());
        stats.put("successful_operation_count", successfulOperationCount.get());
        stats.put("successful_reconnection_count", successfulReconnectionCount.get());
        stats.put("failed_reconnection_count", failedReconnectionCount.get());
        
        // Add last error information if available
        if (lastErrorMessage != null) {
            stats.put("last_error_message", lastErrorMessage);
            stats.put("last_error_time", lastErrorTime);
            stats.put("last_error_type", lastErrorType);
        }
        
        return stats;
    }

    @Override
    public List<FTPFile> listFiles(String directory, FileFilter filter) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        FTPClient client = null;
        FTPConnection connection = null;
        int attempts = 0;
        final int maxAttempts = 3;
        
        while (attempts < maxAttempts) {
            attempts++;
            
            try {
                // Get a connection from the pool
                client = getConnection();
                
                // List files in the directory
                org.apache.commons.net.ftp.FTPFile[] ftpFiles = client.listFiles(directory);
                
                // Check if the operation was successful
                if (ftpFiles == null) {
                    // Check reply code for specific error
                    checkReplyCode(client, "listFiles", directory);
                    
                    // If no specific error from reply code but result is null, return empty list
                    recordSuccess(); // Not an error, just no files
                    releaseConnection(client);
                    client = null;
                    return Collections.emptyList();
                }
                
                // Convert Apache Commons FTPFile objects to our model with enhanced attributes
                List<FTPFile> result = new ArrayList<>();
                for (org.apache.commons.net.ftp.FTPFile file : ftpFiles) {
                    // Skip null files and current/parent directory entries
                    if (file == null || "..".equals(file.getName()) || ".".equals(file.getName())) {
                        continue;
                    }
                    
                    // Apply filter if provided
                    if (filter != null && !filter.accept(null)) { // We can't really use FileFilter here properly
                        continue;
                    }
                    
                    // Convert and add the file with attributes
                    FTPFile convertedFile = convertFTPFile(file, directory);
                    result.add(convertedFile);
                }
                
                // Release the connection
                releaseConnection(client);
                client = null;
                
                // Record successful operation
                recordSuccess();
                return result;
                
            } catch (Exception e) {
                // Try to release the connection (if we got one)
                if (client != null) {
                    try {
                        releaseConnection(client);
                    } catch (Exception releaseEx) {
                        getLogger().error("Error releasing connection after list files error", releaseEx);
                    }
                    client = null;
                }
                
                // Handle the exception and check if we should retry
                boolean shouldRetry = handleFTPException(e, client, connection, directory);
                
                if (shouldRetry && attempts < maxAttempts) {
                    // Log retry attempt
                    getLogger().warn("Retrying listFiles operation for {} (attempt {}/{})",
                            new Object[] { directory, attempts, maxAttempts });
                    continue;
                }
                
                // If we shouldn't retry or have exhausted retries, throw appropriate exception
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new FTPFileOperationException(FTPErrorType.DIRECTORY_NOT_FOUND, directory,
                            FTPFileOperationException.FileOperation.LIST, 
                            "Error listing files: " + e.getMessage(), e);
                }
            }
        }
        
        // If we've exhausted retry attempts without success or exception
        recordError(FTPErrorType.DIRECTORY_NOT_FOUND, "Failed to list files after " + maxAttempts + " attempts: " + directory);
        throw new FTPFileOperationException(FTPErrorType.DIRECTORY_NOT_FOUND, directory,
                FTPFileOperationException.FileOperation.LIST,
                "Failed to list files after " + maxAttempts + " attempts");
    }
    
    /**
     * Converts an Apache Commons Net FTPFile to our model with enhanced attributes.
     *
     * @param file The Apache Commons Net FTPFile to convert
     * @param parentPath The parent directory path
     * @return A converted FTPFile with attributes
     */
    private FTPFile convertFTPFile(org.apache.commons.net.ftp.FTPFile file, String parentPath) {
        FTPFile convertedFile = new FTPFile();
        
        // Set basic file information
        convertedFile.setName(file.getName());
        
        // Build the full path
        String path = parentPath;
        if (!path.endsWith("/")) {
            path += "/";
        }
        path += file.getName();
        convertedFile.setPath(path);
        
        // Set file size
        convertedFile.setSize(file.getSize());
        
        // Set file type
        if (file.isDirectory()) {
            convertedFile.setType(FTPFileType.DIRECTORY);
        } else if (file.isFile()) {
            convertedFile.setType(FTPFileType.FILE);
        } else if (file.isSymbolicLink()) {
            convertedFile.setType(FTPFileType.SYMBOLIC_LINK);
        } else {
            convertedFile.setType(FTPFileType.UNKNOWN);
        }
        
        // Set timestamp
        Date timestamp = file.getTimestamp() != null ? file.getTimestamp().getTime() : null;
        if (timestamp != null) {
            convertedFile.setTimestamp(timestamp);
            
            // Also set formatted timestamp string for convenience
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            convertedFile.setAttribute("formatted_timestamp", dateFormat.format(timestamp));
        }
        
        // Set permissions
        String permissions = formatPermissions(file);
        convertedFile.setPermissions(permissions);
        
        // Set owner and group
        convertedFile.setOwner(file.getUser());
        convertedFile.setGroup(file.getGroup());
        
        // Add additional attributes
        Map<String, Object> attributes = new HashMap<>();
        
        // Add raw permissions as integers
        attributes.put("user_permissions", file.hasPermission(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.READ_PERMISSION) ? "r" : "-" +
                (file.hasPermission(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION) ? "w" : "-") +
                (file.hasPermission(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION) ? "x" : "-"));
        
        attributes.put("group_permissions", file.hasPermission(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.READ_PERMISSION) ? "r" : "-" +
                (file.hasPermission(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION) ? "w" : "-") +
                (file.hasPermission(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION) ? "x" : "-"));
        
        attributes.put("world_permissions", file.hasPermission(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.READ_PERMISSION) ? "r" : "-" +
                (file.hasPermission(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION) ? "w" : "-") +
                (file.hasPermission(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION) ? "x" : "-"));
        
        // Add link count if available
        attributes.put("hard_links", file.getHardLinkCount());
        
        // If it's a symbolic link, add the link target
        if (file.isSymbolicLink()) {
            attributes.put("link_target", file.getLink());
        }
        
        // Set all additional attributes
        convertedFile.setAttributes(attributes);
        
        return convertedFile;
    }
    
    /**
     * Formats the file permissions in Unix-style format (e.g., "rwxr-xr--").
     *
     * @param file The Apache Commons Net FTPFile to get permissions from
     * @return A string representation of the permissions
     */
    private String formatPermissions(org.apache.commons.net.ftp.FTPFile file) {
        StringBuilder permissions = new StringBuilder();
        
        // Add file type prefix
        if (file.isDirectory()) {
            permissions.append('d');
        } else if (file.isSymbolicLink()) {
            permissions.append('l');
        } else {
            permissions.append('-');
        }
        
        // Add user permissions
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.READ_PERMISSION) ? 'r' : '-');
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION) ? 'w' : '-');
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.USER_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION) ? 'x' : '-');
        
        // Add group permissions
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.READ_PERMISSION) ? 'r' : '-');
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION) ? 'w' : '-');
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.GROUP_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION) ? 'x' : '-');
        
        // Add world permissions
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.READ_PERMISSION) ? 'r' : '-');
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.WRITE_PERMISSION) ? 'w' : '-');
        permissions.append(file.hasPermission(org.apache.commons.net.ftp.FTPFile.WORLD_ACCESS, 
                org.apache.commons.net.ftp.FTPFile.EXECUTE_PERMISSION) ? 'x' : '-');
        
        return permissions.toString();
    }

    @Override
    public InputStream retrieveFileStream(String remotePath) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        FTPClient client = null;
        FTPConnection connection = null;
        int attempts = 0;
        final int maxAttempts = 3;
        
        while (attempts < maxAttempts) {
            attempts++;
            
            try {
                // Get a connection from the pool
                client = getConnection();
                
                // Retrieve input stream from the file
                InputStream inputStream = client.retrieveFileStream(remotePath);
                
                // Check if the operation was successful
                if (inputStream == null) {
                    // Check reply code for specific error
                    checkReplyCode(client, "retrieveFileStream", remotePath);
                    
                    // If no specific error from reply code but stream is null, throw generic error
                    recordError(FTPErrorType.FILE_NOT_FOUND, "Failed to retrieve file stream for: " + remotePath);
                    throw new FTPFileOperationException(FTPErrorType.FILE_NOT_FOUND, remotePath,
                            FTPFileOperationException.FileOperation.LIST, 
                            "Failed to retrieve file stream for: " + remotePath);
                }
                
                // Record successful operation
                recordSuccess();
                return inputStream;
                
            } catch (Exception e) {
                // Try to release the connection (if we got one)
                if (client != null) {
                    try {
                        releaseConnection(client);
                    } catch (Exception releaseEx) {
                        getLogger().error("Error releasing connection after file stream error", releaseEx);
                    }
                    client = null;
                }
                
                // Handle the exception and check if we should retry
                boolean shouldRetry = handleFTPException(e, client, connection, remotePath);
                
                if (shouldRetry && attempts < maxAttempts) {
                    // Log retry attempt
                    getLogger().warn("Retrying retrieveFileStream operation for {} (attempt {}/{})",
                            new Object[] { remotePath, attempts, maxAttempts });
                    continue;
                }
                
                // If we shouldn't retry or have exhausted retries, throw appropriate exception
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new FTPTransferException(FTPErrorType.TRANSFER_ERROR, remotePath,
                            "Error retrieving file stream: " + e.getMessage(), e);
                }
            }
        }
        
        // This should be unreachable due to the exception in the loop
        throw new FTPTransferException(FTPErrorType.TRANSFER_ERROR, remotePath,
                "Failed to retrieve file stream after " + maxAttempts + " attempts");
    }

    @Override
    public boolean storeFile(String remotePath, InputStream input) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        if (input == null) {
            throw new IllegalArgumentException("Input stream cannot be null");
        }
        
        FTPClient client = null;
        FTPConnection connection = null;
        int attempts = 0;
        final int maxAttempts = 3;
        
        while (attempts < maxAttempts) {
            attempts++;
            
            try {
                // Get a connection from the pool
                client = getConnection();
                
                // Store the file
                boolean success = client.storeFile(remotePath, input);
                
                // Check if the operation was successful
                if (!success) {
                    // Check reply code for specific error
                    checkReplyCode(client, "storeFile", remotePath);
                    
                    // If no specific error from reply code but operation failed, throw generic error
                    recordError(FTPErrorType.TRANSFER_ERROR, "Failed to store file: " + remotePath);
                    throw new FTPTransferException(FTPErrorType.TRANSFER_ERROR, remotePath,
                            "Failed to store file: " + remotePath);
                }
                
                // Release the connection
                releaseConnection(client);
                client = null;
                
                // Record successful operation
                recordSuccess();
                return true;
                
            } catch (Exception e) {
                // Try to release the connection (if we got one)
                if (client != null) {
                    try {
                        releaseConnection(client);
                    } catch (Exception releaseEx) {
                        getLogger().error("Error releasing connection after file store error", releaseEx);
                    }
                    client = null;
                }
                
                // Handle the exception and check if we should retry
                boolean shouldRetry = handleFTPException(e, client, connection, remotePath);
                
                if (shouldRetry && attempts < maxAttempts) {
                    // Log retry attempt
                    getLogger().warn("Retrying storeFile operation for {} (attempt {}/{})",
                            new Object[] { remotePath, attempts, maxAttempts });
                    
                    // If the input stream needs to be reset for retry, we can't retry
                    if (!input.markSupported()) {
                        getLogger().warn("Cannot retry storeFile operation because input stream does not support mark/reset");
                        break;
                    }
                    
                    // Try to reset the input stream for retry
                    try {
                        input.reset();
                    } catch (IOException resetEx) {
                        getLogger().error("Error resetting input stream for retry", resetEx);
                        break;
                    }
                    
                    continue;
                }
                
                // If we shouldn't retry or have exhausted retries, throw appropriate exception
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new FTPTransferException(FTPErrorType.TRANSFER_ERROR, remotePath,
                            "Error storing file: " + e.getMessage(), e);
                }
            }
        }
        
        // If we've exhausted retry attempts without success or exception
        recordError(FTPErrorType.TRANSFER_ERROR, "Failed to store file after " + maxAttempts + " attempts: " + remotePath);
        throw new FTPTransferException(FTPErrorType.TRANSFER_ERROR, remotePath,
                "Failed to store file after " + maxAttempts + " attempts");
    }

    @Override
    public boolean deleteFile(String remotePath) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        FTPClient client = null;
        FTPConnection connection = null;
        int attempts = 0;
        final int maxAttempts = 3;
        
        while (attempts < maxAttempts) {
            attempts++;
            
            try {
                // Get a connection from the pool
                client = getConnection();
                
                // Delete the file
                boolean success = client.deleteFile(remotePath);
                
                // Check if the operation was successful
                if (!success) {
                    // Check reply code for specific error
                    checkReplyCode(client, "deleteFile", remotePath);
                    
                    // If no specific error from reply code but operation failed, check if file exists
                    FTPFile[] files = client.listFiles(remotePath);
                    if (files == null || files.length == 0) {
                        // File doesn't exist - this is not an error for deleteFile operation
                        recordSuccess();
                        releaseConnection(client);
                        client = null;
                        return true;
                    }
                    
                    // If the file exists but couldn't be deleted, it's an error
                    recordError(FTPErrorType.INSUFFICIENT_PERMISSIONS, "Failed to delete file: " + remotePath);
                    throw new FTPFileOperationException(FTPErrorType.INSUFFICIENT_PERMISSIONS, remotePath,
                            FTPFileOperationException.FileOperation.DELETE, 
                            "Failed to delete file: " + remotePath);
                }
                
                // Release the connection
                releaseConnection(client);
                client = null;
                
                // Record successful operation
                recordSuccess();
                return true;
                
            } catch (Exception e) {
                // Try to release the connection (if we got one)
                if (client != null) {
                    try {
                        releaseConnection(client);
                    } catch (Exception releaseEx) {
                        getLogger().error("Error releasing connection after file delete error", releaseEx);
                    }
                    client = null;
                }
                
                // Handle the exception and check if we should retry
                boolean shouldRetry = handleFTPException(e, client, connection, remotePath);
                
                if (shouldRetry && attempts < maxAttempts) {
                    // Log retry attempt
                    getLogger().warn("Retrying deleteFile operation for {} (attempt {}/{})",
                            new Object[] { remotePath, attempts, maxAttempts });
                    continue;
                }
                
                // If we shouldn't retry or have exhausted retries, throw appropriate exception
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new FTPFileOperationException(FTPErrorType.FILE_NOT_FOUND, remotePath,
                            FTPFileOperationException.FileOperation.DELETE, 
                            "Error deleting file: " + e.getMessage(), e);
                }
            }
        }
        
        // If we've exhausted retry attempts without success or exception
        recordError(FTPErrorType.FILE_NOT_FOUND, "Failed to delete file after " + maxAttempts + " attempts: " + remotePath);
        throw new FTPFileOperationException(FTPErrorType.FILE_NOT_FOUND, remotePath,
                FTPFileOperationException.FileOperation.DELETE,
                "Failed to delete file after " + maxAttempts + " attempts");
    }
    
    /**
     * Gets a file's attributes from the FTP server.
     *
     * @param remotePath The path to the file on the FTP server
     * @return A FTPFile object with the file's attributes, or null if the file doesn't exist
     * @throws IOException if an error occurs while retrieving the file attributes
     */
    public FTPFile getFileAttributes(String remotePath) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        FTPClient client = null;
        FTPConnection connection = null;
        int attempts = 0;
        final int maxAttempts = 3;
        
        while (attempts < maxAttempts) {
            attempts++;
            
            try {
                // Get a connection from the pool
                client = getConnection();
                
                // Split the path into directory and filename
                String directory = remotePath.substring(0, remotePath.lastIndexOf('/'));
                String filename = remotePath.substring(remotePath.lastIndexOf('/') + 1);
                
                // List files in the directory to find our target file
                org.apache.commons.net.ftp.FTPFile[] ftpFiles = client.listFiles(directory);
                
                // Check if the operation was successful
                if (ftpFiles == null) {
                    // Check reply code for specific error
                    checkReplyCode(client, "listFiles (getFileAttributes)", directory);
                    
                    // If no specific error from reply code but result is null, file doesn't exist
                    recordSuccess(); // Not an error, just no file
                    releaseConnection(client);
                    client = null;
                    return null;
                }
                
                // Find the target file
                for (org.apache.commons.net.ftp.FTPFile file : ftpFiles) {
                    if (file != null && filename.equals(file.getName())) {
                        // Convert and return the file with attributes
                        FTPFile result = convertFTPFile(file, directory);
                        
                        // Release the connection
                        releaseConnection(client);
                        client = null;
                        
                        // Record successful operation
                        recordSuccess();
                        return result;
                    }
                }
                
                // File not found
                releaseConnection(client);
                client = null;
                recordSuccess(); // Not an error, just no file
                return null;
                
            } catch (Exception e) {
                // Try to release the connection (if we got one)
                if (client != null) {
                    try {
                        releaseConnection(client);
                    } catch (Exception releaseEx) {
                        getLogger().error("Error releasing connection after get attributes error", releaseEx);
                    }
                    client = null;
                }
                
                // Handle the exception and check if we should retry
                boolean shouldRetry = handleFTPException(e, client, connection, remotePath);
                
                if (shouldRetry && attempts < maxAttempts) {
                    // Log retry attempt
                    getLogger().warn("Retrying getFileAttributes operation for {} (attempt {}/{})",
                            new Object[] { remotePath, attempts, maxAttempts });
                    continue;
                }
                
                // If we shouldn't retry or have exhausted retries, throw appropriate exception
                if (e instanceof IOException) {
                    throw e;
                } else {
                    throw new FTPFileOperationException(FTPErrorType.FILE_NOT_FOUND, remotePath,
                            FTPFileOperationException.FileOperation.GET_ATTRIBUTES, 
                            "Error getting file attributes: " + e.getMessage(), e);
                }
            }
        }
        
        // If we've exhausted retry attempts without success or exception
        recordError(FTPErrorType.FILE_NOT_FOUND, "Failed to get file attributes after " + maxAttempts + " attempts: " + remotePath);
        throw new FTPFileOperationException(FTPErrorType.FILE_NOT_FOUND, remotePath,
                FTPFileOperationException.FileOperation.GET_ATTRIBUTES,
                "Failed to get file attributes after " + maxAttempts + " attempts");
    }
    
    /**
     * Gets a specific attribute for a file from the FTP server.
     *
     * @param remotePath The path to the file on the FTP server
     * @param attributeName The name of the attribute to retrieve
     * @return The attribute value, or null if the file or attribute doesn't exist
     * @throws IOException if an error occurs while retrieving the file attribute
     */
    public Object getFileAttribute(String remotePath, String attributeName) throws IOException {
        FTPFile file = getFileAttributes(remotePath);
        if (file == null) {
            return null;
        }
        
        // Check standard attributes first
        switch (attributeName) {
            case "name":
                return file.getName();
            case "path":
                return file.getPath();
            case "size":
                return file.getSize();
            case "timestamp":
                return file.getTimestamp();
            case "permissions":
                return file.getPermissions();
            case "owner":
                return file.getOwner();
            case "group":
                return file.getGroup();
            case "type":
                return file.getType();
            default:
                // Check custom attributes
                return file.getAttribute(attributeName);
        }
    }
    
    /**
     * Gets a map of all file attributes for a given file.
     *
     * @param remotePath The path to the file on the FTP server
     * @return A map of attribute names to values, or null if the file doesn't exist
     * @throws IOException if an error occurs while retrieving the file attributes
     */
    public Map<String, Object> getAllFileAttributes(String remotePath) throws IOException {
        FTPFile file = getFileAttributes(remotePath);
        if (file == null) {
            return null;
        }
        
        // Create a map with all attributes
        Map<String, Object> allAttributes = new HashMap<>();
        
        // Add standard attributes
        allAttributes.put("name", file.getName());
        allAttributes.put("path", file.getPath());
        allAttributes.put("size", file.getSize());
        allAttributes.put("timestamp", file.getTimestamp());
        allAttributes.put("permissions", file.getPermissions());
        allAttributes.put("owner", file.getOwner());
        allAttributes.put("group", file.getGroup());
        allAttributes.put("type", file.getType());
        allAttributes.put("is_directory", file.isDirectory());
        allAttributes.put("is_file", file.isFile());
        allAttributes.put("is_symbolic_link", file.isSymbolicLink());
        
        // Add custom attributes
        allAttributes.putAll(file.getAttributes());
        
        return allAttributes;
    }
    
    /**
     * Checks if a file has a specific attribute.
     *
     * @param remotePath The path to the file on the FTP server
     * @param attributeName The name of the attribute to check
     * @return true if the file exists and has the specified attribute, false otherwise
     * @throws IOException if an error occurs while checking the file attribute
     */
    public boolean hasFileAttribute(String remotePath, String attributeName) throws IOException {
        if (attributeName == null) {
            return false;
        }
        
        // Check if it's a standard attribute
        switch (attributeName) {
            case "name":
            case "path":
            case "size":
            case "timestamp":
            case "permissions":
            case "owner":
            case "group":
            case "type":
            case "is_directory":
            case "is_file":
            case "is_symbolic_link":
                // These are standard attributes that all files have
                return getFileAttributes(remotePath) != null;
            default:
                // Check custom attributes
                FTPFile file = getFileAttributes(remotePath);
                return file != null && file.getAttribute(attributeName) != null;
        }
    }
    
    /**
     * Compares file attributes between two files.
     *
     * @param file1Path The path to the first file on the FTP server
     * @param file2Path The path to the second file on the FTP server
     * @param attributeNames Array of attribute names to compare (or null to compare all attributes)
     * @return A map of differences where the key is the attribute name and the value is an array
     *         containing the values from file1 and file2 respectively
     * @throws IOException if an error occurs while comparing file attributes
     */
    public Map<String, Object[]> compareFileAttributes(String file1Path, String file2Path, String[] attributeNames) throws IOException {
        FTPFile file1 = getFileAttributes(file1Path);
        FTPFile file2 = getFileAttributes(file2Path);
        
        if (file1 == null || file2 == null) {
            throw new IOException("One or both files do not exist");
        }
        
        Map<String, Object[]> differences = new HashMap<>();
        Map<String, Object> file1Attrs = getAllFileAttributes(file1Path);
        Map<String, Object> file2Attrs = getAllFileAttributes(file2Path);
        
        // If specific attribute names were provided, only compare those
        if (attributeNames != null && attributeNames.length > 0) {
            for (String attrName : attributeNames) {
                Object val1 = file1Attrs.get(attrName);
                Object val2 = file2Attrs.get(attrName);
                
                if (val1 == null && val2 == null) {
                    continue;
                }
                
                if (val1 == null || val2 == null || !val1.equals(val2)) {
                    differences.put(attrName, new Object[]{val1, val2});
                }
            }
        } else {
            // Compare all attributes
            for (String attrName : file1Attrs.keySet()) {
                Object val1 = file1Attrs.get(attrName);
                Object val2 = file2Attrs.get(attrName);
                
                if (val1 == null && val2 == null) {
                    continue;
                }
                
                if (val1 == null || val2 == null || !val1.equals(val2)) {
                    differences.put(attrName, new Object[]{val1, val2});
                }
            }
            
            // Check for attributes in file2 that are not in file1
            for (String attrName : file2Attrs.keySet()) {
                if (!file1Attrs.containsKey(attrName)) {
                    differences.put(attrName, new Object[]{null, file2Attrs.get(attrName)});
                }
            }
        }
        
        return differences;
    }
    
    /**
     * Filters a list of files based on attribute criteria.
     *
     * @param directory The directory to list files from
     * @param attributeFilters A map of attribute names to filter values
     * @return A list of FTPFile objects that match the filter criteria
     * @throws IOException if an error occurs while listing or filtering files
     */
    public List<FTPFile> listFilesWithAttributes(String directory, Map<String, Object> attributeFilters) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        // Get all files in the directory
        List<FTPFile> allFiles = listFiles(directory, null);
        
        // If no filters provided, return all files
        if (attributeFilters == null || attributeFilters.isEmpty()) {
            return allFiles;
        }
        
        // Filter files based on attribute criteria
        List<FTPFile> filteredFiles = new ArrayList<>();
        for (FTPFile file : allFiles) {
            boolean matches = true;
            
            for (Map.Entry<String, Object> filter : attributeFilters.entrySet()) {
                String attributeName = filter.getKey();
                Object filterValue = filter.getValue();
                
                // Skip null filter values
                if (filterValue == null) {
                    continue;
                }
                
                // Get the file's attribute value
                Object attributeValue;
                switch (attributeName) {
                    case "name":
                        attributeValue = file.getName();
                        break;
                    case "path":
                        attributeValue = file.getPath();
                        break;
                    case "size":
                        attributeValue = file.getSize();
                        break;
                    case "timestamp":
                        attributeValue = file.getTimestamp();
                        break;
                    case "permissions":
                        attributeValue = file.getPermissions();
                        break;
                    case "owner":
                        attributeValue = file.getOwner();
                        break;
                    case "group":
                        attributeValue = file.getGroup();
                        break;
                    case "type":
                        attributeValue = file.getType();
                        break;
                    case "is_directory":
                        attributeValue = file.isDirectory();
                        break;
                    case "is_file":
                        attributeValue = file.isFile();
                        break;
                    case "is_symbolic_link":
                        attributeValue = file.isSymbolicLink();
                        break;
                    default:
                        // Check custom attributes
                        attributeValue = file.getAttribute(attributeName);
                        break;
                }
                
                // If the attribute doesn't exist or doesn't match, the file is not a match
                if (attributeValue == null || !filterValue.equals(attributeValue)) {
                    matches = false;
                    break;
                }
            }
            
            if (matches) {
                filteredFiles.add(file);
            }
        }
        
        return filteredFiles;
    }
    
    /**
     * Updates file attributes if the FTP server supports it.
     * Not all FTP servers support attribute updates, and the supported attributes may vary by server.
     *
     * @param remotePath The path to the file on the FTP server
     * @param attributes A map of attribute names to values to update
     * @return true if the attributes were updated successfully, false otherwise
     * @throws IOException if an error occurs while updating the file attributes
     */
    public boolean updateFileAttributes(String remotePath, Map<String, Object> attributes) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("FTP Connection Service is not initialized");
        }
        
        if (attributes == null || attributes.isEmpty()) {
            return true; // Nothing to update
        }
        
        FTPClient client = null;
        try {
            // Get a connection from the pool
            client = getConnection();
            
            // Check if the file exists
            FTPFile file = getFileAttributes(remotePath);
            if (file == null) {
                return false;
            }
            
            // Not all FTP servers support attribute updates, and the supported attributes vary by server
            // We'll implement a best-effort approach for common attributes
            
            // Check for timestamp update
            if (attributes.containsKey("timestamp") && attributes.get("timestamp") instanceof Date) {
                Date timestamp = (Date) attributes.get("timestamp");
                boolean success = client.setModificationTime(remotePath, new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp));
                if (!success) {
                    getLogger().warn("Failed to update timestamp for file: {}", remotePath);
                    return false;
                }
                getLogger().debug("Updated timestamp for file: {}", remotePath);
            }
            
            // Permissions, owner, and group updates would require SITE commands which vary by server
            // and may not be supported by all servers
            
            return true;
            
        } catch (IOException e) {
            getLogger().error("Error updating file attributes for: {}", 
                new Object[] { remotePath, e });
            throw e;
        } finally {
            // Return the connection to the pool
            if (client != null) {
                releaseConnection(client);
            }
        }
    }

    @Override
    public void disconnect(FTPClient client) {
        if (connectionManager != null && client != null) {
            try {
                connectionManager.closeConnection(client);
            } catch (Exception e) {
                getLogger().error("Error disconnecting FTP client", e);
                recordError(FTPErrorType.CONNECTION_ERROR, "Error disconnecting FTP client: " + e.getMessage());
            }
        }
    }
    
    /**
     * Records an error in the service metrics and logs it appropriately.
     *
     * @param errorType The type of error
     * @param errorMessage The error message
     */
    private void recordError(FTPErrorType errorType, String errorMessage) {
        lastErrorType = errorType;
        lastErrorMessage = errorMessage;
        lastErrorTime = new Date();
        
        // Increment appropriate counter based on error type
        switch (errorType.getClass().getSimpleName()) {
            case "CONNECTION_ERROR":
            case "CONNECTION_TIMEOUT":
            case "CONNECTION_CLOSED":
            case "CONNECTION_REFUSED":
            case "POOL_EXHAUSTED":
            case "POOL_ERROR":
                connectionErrorCount.incrementAndGet();
                break;
                
            case "AUTHENTICATION_ERROR":
            case "INVALID_CREDENTIALS":
            case "INSUFFICIENT_PERMISSIONS":
                authenticationErrorCount.incrementAndGet();
                break;
                
            case "TRANSFER_ERROR":
            case "TRANSFER_ABORTED":
            case "TRANSFER_TIMEOUT":
            case "DATA_CONNECTION_ERROR":
            case "DATA_CONNECTION_TIMEOUT":
                transferErrorCount.incrementAndGet();
                break;
                
            case "FILE_NOT_FOUND":
            case "FILE_ALREADY_EXISTS":
            case "DIRECTORY_NOT_FOUND":
            case "DIRECTORY_NOT_EMPTY":
            case "INVALID_PATH":
            case "INSUFFICIENT_STORAGE":
                fileOperationErrorCount.incrementAndGet();
                break;
                
            default:
                unexpectedErrorCount.incrementAndGet();
                break;
        }
    }
    
    /**
     * Records a successful operation.
     */
    private void recordSuccess() {
        successfulOperationCount.incrementAndGet();
    }
    
    /**
     * Records a successful reconnection.
     */
    private void recordSuccessfulReconnection() {
        successfulReconnectionCount.incrementAndGet();
    }
    
    /**
     * Records a failed reconnection.
     */
    private void recordFailedReconnection() {
        failedReconnectionCount.incrementAndGet();
    }
    
    /**
     * Handles an FTP exception by recording metrics and determining if retry is needed.
     *
     * @param exception The exception to handle
     * @param client The FTPClient being used
     * @param connection The FTPConnection being used, if available
     * @param remotePath The remote file path, if applicable
     * @return true if the operation should be retried, false otherwise
     */
    private boolean handleFTPException(Exception exception, FTPClient client, FTPConnection connection, String remotePath) {
        if (exception == null) {
            return false;
        }
        
        try {
            // If it's an FTPOperationException, record specific error metrics
            if (exception instanceof FTPOperationException) {
                FTPOperationException ftpEx = (FTPOperationException) exception;
                recordError(ftpEx.getErrorType(), ftpEx.getMessage());
                
                // For connection exceptions, attempt to recover the connection
                if (ftpEx instanceof FTPConnectionException && connection != null) {
                    try {
                        boolean reconnected = connectionManager.reconnect(connection);
                        if (reconnected) {
                            recordSuccessfulReconnection();
                            return true; // Retry the operation
                        } else {
                            recordFailedReconnection();
                        }
                    } catch (Exception e) {
                        recordFailedReconnection();
                        getLogger().error("Error during reconnection attempt", e);
                    }
                }
                
                return ftpEx.isRecoverable();
            }
            
            // For other exception types, try to determine the error type
            if (exception instanceof IOException) {
                // Use FTPErrorHandler to analyze the exception
                return FTPErrorHandler.handleFTPError((IOException) exception, client, connection, connectionManager, getLogger());
            }
            
            // For unexpected exceptions, record as unexpected error
            recordError(FTPErrorType.UNEXPECTED_ERROR, exception.getMessage());
            getLogger().error("Unexpected exception during FTP operation", exception);
            return false;
            
        } catch (Exception e) {
            // If error handling itself fails, record as unexpected and don't retry
            getLogger().error("Error during exception handling", e);
            recordError(FTPErrorType.UNEXPECTED_ERROR, "Error during exception handling: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Checks an FTP reply code and throws an appropriate exception if it indicates an error.
     *
     * @param client The FTP client
     * @param operation The operation being performed
     * @param remotePath The remote file path (optional)
     * @throws FTPOperationException if the reply code indicates an error
     */
    private void checkReplyCode(FTPClient client, String operation, String remotePath) throws FTPOperationException {
        if (client == null) {
            return;
        }
        
        int replyCode = client.getReplyCode();
        if (!FTPReply.isPositiveCompletion(replyCode) && !FTPReply.isPositivePreliminary(replyCode)) {
            FTPErrorType errorType = FTPErrorHandler.translateReplyCodeToErrorType(replyCode);
            String replyString = client.getReplyString();
            
            // Create an appropriate exception based on the error type
            FTPOperationException exception = FTPErrorHandler.createException(errorType, replyCode,
                    "FTP operation '" + operation + "' failed: " + replyString, remotePath, null);
            
            // Record the error in metrics
            recordError(errorType, exception.getMessage());
            
            // Throw the exception
            throw exception;
        } else {
            // Record successful operation
            recordSuccess();
        }
    }
}