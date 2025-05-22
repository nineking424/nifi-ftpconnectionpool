package org.apache.nifi.controllers.ftp;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.FormatUtils;

import java.util.concurrent.TimeUnit;

/**
 * Configuration class that encapsulates all FTP connection settings.
 * This class is immutable, and instances are created using the Builder pattern.
 */
public class FTPConnectionConfig {
    // Basic connection parameters
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    
    // Connection timeouts
    private final int connectionTimeout;
    private final int dataTimeout;
    private final int controlTimeout;
    
    // Connection mode
    private final boolean activeMode;
    
    // Connection pool parameters
    private final int maxConnections;
    private final int minConnections;
    private final int keepAliveInterval;
    private final int connectionIdleTimeout;
    
    // Transfer parameters
    private final int bufferSize;
    private final String controlEncoding;
    private final String transferMode; // ASCII or Binary
    private final boolean localActivePortRange; // Whether to use a specific port range for active mode
    private final int activePortRangeStart; // Starting port for active mode range
    private final int activePortRangeEnd; // Ending port for active mode range
    private final String activeExternalIPAddress; // External IP for active mode behind NAT
    
    // SSL/TLS parameters
    private final boolean useClientMode;
    private final boolean useImplicitSSL;
    private final boolean useExplicitSSL; // FTPS with explicit TLS
    private final String[] enabledProtocols; // Enabled SSL/TLS protocols
    private final String[] enabledCipherSuites; // Enabled SSL/TLS cipher suites
    private final boolean validateServerCertificate; // Whether to validate the server's certificate
    private final String trustStorePath; // Path to truststore for server certificate validation
    private final String trustStorePassword; // Password for truststore
    private final String trustStoreType; // Type of truststore (JKS, PKCS12, etc.)
    
    // Optional proxy parameters
    private final String proxyHost;
    private final Integer proxyPort;
    private final String proxyUsername;
    private final String proxyPassword;
    private final String proxyType; // SOCKS, HTTP, etc.

    /**
     * Private constructor - use Builder to create instances.
     */
    private FTPConnectionConfig(Builder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
        this.connectionTimeout = builder.connectionTimeout;
        this.dataTimeout = builder.dataTimeout;
        this.controlTimeout = builder.controlTimeout;
        this.activeMode = builder.activeMode;
        this.maxConnections = builder.maxConnections;
        this.minConnections = builder.minConnections;
        this.keepAliveInterval = builder.keepAliveInterval;
        this.connectionIdleTimeout = builder.connectionIdleTimeout;
        this.bufferSize = builder.bufferSize;
        this.controlEncoding = builder.controlEncoding;
        this.transferMode = builder.transferMode;
        this.localActivePortRange = builder.localActivePortRange;
        this.activePortRangeStart = builder.activePortRangeStart;
        this.activePortRangeEnd = builder.activePortRangeEnd;
        this.activeExternalIPAddress = builder.activeExternalIPAddress;
        this.useClientMode = builder.useClientMode;
        this.useImplicitSSL = builder.useImplicitSSL;
        this.useExplicitSSL = builder.useExplicitSSL;
        this.enabledProtocols = builder.enabledProtocols;
        this.enabledCipherSuites = builder.enabledCipherSuites;
        this.validateServerCertificate = builder.validateServerCertificate;
        this.trustStorePath = builder.trustStorePath;
        this.trustStorePassword = builder.trustStorePassword;
        this.trustStoreType = builder.trustStoreType;
        this.proxyHost = builder.proxyHost;
        this.proxyPort = builder.proxyPort;
        this.proxyUsername = builder.proxyUsername;
        this.proxyPassword = builder.proxyPassword;
        this.proxyType = builder.proxyType;
    }

    /**
     * Creates a new FTPConnectionConfig from a NiFi ConfigurationContext.
     *
     * @param context the NiFi ConfigurationContext
     * @param logger the logger to use for the secure credential handler
     * @return a new FTPConnectionConfig
     */
    public static FTPConnectionConfig fromContext(final ConfigurationContext context, final ComponentLog logger) {
        Builder builder = new Builder();
        
        // Create a secure credential handler
        SecureCredentialHandler credentialHandler = new SecureCredentialHandler(logger);
        
        // Basic connection parameters
        builder.hostname(context.getProperty(PersistentFTPConnectionService.HOSTNAME).evaluateAttributeExpressions().getValue());
        builder.port(context.getProperty(PersistentFTPConnectionService.PORT).evaluateAttributeExpressions().asInteger());
        builder.username(context.getProperty(PersistentFTPConnectionService.USERNAME).evaluateAttributeExpressions().getValue());
        
        // Handle password securely using the enum type for better type safety
        String password = context.getProperty(PersistentFTPConnectionService.PASSWORD).evaluateAttributeExpressions().getValue();
        credentialHandler.storeCredential(SecureCredentialHandler.CredentialType.PASSWORD, password);
        builder.password(password);
        
        // Connection timeouts
        PropertyValue connectionTimeoutProperty = context.getProperty(PersistentFTPConnectionService.CONNECTION_TIMEOUT).evaluateAttributeExpressions();
        long connectionTimeoutMillis = FormatUtils.getTimeDuration(connectionTimeoutProperty.getValue(), TimeUnit.MILLISECONDS);
        builder.connectionTimeout((int) connectionTimeoutMillis);
        
        PropertyValue dataTimeoutProperty = context.getProperty(PersistentFTPConnectionService.DATA_TIMEOUT).evaluateAttributeExpressions();
        long dataTimeoutMillis = FormatUtils.getTimeDuration(dataTimeoutProperty.getValue(), TimeUnit.MILLISECONDS);
        builder.dataTimeout((int) dataTimeoutMillis);
        
        // Control timeout if specified
        if (context.getProperty(PersistentFTPConnectionService.CONTROL_TIMEOUT) != null && 
            context.getProperty(PersistentFTPConnectionService.CONTROL_TIMEOUT).isSet()) {
            PropertyValue controlTimeoutProperty = context.getProperty(PersistentFTPConnectionService.CONTROL_TIMEOUT).evaluateAttributeExpressions();
            long controlTimeoutMillis = FormatUtils.getTimeDuration(controlTimeoutProperty.getValue(), TimeUnit.MILLISECONDS);
            builder.controlTimeout((int) controlTimeoutMillis);
        }
        
        // Connection mode
        builder.activeMode(context.getProperty(PersistentFTPConnectionService.ACTIVE_MODE).asBoolean());
        
        // Active mode specific configuration
        if (context.getProperty(PersistentFTPConnectionService.LOCAL_ACTIVE_PORT_RANGE) != null &&
            context.getProperty(PersistentFTPConnectionService.LOCAL_ACTIVE_PORT_RANGE).isSet()) {
            boolean useLocalPortRange = context.getProperty(PersistentFTPConnectionService.LOCAL_ACTIVE_PORT_RANGE).asBoolean();
            builder.localActivePortRange(useLocalPortRange);
            
            if (useLocalPortRange) {
                if (context.getProperty(PersistentFTPConnectionService.ACTIVE_PORT_RANGE_START) != null && 
                    context.getProperty(PersistentFTPConnectionService.ACTIVE_PORT_RANGE_START).isSet()) {
                    builder.activePortRangeStart(context.getProperty(PersistentFTPConnectionService.ACTIVE_PORT_RANGE_START).asInteger());
                }
                
                if (context.getProperty(PersistentFTPConnectionService.ACTIVE_PORT_RANGE_END) != null && 
                    context.getProperty(PersistentFTPConnectionService.ACTIVE_PORT_RANGE_END).isSet()) {
                    builder.activePortRangeEnd(context.getProperty(PersistentFTPConnectionService.ACTIVE_PORT_RANGE_END).asInteger());
                }
            }
        }
        
        // External IP address for active mode behind NAT
        if (context.getProperty(PersistentFTPConnectionService.ACTIVE_EXTERNAL_IP_ADDRESS) != null &&
            context.getProperty(PersistentFTPConnectionService.ACTIVE_EXTERNAL_IP_ADDRESS).isSet()) {
            String externalIp = context.getProperty(PersistentFTPConnectionService.ACTIVE_EXTERNAL_IP_ADDRESS)
                .evaluateAttributeExpressions().getValue();
            builder.activeExternalIPAddress(externalIp);
        }
        
        // Connection pool parameters
        builder.maxConnections(context.getProperty(PersistentFTPConnectionService.MAX_CONNECTIONS).evaluateAttributeExpressions().asInteger());
        builder.minConnections(context.getProperty(PersistentFTPConnectionService.MIN_CONNECTIONS).evaluateAttributeExpressions().asInteger());
        
        PropertyValue keepAliveProperty = context.getProperty(PersistentFTPConnectionService.KEEP_ALIVE_INTERVAL).evaluateAttributeExpressions();
        long keepAliveMillis = FormatUtils.getTimeDuration(keepAliveProperty.getValue(), TimeUnit.MILLISECONDS);
        builder.keepAliveInterval((int) keepAliveMillis);
        
        PropertyValue idleTimeoutProperty = context.getProperty(PersistentFTPConnectionService.CONNECTION_IDLE_TIMEOUT).evaluateAttributeExpressions();
        long idleTimeoutMillis = FormatUtils.getTimeDuration(idleTimeoutProperty.getValue(), TimeUnit.MILLISECONDS);
        builder.connectionIdleTimeout((int) idleTimeoutMillis);
        
        // Transfer parameters
        PropertyValue bufferSizeProperty = context.getProperty(PersistentFTPConnectionService.BUFFER_SIZE).evaluateAttributeExpressions();
        int bufferSize = (int) FormatUtils.getDataSize(bufferSizeProperty.getValue(), DataUnit.B);
        builder.bufferSize(bufferSize);
        
        builder.controlEncoding(context.getProperty(PersistentFTPConnectionService.CONTROL_ENCODING).evaluateAttributeExpressions().getValue());
        
        // Transfer mode setting
        if (context.getProperty(PersistentFTPConnectionService.TRANSFER_MODE) != null) {
            builder.transferMode(context.getProperty(PersistentFTPConnectionService.TRANSFER_MODE).getValue());
        }
        
        // SSL/TLS parameters
        if (context.getProperty(PersistentFTPConnectionService.USE_CLIENT_MODE) != null) {
            builder.useClientMode(context.getProperty(PersistentFTPConnectionService.USE_CLIENT_MODE).asBoolean());
        }
        
        if (context.getProperty(PersistentFTPConnectionService.USE_IMPLICIT_SSL) != null) {
            builder.useImplicitSSL(context.getProperty(PersistentFTPConnectionService.USE_IMPLICIT_SSL).asBoolean());
        }
        
        if (context.getProperty(PersistentFTPConnectionService.USE_EXPLICIT_SSL) != null && 
            context.getProperty(PersistentFTPConnectionService.USE_EXPLICIT_SSL).isSet()) {
            builder.useExplicitSSL(context.getProperty(PersistentFTPConnectionService.USE_EXPLICIT_SSL).asBoolean());
        }
        
        // SSL protocols and cipher suites
        if (context.getProperty(PersistentFTPConnectionService.ENABLED_PROTOCOLS) != null && 
            context.getProperty(PersistentFTPConnectionService.ENABLED_PROTOCOLS).isSet()) {
            String protocolsString = context.getProperty(PersistentFTPConnectionService.ENABLED_PROTOCOLS).getValue();
            if (protocolsString != null && !protocolsString.isEmpty()) {
                String[] protocols = protocolsString.split(",");
                // Trim each protocol string
                for (int i = 0; i < protocols.length; i++) {
                    protocols[i] = protocols[i].trim();
                }
                builder.enabledProtocols(protocols);
            }
        }
        
        if (context.getProperty(PersistentFTPConnectionService.ENABLED_CIPHER_SUITES) != null && 
            context.getProperty(PersistentFTPConnectionService.ENABLED_CIPHER_SUITES).isSet()) {
            String cipherSuitesString = context.getProperty(PersistentFTPConnectionService.ENABLED_CIPHER_SUITES).getValue();
            if (cipherSuitesString != null && !cipherSuitesString.isEmpty()) {
                String[] cipherSuites = cipherSuitesString.split(",");
                // Trim each cipher suite string
                for (int i = 0; i < cipherSuites.length; i++) {
                    cipherSuites[i] = cipherSuites[i].trim();
                }
                builder.enabledCipherSuites(cipherSuites);
            }
        }
        
        // SSL certificate validation
        if (context.getProperty(PersistentFTPConnectionService.VALIDATE_SERVER_CERTIFICATE) != null) {
            builder.validateServerCertificate(context.getProperty(PersistentFTPConnectionService.VALIDATE_SERVER_CERTIFICATE).asBoolean());
        }
        
        // SSL truststore settings
        if (context.getProperty(PersistentFTPConnectionService.TRUST_STORE_PATH) != null && 
            context.getProperty(PersistentFTPConnectionService.TRUST_STORE_PATH).isSet()) {
            builder.trustStorePath(context.getProperty(PersistentFTPConnectionService.TRUST_STORE_PATH).getValue());
        }
        
        if (context.getProperty(PersistentFTPConnectionService.TRUST_STORE_PASSWORD) != null && 
            context.getProperty(PersistentFTPConnectionService.TRUST_STORE_PASSWORD).isSet()) {
            String trustStorePassword = context.getProperty(PersistentFTPConnectionService.TRUST_STORE_PASSWORD).getValue();
            credentialHandler.storeCredential(SecureCredentialHandler.CredentialType.TRUST_STORE_PASSWORD, trustStorePassword);
            builder.trustStorePassword(trustStorePassword);
        }
        
        if (context.getProperty(PersistentFTPConnectionService.TRUST_STORE_TYPE) != null && 
            context.getProperty(PersistentFTPConnectionService.TRUST_STORE_TYPE).isSet()) {
            builder.trustStoreType(context.getProperty(PersistentFTPConnectionService.TRUST_STORE_TYPE).getValue());
        }
        
        // Proxy settings - now using explicit properties
        handleProxySettings(context, builder, credentialHandler, logger);
        
        return builder.build();
    }
    
    /**
     * Handles proxy settings from the configuration context.
     * This method now processes the explicit proxy properties added to the service.
     *
     * @param context the configuration context
     * @param builder the builder to update
     * @param credentialHandler the credential handler for secure storage
     * @param logger the logger for diagnostics
     */
    private static void handleProxySettings(ConfigurationContext context, Builder builder, 
                                           SecureCredentialHandler credentialHandler, ComponentLog logger) {
        try {
            // Process the explicitly defined proxy properties first
            if (context.getProperty(PersistentFTPConnectionService.PROXY_HOST) != null && 
                context.getProperty(PersistentFTPConnectionService.PROXY_HOST).isSet()) {
                
                String proxyHost = context.getProperty(PersistentFTPConnectionService.PROXY_HOST)
                    .evaluateAttributeExpressions().getValue();
                
                if (proxyHost != null && !proxyHost.isEmpty()) {
                    builder.proxyHost(proxyHost);
                    logger.debug("Set proxy host from explicit property: {}", proxyHost);
                    
                    // If we have a proxy host, check for other proxy settings
                    if (context.getProperty(PersistentFTPConnectionService.PROXY_PORT) != null && 
                        context.getProperty(PersistentFTPConnectionService.PROXY_PORT).isSet()) {
                        Integer proxyPort = context.getProperty(PersistentFTPConnectionService.PROXY_PORT)
                            .evaluateAttributeExpressions().asInteger();
                        builder.proxyPort(proxyPort);
                        logger.debug("Set proxy port from explicit property: {}", proxyPort);
                    }
                    
                    if (context.getProperty(PersistentFTPConnectionService.PROXY_USERNAME) != null && 
                        context.getProperty(PersistentFTPConnectionService.PROXY_USERNAME).isSet()) {
                        String proxyUsername = context.getProperty(PersistentFTPConnectionService.PROXY_USERNAME)
                            .evaluateAttributeExpressions().getValue();
                        builder.proxyUsername(proxyUsername);
                        logger.debug("Set proxy username from explicit property: {}", proxyUsername);
                    }
                    
                    if (context.getProperty(PersistentFTPConnectionService.PROXY_PASSWORD) != null && 
                        context.getProperty(PersistentFTPConnectionService.PROXY_PASSWORD).isSet()) {
                        String proxyPassword = context.getProperty(PersistentFTPConnectionService.PROXY_PASSWORD)
                            .evaluateAttributeExpressions().getValue();
                        if (proxyPassword != null && !proxyPassword.isEmpty()) {
                            // Store password securely
                            credentialHandler.storeCredential(SecureCredentialHandler.CredentialType.PROXY_PASSWORD, proxyPassword);
                            builder.proxyPassword(proxyPassword);
                            logger.debug("Set and securely stored proxy password from explicit property");
                        }
                    }
                    
                    if (context.getProperty(PersistentFTPConnectionService.PROXY_TYPE) != null && 
                        context.getProperty(PersistentFTPConnectionService.PROXY_TYPE).isSet()) {
                        String proxyType = context.getProperty(PersistentFTPConnectionService.PROXY_TYPE).getValue();
                        builder.proxyType(proxyType);
                        logger.debug("Set proxy type from explicit property: {}", proxyType);
                    }
                }
            } else {
                // As a fallback, still look for proxy settings using property name pattern matching
                // for backward compatibility or cases where custom property names are used
                for (String propertyName : context.getPropertyKeys()) {
                    String normalizedName = propertyName.toLowerCase().replace(" ", "");
                    
                    // Skip properties that are handled by explicit properties
                    if (propertyName.equals(PersistentFTPConnectionService.PROXY_HOST.getName()) ||
                        propertyName.equals(PersistentFTPConnectionService.PROXY_PORT.getName()) ||
                        propertyName.equals(PersistentFTPConnectionService.PROXY_USERNAME.getName()) ||
                        propertyName.equals(PersistentFTPConnectionService.PROXY_PASSWORD.getName()) ||
                        propertyName.equals(PersistentFTPConnectionService.PROXY_TYPE.getName())) {
                        continue;
                    }
                    
                    // Handle proxy host
                    if (normalizedName.contains("proxy") && normalizedName.contains("host")) {
                        PropertyValue property = context.getProperty(propertyName);
                        if (property != null && property.isSet()) {
                            String proxyHost = property.evaluateAttributeExpressions().getValue();
                            if (proxyHost != null && !proxyHost.isEmpty()) {
                                builder.proxyHost(proxyHost);
                                logger.debug("Found and set proxy host via pattern matching: {}", proxyHost);
                            }
                        }
                    }
                    
                    // Handle proxy port
                    if (normalizedName.contains("proxy") && normalizedName.contains("port")) {
                        PropertyValue property = context.getProperty(propertyName);
                        if (property != null && property.isSet()) {
                            Integer proxyPort = property.evaluateAttributeExpressions().asInteger();
                            if (proxyPort != null) {
                                builder.proxyPort(proxyPort);
                                logger.debug("Found and set proxy port via pattern matching: {}", proxyPort);
                            }
                        }
                    }
                    
                    // Handle proxy username
                    if (normalizedName.contains("proxy") && normalizedName.contains("username")) {
                        PropertyValue property = context.getProperty(propertyName);
                        if (property != null && property.isSet()) {
                            String proxyUsername = property.evaluateAttributeExpressions().getValue();
                            if (proxyUsername != null && !proxyUsername.isEmpty()) {
                                builder.proxyUsername(proxyUsername);
                                logger.debug("Found and set proxy username via pattern matching: {}", proxyUsername);
                            }
                        }
                    }
                    
                    // Handle proxy password - store securely
                    if (normalizedName.contains("proxy") && normalizedName.contains("password")) {
                        PropertyValue property = context.getProperty(propertyName);
                        if (property != null && property.isSet()) {
                            String proxyPassword = property.evaluateAttributeExpressions().getValue();
                            if (proxyPassword != null && !proxyPassword.isEmpty()) {
                                // Store password securely
                                credentialHandler.storeCredential(SecureCredentialHandler.CredentialType.PROXY_PASSWORD, proxyPassword);
                                builder.proxyPassword(proxyPassword);
                                logger.debug("Found and securely stored proxy password via pattern matching");
                            }
                        }
                    }
                    
                    // Handle proxy type
                    if (normalizedName.contains("proxy") && normalizedName.contains("type")) {
                        PropertyValue property = context.getProperty(propertyName);
                        if (property != null && property.isSet()) {
                            String proxyType = property.getValue();
                            if (proxyType != null && !proxyType.isEmpty()) {
                                builder.proxyType(proxyType);
                                logger.debug("Found and set proxy type via pattern matching: {}", proxyType);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Log but don't fail if proxy settings can't be processed
            logger.warn("Error processing proxy settings: {}", e.getMessage(), e);
        }
    }

    /**
     * Builder class for FTPConnectionConfig.
     */
    public static class Builder {
        // Basic connection parameters
        private String hostname;
        private int port = 21; // Default FTP port
        private String username;
        private String password;
        
        // Connection timeouts
        private int connectionTimeout = 30000; // Default 30 seconds
        private int dataTimeout = 60000; // Default 60 seconds
        private int controlTimeout = 60000; // Default 60 seconds
        
        // Connection mode
        private boolean activeMode = false; // Default to passive mode
        
        // Connection pool parameters
        private int maxConnections = 10; // Default max connections
        private int minConnections = 2; // Default min connections
        private int keepAliveInterval = 60000; // Default 60 seconds
        private int connectionIdleTimeout = 300000; // Default 5 minutes
        
        // Transfer parameters
        private int bufferSize = 1048576; // Default 1MB
        private String controlEncoding = "UTF-8"; // Default encoding
        private String transferMode = "Binary"; // Default transfer mode
        private boolean localActivePortRange = false; // Default to not use port range
        private int activePortRangeStart = 40000; // Default start port
        private int activePortRangeEnd = 50000; // Default end port
        private String activeExternalIPAddress = null; // Default to no external IP
        
        // SSL/TLS parameters
        private boolean useClientMode = true; // Default
        private boolean useImplicitSSL = false; // Default
        private boolean useExplicitSSL = false; // Default
        private String[] enabledProtocols = null; // Default to null (use system defaults)
        private String[] enabledCipherSuites = null; // Default to null (use system defaults)
        private boolean validateServerCertificate = true; // Default to validate
        private String trustStorePath = null; // Default to null (use system default)
        private String trustStorePassword = null; // Default to null
        private String trustStoreType = "JKS"; // Default to JKS
        
        // Optional proxy parameters
        private String proxyHost;
        private Integer proxyPort;
        private String proxyUsername;
        private String proxyPassword;
        private String proxyType = "SOCKS"; // Default to SOCKS

        /**
         * Sets the hostname.
         */
        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * Sets the port.
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the username.
         */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the password.
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the connection timeout in milliseconds.
         */
        public Builder connectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the data timeout in milliseconds.
         */
        public Builder dataTimeout(int dataTimeout) {
            this.dataTimeout = dataTimeout;
            return this;
        }
        
        /**
         * Sets the control channel timeout in milliseconds.
         */
        public Builder controlTimeout(int controlTimeout) {
            this.controlTimeout = controlTimeout;
            return this;
        }

        /**
         * Sets whether to use active mode.
         */
        public Builder activeMode(boolean activeMode) {
            this.activeMode = activeMode;
            return this;
        }

        /**
         * Sets the maximum number of connections.
         */
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        /**
         * Sets the minimum number of connections.
         */
        public Builder minConnections(int minConnections) {
            this.minConnections = minConnections;
            return this;
        }

        /**
         * Sets the keep-alive interval in milliseconds.
         */
        public Builder keepAliveInterval(int keepAliveInterval) {
            this.keepAliveInterval = keepAliveInterval;
            return this;
        }

        /**
         * Sets the connection idle timeout in milliseconds.
         */
        public Builder connectionIdleTimeout(int connectionIdleTimeout) {
            this.connectionIdleTimeout = connectionIdleTimeout;
            return this;
        }

        /**
         * Sets the buffer size in bytes.
         */
        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         * Sets the control encoding.
         */
        public Builder controlEncoding(String controlEncoding) {
            this.controlEncoding = controlEncoding;
            return this;
        }

        /**
         * Sets the transfer mode (ASCII or Binary).
         */
        public Builder transferMode(String transferMode) {
            this.transferMode = transferMode;
            return this;
        }
        
        /**
         * Sets whether to use a local active port range.
         */
        public Builder localActivePortRange(boolean localActivePortRange) {
            this.localActivePortRange = localActivePortRange;
            return this;
        }
        
        /**
         * Sets the starting port for active mode range.
         */
        public Builder activePortRangeStart(int activePortRangeStart) {
            this.activePortRangeStart = activePortRangeStart;
            return this;
        }
        
        /**
         * Sets the ending port for active mode range.
         */
        public Builder activePortRangeEnd(int activePortRangeEnd) {
            this.activePortRangeEnd = activePortRangeEnd;
            return this;
        }
        
        /**
         * Sets the external IP address for active mode.
         */
        public Builder activeExternalIPAddress(String activeExternalIPAddress) {
            this.activeExternalIPAddress = activeExternalIPAddress;
            return this;
        }

        /**
         * Sets whether to use client mode.
         */
        public Builder useClientMode(boolean useClientMode) {
            this.useClientMode = useClientMode;
            return this;
        }

        /**
         * Sets whether to use implicit SSL.
         */
        public Builder useImplicitSSL(boolean useImplicitSSL) {
            this.useImplicitSSL = useImplicitSSL;
            return this;
        }
        
        /**
         * Sets whether to use explicit SSL/TLS (FTPS with explicit TLS).
         */
        public Builder useExplicitSSL(boolean useExplicitSSL) {
            this.useExplicitSSL = useExplicitSSL;
            return this;
        }
        
        /**
         * Sets the enabled SSL/TLS protocols.
         */
        public Builder enabledProtocols(String[] enabledProtocols) {
            this.enabledProtocols = enabledProtocols;
            return this;
        }
        
        /**
         * Sets the enabled SSL/TLS cipher suites.
         */
        public Builder enabledCipherSuites(String[] enabledCipherSuites) {
            this.enabledCipherSuites = enabledCipherSuites;
            return this;
        }
        
        /**
         * Sets whether to validate the server's certificate.
         */
        public Builder validateServerCertificate(boolean validateServerCertificate) {
            this.validateServerCertificate = validateServerCertificate;
            return this;
        }
        
        /**
         * Sets the path to the truststore for server certificate validation.
         */
        public Builder trustStorePath(String trustStorePath) {
            this.trustStorePath = trustStorePath;
            return this;
        }
        
        /**
         * Sets the password for the truststore.
         */
        public Builder trustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }
        
        /**
         * Sets the type of the truststore.
         */
        public Builder trustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * Sets the proxy host.
         */
        public Builder proxyHost(String proxyHost) {
            this.proxyHost = proxyHost;
            return this;
        }

        /**
         * Sets the proxy port.
         */
        public Builder proxyPort(Integer proxyPort) {
            this.proxyPort = proxyPort;
            return this;
        }

        /**
         * Sets the proxy username.
         */
        public Builder proxyUsername(String proxyUsername) {
            this.proxyUsername = proxyUsername;
            return this;
        }

        /**
         * Sets the proxy password.
         */
        public Builder proxyPassword(String proxyPassword) {
            this.proxyPassword = proxyPassword;
            return this;
        }
        
        /**
         * Sets the proxy type.
         */
        public Builder proxyType(String proxyType) {
            this.proxyType = proxyType;
            return this;
        }

        /**
         * Builds the FTPConnectionConfig.
         */
        public FTPConnectionConfig build() {
            return new FTPConnectionConfig(this);
        }
    }

    /**
     * Enum for data size units used to convert data size strings to bytes.
     */
    public enum DataUnit {
        B(1L),
        KB(1024L),
        MB(1024L * 1024L),
        GB(1024L * 1024L * 1024L),
        TB(1024L * 1024L * 1024L * 1024L);

        private final long bytes;

        DataUnit(long bytes) {
            this.bytes = bytes;
        }

        public long getBytes() {
            return bytes;
        }
    }

    // Getters
    
    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getDataTimeout() {
        return dataTimeout;
    }

    public boolean isActiveMode() {
        return activeMode;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getMinConnections() {
        return minConnections;
    }

    public int getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public int getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public String getControlEncoding() {
        return controlEncoding;
    }

    public boolean isUseClientMode() {
        return useClientMode;
    }

    public boolean isUseImplicitSSL() {
        return useImplicitSSL;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public Integer getProxyPort() {
        return proxyPort;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }
    
    public int getControlTimeout() {
        return controlTimeout;
    }
    
    public String getTransferMode() {
        return transferMode;
    }
    
    public boolean isLocalActivePortRange() {
        return localActivePortRange;
    }
    
    public int getActivePortRangeStart() {
        return activePortRangeStart;
    }
    
    public int getActivePortRangeEnd() {
        return activePortRangeEnd;
    }
    
    public String getActiveExternalIPAddress() {
        return activeExternalIPAddress;
    }
    
    public boolean isUseExplicitSSL() {
        return useExplicitSSL;
    }
    
    public String[] getEnabledProtocols() {
        return enabledProtocols;
    }
    
    public String[] getEnabledCipherSuites() {
        return enabledCipherSuites;
    }
    
    public boolean isValidateServerCertificate() {
        return validateServerCertificate;
    }
    
    public String getTrustStorePath() {
        return trustStorePath;
    }
    
    public String getTrustStorePassword() {
        return trustStorePassword;
    }
    
    public String getTrustStoreType() {
        return trustStoreType;
    }
    
    public String getProxyType() {
        return proxyType;
    }
}