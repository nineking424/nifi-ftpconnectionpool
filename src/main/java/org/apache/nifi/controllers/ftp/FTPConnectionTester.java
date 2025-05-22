package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.controllers.ftp.exception.FTPErrorType;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSession;

/**
 * Utility class for comprehensive FTP connection testing and diagnostics.
 */
public class FTPConnectionTester {
    private final ComponentLog logger;
    
    /**
     * Class representing a test result with detailed diagnostics.
     */
    public static class TestResult {
        private final boolean success;
        private final String message;
        private final Map<String, Object> diagnostics;
        private final Instant timestamp;
        private final long durationMs;
        private final List<String> issues;
        private final List<String> recommendations;
        
        /**
         * Creates a new test result.
         * 
         * @param success whether the test was successful
         * @param message a description of the test result
         * @param diagnostics diagnostic information about the test
         * @param timestamp when the test was performed
         * @param durationMs how long the test took in milliseconds
         * @param issues list of identified issues
         * @param recommendations list of recommendations to resolve issues
         */
        public TestResult(boolean success, String message, Map<String, Object> diagnostics,
                          Instant timestamp, long durationMs, 
                          List<String> issues, List<String> recommendations) {
            this.success = success;
            this.message = message;
            this.diagnostics = diagnostics;
            this.timestamp = timestamp;
            this.durationMs = durationMs;
            this.issues = issues;
            this.recommendations = recommendations;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public String getMessage() {
            return message;
        }
        
        public Map<String, Object> getDiagnostics() {
            return diagnostics;
        }
        
        public Instant getTimestamp() {
            return timestamp;
        }
        
        public long getDurationMs() {
            return durationMs;
        }
        
        public List<String> getIssues() {
            return issues;
        }
        
        public List<String> getRecommendations() {
            return recommendations;
        }
        
        /**
         * Returns a map representation of the test result.
         */
        public Map<String, Object> toMap() {
            Map<String, Object> result = new HashMap<>();
            result.put("success", success);
            result.put("message", message);
            result.put("timestamp", timestamp.toString());
            result.put("durationMs", durationMs);
            result.put("diagnostics", diagnostics);
            result.put("issues", issues);
            result.put("recommendations", recommendations);
            return result;
        }
    }
    
    /**
     * Creates a new FTPConnectionTester with the given logger.
     *
     * @param logger the logger to use
     */
    public FTPConnectionTester(ComponentLog logger) {
        this.logger = logger;
    }
    
    /**
     * Performs a comprehensive test of an FTP connection using multiple diagnostics.
     *
     * @param connection the FTP connection to test
     * @param fullTest whether to perform a full test with file operations
     * @param timeoutMs the timeout for the test in milliseconds
     * @return the test result
     */
    public TestResult testConnection(FTPConnection connection, boolean fullTest, long timeoutMs) {
        if (connection == null) {
            return createFailureResult("Connection is null", null, 0);
        }
        
        FTPClient client = connection.getClient();
        if (client == null) {
            return createFailureResult("Connection has no client", 
                    Map.of("connectionId", connection.getId(),
                          "connectionState", connection.getState().name()), 
                    0);
        }
        
        // Start timing the test
        Instant startTime = Instant.now();
        
        try {
            // Run test with timeout
            return CompletableFuture.supplyAsync(() -> performConnectionTest(connection, client, fullTest))
                .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            
            // Create diagnostics for timeout
            Map<String, Object> diagnostics = new HashMap<>();
            diagnostics.put("connectionId", connection.getId());
            diagnostics.put("connectionState", connection.getState().name());
            diagnostics.put("hostname", connection.getConfig().getHostname());
            diagnostics.put("port", connection.getConfig().getPort());
            diagnostics.put("exceptionType", e.getClass().getSimpleName());
            diagnostics.put("exceptionMessage", e.getMessage());
            diagnostics.put("timeoutMs", timeoutMs);
            
            // Add details about the connection
            addConnectionDetails(diagnostics, connection);
            
            // Create issues and recommendations
            List<String> issues = new ArrayList<>();
            List<String> recommendations = new ArrayList<>();
            
            issues.add("Connection test timed out after " + timeoutMs + "ms");
            
            if (e.getMessage().contains("timeout")) {
                issues.add("Server response timeout");
                recommendations.add("Check network connectivity and server load");
                recommendations.add("Increase connection timeout settings");
            } else {
                issues.add("Test execution error: " + e.getMessage());
                recommendations.add("Check server availability");
                recommendations.add("Verify connection settings");
            }
            
            return new TestResult(false, "Connection test timed out or failed: " + e.getMessage(),
                    diagnostics, startTime, duration, issues, recommendations);
        }
    }
    
    /**
     * Performs the actual connection test with detailed diagnostics.
     */
    private TestResult performConnectionTest(FTPConnection connection, FTPClient client, boolean fullTest) {
        Instant startTime = Instant.now();
        Map<String, Object> diagnostics = new HashMap<>();
        List<String> issues = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        // Add connection details to diagnostics
        diagnostics.put("connectionId", connection.getId());
        diagnostics.put("connectionState", connection.getState().name());
        diagnostics.put("hostname", connection.getConfig().getHostname());
        diagnostics.put("port", connection.getConfig().getPort());
        
        // Add more details about the connection
        addConnectionDetails(diagnostics, connection);
        
        try {
            // First check if the client is connected
            if (!client.isConnected()) {
                issues.add("FTP client is not connected");
                recommendations.add("Reconnect the FTP client");
                
                long duration = Duration.between(startTime, Instant.now()).toMillis();
                return new TestResult(false, "FTP client is not connected",
                        diagnostics, startTime, duration, issues, recommendations);
            }
            
            // Test 1: Basic connectivity with NOOP
            boolean noopResult = false;
            try {
                diagnostics.put("test_noop_start", Instant.now().toString());
                noopResult = client.sendNoOp();
                int replyCode = client.getReplyCode();
                String replyString = client.getReplyString();
                
                diagnostics.put("test_noop_result", noopResult);
                diagnostics.put("test_noop_reply_code", replyCode);
                diagnostics.put("test_noop_reply_string", replyString);
                
                if (!noopResult) {
                    issues.add("NOOP command failed with reply code: " + replyCode);
                    recommendations.add("Check server status and connectivity");
                    
                    // Special handling for specific reply codes
                    if (replyCode == 421) {
                        issues.add("Server closed control connection (421)");
                        recommendations.add("Check server idle timeout settings");
                    } else if (replyCode >= 500 && replyCode < 600) {
                        issues.add("Server reported permanent error: " + replyString);
                        recommendations.add("Check server configuration and permissions");
                    }
                }
            } catch (IOException e) {
                diagnostics.put("test_noop_exception", e.getMessage());
                diagnostics.put("test_noop_exception_type", e.getClass().getSimpleName());
                
                issues.add("NOOP command failed with exception: " + e.getMessage());
                recommendations.add("Check network connectivity to the server");
                recommendations.add("Verify server is running and accepting connections");
            }
            
            // If NOOP failed and we don't want a full test, return early
            if (!noopResult && !fullTest) {
                long duration = Duration.between(startTime, Instant.now()).toMillis();
                return new TestResult(false, "Basic connectivity test failed",
                        diagnostics, startTime, duration, issues, recommendations);
            }
            
            // Test 2: Network level tests (if applicable)
            try {
                // Get socket information
                Socket socket = client.getSocket();
                if (socket != null) {
                    diagnostics.put("socket_local_address", socket.getLocalAddress().getHostAddress());
                    diagnostics.put("socket_local_port", socket.getLocalPort());
                    diagnostics.put("socket_remote_address", socket.getInetAddress().getHostAddress());
                    diagnostics.put("socket_remote_port", socket.getPort());
                    diagnostics.put("socket_keepalive", socket.getKeepAlive());
                    diagnostics.put("socket_so_timeout", socket.getSoTimeout());
                }
                
                // Try to resolve hostname to check DNS
                try {
                    String hostname = connection.getConfig().getHostname();
                    InetAddress[] addresses = InetAddress.getAllByName(hostname);
                    List<String> resolvedAddresses = new ArrayList<>();
                    
                    for (InetAddress address : addresses) {
                        resolvedAddresses.add(address.getHostAddress());
                    }
                    
                    diagnostics.put("hostname_resolution", resolvedAddresses);
                } catch (Exception e) {
                    diagnostics.put("hostname_resolution_error", e.getMessage());
                    issues.add("Failed to resolve hostname: " + e.getMessage());
                    recommendations.add("Check DNS configuration");
                }
            } catch (Exception e) {
                diagnostics.put("network_test_exception", e.getMessage());
            }
            
            // Stop here if not doing a full test or if NOOP failed
            if (!fullTest || !noopResult) {
                long duration = Duration.between(startTime, Instant.now()).toMillis();
                boolean success = noopResult;
                String message = success ? 
                        "Basic connectivity test succeeded" : 
                        "Basic connectivity test failed";
                
                return new TestResult(success, message, diagnostics, 
                        startTime, duration, issues, recommendations);
            }
            
            // Test 3: Directory operations
            boolean dirOpResult = false;
            try {
                diagnostics.put("test_dir_op_start", Instant.now().toString());
                
                // Get current directory
                String currentDir = client.printWorkingDirectory();
                diagnostics.put("test_current_dir", currentDir);
                
                // Try to list files
                diagnostics.put("test_list_start", Instant.now().toString());
                org.apache.commons.net.ftp.FTPFile[] files = client.listFiles();
                diagnostics.put("test_list_file_count", files != null ? files.length : 0);
                
                // Try to change to root directory and back
                boolean cdRootResult = client.changeWorkingDirectory("/");
                diagnostics.put("test_cd_root_result", cdRootResult);
                
                if (currentDir != null) {
                    boolean cdBackResult = client.changeWorkingDirectory(currentDir);
                    diagnostics.put("test_cd_back_result", cdBackResult);
                    
                    if (!cdBackResult) {
                        issues.add("Failed to change back to original directory");
                        recommendations.add("Check directory permissions");
                    }
                }
                
                dirOpResult = cdRootResult && files != null;
                
                if (!dirOpResult) {
                    issues.add("Directory operations test failed");
                    recommendations.add("Check directory permissions");
                    recommendations.add("Verify user has appropriate access rights");
                }
            } catch (IOException e) {
                diagnostics.put("test_dir_op_exception", e.getMessage());
                diagnostics.put("test_dir_op_exception_type", e.getClass().getSimpleName());
                
                issues.add("Directory operations failed with exception: " + e.getMessage());
                recommendations.add("Check directory permissions");
                recommendations.add("Verify user has appropriate access rights");
            }
            
            // Test 4: Test server features
            try {
                diagnostics.put("test_features_start", Instant.now().toString());
                
                // Get server features
                String[] features = client.featureValues();
                if (features != null) {
                    diagnostics.put("server_features", String.join(", ", features));
                }
                
                // Get server system type
                String systemType = client.getSystemType();
                diagnostics.put("server_system_type", systemType);
                
                // Check if server supports MLSD
                boolean supportsMlsd = client.hasFeature("MLSD");
                diagnostics.put("server_supports_mlsd", supportsMlsd);
                
                // Check if server supports MLST
                boolean supportsMlst = client.hasFeature("MLST");
                diagnostics.put("server_supports_mlst", supportsMlst);
                
                // Check if server supports UTF8
                boolean supportsUtf8 = client.hasFeature("UTF8");
                diagnostics.put("server_supports_utf8", supportsUtf8);
            } catch (IOException e) {
                diagnostics.put("test_features_exception", e.getMessage());
            }
            
            // Test 5: Final connectivity recheck
            boolean finalNoopResult = false;
            try {
                diagnostics.put("test_final_noop_start", Instant.now().toString());
                finalNoopResult = client.sendNoOp();
                diagnostics.put("test_final_noop_result", finalNoopResult);
                
                if (!finalNoopResult) {
                    issues.add("Final connectivity check failed");
                    recommendations.add("Connection may be unstable");
                }
            } catch (IOException e) {
                diagnostics.put("test_final_noop_exception", e.getMessage());
                
                issues.add("Final connectivity check failed with exception: " + e.getMessage());
                recommendations.add("Connection may be unstable");
            }
            
            // Calculate overall success
            boolean overallSuccess = noopResult && (finalNoopResult || !fullTest);
            if (fullTest) {
                overallSuccess = overallSuccess && dirOpResult;
            }
            
            // If we have no issues but tests failed, add a generic issue
            if (issues.isEmpty() && !overallSuccess) {
                issues.add("Connection test failed but no specific issues were identified");
                recommendations.add("Check server logs for more information");
            }
            
            // Prepare result
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            String message = overallSuccess ? 
                    "Connection test " + (fullTest ? "fully " : "") + "succeeded" : 
                    "Connection test " + (fullTest ? "fully " : "") + "failed";
            
            return new TestResult(overallSuccess, message, diagnostics, 
                    startTime, duration, issues, recommendations);
            
        } catch (Exception e) {
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            diagnostics.put("unexpected_exception", e.getMessage());
            diagnostics.put("unexpected_exception_type", e.getClass().getSimpleName());
            
            issues.add("Unexpected error during connection test: " + e.getMessage());
            recommendations.add("Check server configuration");
            recommendations.add("Verify network connectivity");
            
            return new TestResult(false, "Connection test failed with unexpected error: " + e.getMessage(),
                    diagnostics, startTime, duration, issues, recommendations);
        }
    }
    
    /**
     * Adds detailed connection information to the diagnostics map.
     */
    private void addConnectionDetails(Map<String, Object> diagnostics, FTPConnection connection) {
        try {
            // Add connection details
            FTPConnectionConfig config = connection.getConfig();
            diagnostics.put("username", config.getUsername());
            diagnostics.put("active_mode", config.isActiveMode());
            diagnostics.put("use_implicit_ssl", config.isUseImplicitSSL());
            diagnostics.put("use_explicit_ssl", config.isUseExplicitSSL());
            diagnostics.put("connection_timeout", config.getConnectionTimeout());
            diagnostics.put("data_timeout", config.getDataTimeout());
            diagnostics.put("control_timeout", config.getControlTimeout());
            diagnostics.put("buffer_size", config.getBufferSize());
            diagnostics.put("control_encoding", config.getControlEncoding());
            diagnostics.put("keep_alive_interval", config.getKeepAliveInterval());
            diagnostics.put("transfer_mode", config.getTransferMode());
            
            // Add active mode configuration if applicable
            if (config.isActiveMode()) {
                Map<String, Object> activeModeInfo = new HashMap<>();
                activeModeInfo.put("using_port_range", config.isLocalActivePortRange());
                if (config.isLocalActivePortRange()) {
                    activeModeInfo.put("port_range_start", config.getActivePortRangeStart());
                    activeModeInfo.put("port_range_end", config.getActivePortRangeEnd());
                }
                activeModeInfo.put("external_ip", config.getActiveExternalIPAddress());
                diagnostics.put("active_mode_config", activeModeInfo);
            }
            
            // Add SSL/TLS information if configured
            if (config.isUseImplicitSSL() || config.isUseExplicitSSL()) {
                Map<String, Object> sslInfo = new HashMap<>();
                sslInfo.put("use_client_mode", config.isUseClientMode());
                sslInfo.put("validate_server_cert", config.isValidateServerCertificate());
                
                if (config.getEnabledProtocols() != null) {
                    sslInfo.put("enabled_protocols", String.join(", ", config.getEnabledProtocols()));
                }
                
                if (config.getEnabledCipherSuites() != null) {
                    sslInfo.put("enabled_cipher_suites", String.join(", ", config.getEnabledCipherSuites()));
                }
                
                if (config.getTrustStorePath() != null) {
                    sslInfo.put("trust_store_path", config.getTrustStorePath());
                    sslInfo.put("trust_store_type", config.getTrustStoreType());
                }
                
                diagnostics.put("ssl_config", sslInfo);
            }
            
            // Add proxy information if configured
            if (config.getProxyHost() != null && !config.getProxyHost().isEmpty()) {
                Map<String, Object> proxyInfo = new HashMap<>();
                proxyInfo.put("host", config.getProxyHost());
                proxyInfo.put("port", config.getProxyPort());
                proxyInfo.put("username", config.getProxyUsername());
                proxyInfo.put("type", config.getProxyType());
                diagnostics.put("proxy_info", proxyInfo);
            }
            
            // Add connection timing information
            diagnostics.put("created_at", connection.getCreatedAt().toString());
            
            if (connection.getLastUsedAt() != null) {
                diagnostics.put("last_used_at", connection.getLastUsedAt().toString());
                diagnostics.put("time_since_last_used_ms", 
                        Duration.between(connection.getLastUsedAt(), Instant.now()).toMillis());
            }
            
            if (connection.getLastTestedAt() != null) {
                diagnostics.put("last_tested_at", connection.getLastTestedAt().toString());
                diagnostics.put("time_since_last_tested_ms", 
                        Duration.between(connection.getLastTestedAt(), Instant.now()).toMillis());
            }
            
            // Add reconnection information
            diagnostics.put("reconnect_attempts", connection.getReconnectAttempts());
            diagnostics.put("last_error_message", connection.getLastErrorMessage());
            
        } catch (Exception e) {
            diagnostics.put("connection_details_error", "Failed to gather connection details: " + e.getMessage());
        }
    }
    
    /**
     * Creates a failure result with minimal information.
     */
    private TestResult createFailureResult(String message, Map<String, Object> extraDiagnostics, long durationMs) {
        Instant timestamp = Instant.now();
        Map<String, Object> diagnostics = new HashMap<>();
        
        if (extraDiagnostics != null) {
            diagnostics.putAll(extraDiagnostics);
        }
        
        List<String> issues = new ArrayList<>();
        issues.add(message);
        
        List<String> recommendations = new ArrayList<>();
        recommendations.add("Check connection configuration");
        
        return new TestResult(false, message, diagnostics, timestamp, durationMs, issues, recommendations);
    }
    
    /**
     * Tests the SSL/TLS configuration of an FTPS connection.
     * Verifies the negotiated protocol version, cipher suite, and certificate validation.
     *
     * @param connection the FTP connection to test
     * @return the test result
     */
    public TestResult testSSLConfiguration(FTPConnection connection) {
        if (connection == null) {
            return createFailureResult("Connection is null", null, 0);
        }
        
        FTPConnectionConfig config = connection.getConfig();
        if (!config.isUseImplicitSSL() && !config.isUseExplicitSSL()) {
            return createFailureResult("Connection is not configured for SSL/TLS",
                    Map.of("connectionId", connection.getId(),
                           "connectionState", connection.getState().name()),
                    0);
        }
        
        Instant startTime = Instant.now();
        Map<String, Object> diagnostics = new HashMap<>();
        List<String> issues = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        // Add basic connection details
        diagnostics.put("connectionId", connection.getId());
        diagnostics.put("connectionState", connection.getState().name());
        diagnostics.put("hostname", config.getHostname());
        diagnostics.put("port", config.getPort());
        diagnostics.put("use_implicit_ssl", config.isUseImplicitSSL());
        diagnostics.put("use_explicit_ssl", config.isUseExplicitSSL());
        
        try {
            // Get the underlying socket which should be an SSLSocket for FTPS connections
            if (connection.getClient() != null && connection.getClient().isConnected()) {
                Socket socket = connection.getClient().getSocket();
                
                if (socket instanceof SSLSocket) {
                    SSLSocket sslSocket = (SSLSocket) socket;
                    SSLSession session = sslSocket.getSession();
                    
                    // Add SSL session details
                    diagnostics.put("ssl_protocol", session.getProtocol());
                    diagnostics.put("ssl_cipher_suite", session.getCipherSuite());
                    diagnostics.put("peer_host", session.getPeerHost());
                    diagnostics.put("peer_port", session.getPeerPort());
                    
                    // Check certificate validation if enabled
                    if (config.isValidateServerCertificate()) {
                        try {
                            // Get the server certificate chain
                            java.security.cert.Certificate[] certs = session.getPeerCertificates();
                            if (certs != null && certs.length > 0) {
                                diagnostics.put("certificate_count", certs.length);
                                
                                // Get first certificate details
                                if (certs[0] instanceof java.security.cert.X509Certificate) {
                                    java.security.cert.X509Certificate x509 = 
                                            (java.security.cert.X509Certificate) certs[0];
                                    
                                    diagnostics.put("certificate_subject", x509.getSubjectX500Principal().getName());
                                    diagnostics.put("certificate_issuer", x509.getIssuerX500Principal().getName());
                                    diagnostics.put("certificate_valid_from", x509.getNotBefore());
                                    diagnostics.put("certificate_valid_until", x509.getNotAfter());
                                    
                                    // Check certificate expiration
                                    if (x509.getNotAfter().before(new java.util.Date())) {
                                        issues.add("Server certificate has expired");
                                        recommendations.add("Update the server certificate");
                                    }
                                }
                            } else {
                                issues.add("No certificates received from server");
                                recommendations.add("Check server SSL/TLS configuration");
                            }
                        } catch (javax.net.ssl.SSLPeerUnverifiedException e) {
                            diagnostics.put("certificate_verification_error", e.getMessage());
                            issues.add("Failed to verify server certificate: " + e.getMessage());
                            recommendations.add("Check truststore configuration");
                            recommendations.add("Verify server certificate is trusted");
                        }
                    }
                    
                    // Check protocol and cipher suite against configured values
                    if (config.getEnabledProtocols() != null && config.getEnabledProtocols().length > 0) {
                        boolean protocolMatched = false;
                        for (String protocol : config.getEnabledProtocols()) {
                            if (session.getProtocol().equalsIgnoreCase(protocol)) {
                                protocolMatched = true;
                                break;
                            }
                        }
                        
                        if (!protocolMatched) {
                            issues.add("Negotiated protocol " + session.getProtocol() + 
                                    " not in configured enabled protocols");
                            recommendations.add("Review enabled protocols configuration");
                        }
                    }
                    
                    if (config.getEnabledCipherSuites() != null && config.getEnabledCipherSuites().length > 0) {
                        boolean cipherMatched = false;
                        for (String cipher : config.getEnabledCipherSuites()) {
                            if (session.getCipherSuite().equalsIgnoreCase(cipher)) {
                                cipherMatched = true;
                                break;
                            }
                        }
                        
                        if (!cipherMatched) {
                            issues.add("Negotiated cipher suite " + session.getCipherSuite() + 
                                    " not in configured enabled cipher suites");
                            recommendations.add("Review enabled cipher suites configuration");
                        }
                    }
                    
                    // Add success status if no issues were found
                    if (issues.isEmpty()) {
                        diagnostics.put("ssl_negotiation_successful", true);
                    }
                } else {
                    issues.add("Connection socket is not an SSL socket");
                    if (config.isUseExplicitSSL()) {
                        recommendations.add("Explicit SSL might not have been negotiated. Check server logs");
                    } else {
                        recommendations.add("Check SSL/TLS configuration on both client and server");
                    }
                }
            } else {
                issues.add("Connection is not established or client is null");
                recommendations.add("Establish connection before testing SSL configuration");
            }
        } catch (Exception e) {
            diagnostics.put("error", e.getMessage());
            diagnostics.put("error_type", e.getClass().getSimpleName());
            issues.add("Error testing SSL configuration: " + e.getMessage());
            recommendations.add("Check SSL/TLS configuration");
        }
        
        long duration = Duration.between(startTime, Instant.now()).toMillis();
        boolean success = issues.isEmpty();
        String message = success ? "SSL/TLS configuration test successful" : 
                "SSL/TLS configuration test failed"; 
        
        return new TestResult(success, message, diagnostics, startTime, duration, issues, recommendations);
    }
    
    /**
     * Tests the proxy configuration by verifying connectivity through the configured proxy.
     * 
     * @param config the FTP connection configuration
     * @param timeoutMs the timeout for the test in milliseconds
     * @return the test result
     */
    public TestResult testProxyConfiguration(FTPConnectionConfig config, int timeoutMs) {
        if (config == null) {
            return createFailureResult("Configuration is null", null, 0);
        }
        
        if (config.getProxyHost() == null || config.getProxyHost().isEmpty() || config.getProxyPort() == null) {
            return createFailureResult("No proxy configured", 
                    Map.of("hostname", config.getHostname(),
                           "port", config.getPort()),
                    0);
        }
        
        Instant startTime = Instant.now();
        Map<String, Object> diagnostics = new HashMap<>();
        List<String> issues = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        // Add basic configuration details
        diagnostics.put("hostname", config.getHostname());
        diagnostics.put("port", config.getPort());
        diagnostics.put("proxy_host", config.getProxyHost());
        diagnostics.put("proxy_port", config.getProxyPort());
        diagnostics.put("proxy_type", config.getProxyType());
        diagnostics.put("proxy_username", config.getProxyUsername());
        diagnostics.put("timeout_ms", timeoutMs);
        
        try {
            // First test if the proxy server itself is reachable
            diagnostics.put("proxy_server_test_start", Instant.now().toString());
            
            TestResult proxyReachabilityResult = testServerReachability(
                    config.getProxyHost(), config.getProxyPort(), timeoutMs / 2);
            
            diagnostics.put("proxy_server_reachable", proxyReachabilityResult.isSuccess());
            
            if (!proxyReachabilityResult.isSuccess()) {
                issues.add("Proxy server is not reachable");
                issues.addAll(proxyReachabilityResult.getIssues());
                recommendations.add("Check proxy server availability");
                recommendations.addAll(proxyReachabilityResult.getRecommendations());
                
                long duration = Duration.between(startTime, Instant.now()).toMillis();
                return new TestResult(false, "Proxy server is not reachable",
                        diagnostics, startTime, duration, issues, recommendations);
            }
            
            // Now test connecting through the proxy to the target server
            // This requires creating an actual connection attempt
            diagnostics.put("proxy_connection_test_start", Instant.now().toString());
            
            try {
                // Create a proxy client for testing
                org.apache.commons.net.ProxyClient proxyClient = new org.apache.commons.net.ProxyClient();
                
                // Set proxy credentials if available
                if (config.getProxyUsername() != null && !config.getProxyUsername().isEmpty()) {
                    proxyClient.setProxyCredentials(config.getProxyUsername(), config.getProxyPassword());
                }
                
                // Determine proxy type
                org.apache.commons.net.ProxyClient.Type proxyType = org.apache.commons.net.ProxyClient.Type.SOCKS_V5;
                if (config.getProxyType() != null) {
                    switch (config.getProxyType().toUpperCase()) {
                        case "HTTP":
                            proxyType = org.apache.commons.net.ProxyClient.Type.HTTP;
                            break;
                        case "SOCKS":
                        case "SOCKS4":
                        case "SOCKS4A":
                            proxyType = org.apache.commons.net.ProxyClient.Type.SOCKS_V4;
                            break;
                        case "SOCKS5":
                        default:
                            proxyType = org.apache.commons.net.ProxyClient.Type.SOCKS_V5;
                            break;
                    }
                }
                
                // Try to connect through the proxy
                Socket socket = null;
                try {
                    diagnostics.put("proxy_connect_start", Instant.now().toString());
                    
                    socket = proxyClient.connectForDataTransferProxy(
                            proxyType,
                            config.getProxyHost(),
                            config.getProxyPort(),
                            config.getHostname(),
                            config.getPort(),
                            timeoutMs);
                    
                    if (socket != null && socket.isConnected()) {
                        diagnostics.put("proxy_connection_successful", true);
                        diagnostics.put("local_address", socket.getLocalAddress().getHostAddress());
                        diagnostics.put("local_port", socket.getLocalPort());
                        diagnostics.put("remote_address", socket.getInetAddress().getHostAddress());
                        diagnostics.put("remote_port", socket.getPort());
                    } else {
                        diagnostics.put("proxy_connection_successful", false);
                        issues.add("Failed to connect through proxy");
                        recommendations.add("Check proxy configuration and permissions");
                    }
                } finally {
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            // Ignore close errors
                        }
                    }
                }
            } catch (IOException e) {
                diagnostics.put("proxy_connection_error", e.getMessage());
                diagnostics.put("proxy_connection_error_type", e.getClass().getSimpleName());
                
                issues.add("Failed to connect through proxy: " + e.getMessage());
                
                if (e.getMessage().contains("authentication") || e.getMessage().contains("Authentication")) {
                    recommendations.add("Check proxy authentication credentials");
                } else if (e.getMessage().contains("timed out") || e.getMessage().contains("timeout")) {
                    recommendations.add("Increase connection timeout");
                    recommendations.add("Check if proxy allows connections to the target server");
                } else if (e.getMessage().contains("Connection refused")) {
                    recommendations.add("Check if target server is accessible from the proxy");
                } else {
                    recommendations.add("Check proxy configuration");
                    recommendations.add("Verify proxy allows connections to the target host and port");
                }
            }
            
        } catch (Exception e) {
            diagnostics.put("error", e.getMessage());
            diagnostics.put("error_type", e.getClass().getSimpleName());
            
            issues.add("Unexpected error during proxy test: " + e.getMessage());
            recommendations.add("Check proxy configuration");
        }
        
        long duration = Duration.between(startTime, Instant.now()).toMillis();
        boolean success = issues.isEmpty();
        String message = success ? 
                "Proxy configuration test successful" : 
                "Proxy configuration test failed";
        
        return new TestResult(success, message, diagnostics, startTime, duration, issues, recommendations);
    }
    
    /**
     * Tests the active mode configuration by attempting to establish a data connection.
     * 
     * @param connection the FTP connection to test
     * @return the test result
     */
    public TestResult testActiveModeConfiguration(FTPConnection connection) {
        if (connection == null) {
            return createFailureResult("Connection is null", null, 0);
        }
        
        FTPConnectionConfig config = connection.getConfig();
        if (!config.isActiveMode()) {
            return createFailureResult("Connection is not configured for active mode",
                    Map.of("connectionId", connection.getId(),
                           "connectionState", connection.getState().name()),
                    0);
        }
        
        Instant startTime = Instant.now();
        Map<String, Object> diagnostics = new HashMap<>();
        List<String> issues = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        // Add basic connection details
        diagnostics.put("connectionId", connection.getId());
        diagnostics.put("connectionState", connection.getState().name());
        diagnostics.put("hostname", config.getHostname());
        diagnostics.put("port", config.getPort());
        
        // Add active mode configuration details
        diagnostics.put("active_mode", true);
        diagnostics.put("using_port_range", config.isLocalActivePortRange());
        if (config.isLocalActivePortRange()) {
            diagnostics.put("port_range_start", config.getActivePortRangeStart());
            diagnostics.put("port_range_end", config.getActivePortRangeEnd());
        }
        diagnostics.put("external_ip", config.getActiveExternalIPAddress());
        
        try {
            // Check if the connection is established
            if (connection.getClient() == null || !connection.getClient().isConnected()) {
                issues.add("Connection is not established");
                recommendations.add("Establish connection before testing active mode configuration");
                
                long duration = Duration.between(startTime, Instant.now()).toMillis();
                return new TestResult(false, "Connection is not established",
                        diagnostics, startTime, duration, issues, recommendations);
            }
            
            // Try to list files to test data connection in active mode
            FTPClient client = connection.getClient();
            try {
                diagnostics.put("list_start", Instant.now().toString());
                
                // Force active mode
                client.enterLocalActiveMode();
                
                // Try listing files which will establish a data connection
                org.apache.commons.net.ftp.FTPFile[] files = client.listFiles();
                
                diagnostics.put("list_success", true);
                diagnostics.put("file_count", files != null ? files.length : 0);
                diagnostics.put("reply_code", client.getReplyCode());
                diagnostics.put("reply_string", client.getReplyString());
                
                // Check for any PORT command errors in the reply
                String replyString = client.getReplyString();
                if (replyString != null && 
                        (replyString.contains("PORT") || replyString.contains("port")) &&
                        (replyString.contains("fail") || replyString.contains("error") || 
                         replyString.contains("denied"))) {
                    
                    issues.add("PORT command error: " + replyString);
                    
                    if (replyString.contains("refused") || replyString.contains("connection")) {
                        recommendations.add("Check firewall settings for incoming connections");
                        recommendations.add("Verify active port range is open in the firewall");
                    } else if (replyString.contains("address")) {
                        recommendations.add("Check the configured external IP address");
                    } else {
                        recommendations.add("Check server configuration for active mode");
                    }
                }
                
            } catch (IOException e) {
                diagnostics.put("active_mode_error", e.getMessage());
                diagnostics.put("active_mode_error_type", e.getClass().getSimpleName());
                diagnostics.put("reply_code", client.getReplyCode());
                diagnostics.put("reply_string", client.getReplyString());
                
                issues.add("Active mode data connection failed: " + e.getMessage());
                
                // Provide specific recommendations based on the error
                if (e.getMessage().contains("Connection refused")) {
                    recommendations.add("Check firewall settings for incoming connections");
                    recommendations.add("Ensure active port range is open in the firewall");
                } else if (e.getMessage().contains("timed out") || e.getMessage().contains("timeout")) {
                    recommendations.add("Check firewall settings for blocking connections");
                    recommendations.add("Increase data connection timeout");
                } else if (e.getMessage().contains("address") || e.getMessage().contains("Address")) {
                    recommendations.add("Verify the configured external IP address is correct");
                    recommendations.add("Check network configuration for NAT/PAT setup");
                } else {
                    recommendations.add("Check server logs for more information");
                    recommendations.add("Consider using passive mode if active mode is problematic");
                }
            }
            
        } catch (Exception e) {
            diagnostics.put("error", e.getMessage());
            diagnostics.put("error_type", e.getClass().getSimpleName());
            
            issues.add("Unexpected error during active mode test: " + e.getMessage());
            recommendations.add("Check active mode configuration");
        }
        
        long duration = Duration.between(startTime, Instant.now()).toMillis();
        boolean success = issues.isEmpty();
        String message = success ? 
                "Active mode configuration test successful" : 
                "Active mode configuration test failed";
        
        return new TestResult(success, message, diagnostics, startTime, duration, issues, recommendations);
    }
    
    /**
     * Tests if a server is reachable at the network level without establishing a full FTP connection.
     *
     * @param hostname the server hostname
     * @param port the server port
     * @param timeoutMs the connection timeout in milliseconds
     * @return the test result
     */
    public TestResult testServerReachability(String hostname, int port, int timeoutMs) {
        Instant startTime = Instant.now();
        Map<String, Object> diagnostics = new HashMap<>();
        List<String> issues = new ArrayList<>();
        List<String> recommendations = new ArrayList<>();
        
        diagnostics.put("hostname", hostname);
        diagnostics.put("port", port);
        diagnostics.put("timeout_ms", timeoutMs);
        
        try {
            // Try to resolve the hostname
            diagnostics.put("dns_resolution_start", Instant.now().toString());
            InetAddress[] addresses = InetAddress.getAllByName(hostname);
            List<String> resolvedAddresses = new ArrayList<>();
            
            for (InetAddress address : addresses) {
                resolvedAddresses.add(address.getHostAddress());
            }
            
            diagnostics.put("resolved_addresses", resolvedAddresses);
            diagnostics.put("dns_resolution_success", true);
            
            // Try to connect to the server
            diagnostics.put("socket_connection_start", Instant.now().toString());
            
            Socket socket = new Socket();
            socket.connect(new java.net.InetSocketAddress(hostname, port), timeoutMs);
            
            diagnostics.put("socket_connection_success", true);
            diagnostics.put("local_address", socket.getLocalAddress().getHostAddress());
            diagnostics.put("local_port", socket.getLocalPort());
            
            // Close the socket
            socket.close();
            
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            return new TestResult(true, "Server is reachable", diagnostics, 
                    startTime, duration, issues, recommendations);
            
        } catch (java.net.UnknownHostException e) {
            // DNS resolution failed
            diagnostics.put("dns_resolution_success", false);
            diagnostics.put("dns_resolution_error", e.getMessage());
            
            issues.add("DNS resolution failed: " + e.getMessage());
            recommendations.add("Check hostname spelling");
            recommendations.add("Verify DNS configuration");
            
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            return new TestResult(false, "Server hostname cannot be resolved",
                    diagnostics, startTime, duration, issues, recommendations);
            
        } catch (java.net.SocketTimeoutException e) {
            // Connection timed out
            diagnostics.put("socket_connection_success", false);
            diagnostics.put("socket_connection_error", e.getMessage());
            
            issues.add("Connection timed out: " + e.getMessage());
            recommendations.add("Check firewall settings");
            recommendations.add("Verify server is running on specified port");
            recommendations.add("Increase connection timeout");
            
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            return new TestResult(false, "Connection to server timed out",
                    diagnostics, startTime, duration, issues, recommendations);
            
        } catch (java.net.ConnectException e) {
            // Connection refused
            diagnostics.put("socket_connection_success", false);
            diagnostics.put("socket_connection_error", e.getMessage());
            
            issues.add("Connection refused: " + e.getMessage());
            recommendations.add("Verify server is running on specified port");
            recommendations.add("Check firewall settings");
            
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            return new TestResult(false, "Connection to server refused",
                    diagnostics, startTime, duration, issues, recommendations);
            
        } catch (Exception e) {
            // Other error
            diagnostics.put("error", e.getMessage());
            diagnostics.put("error_type", e.getClass().getSimpleName());
            
            issues.add("Unexpected error: " + e.getMessage());
            recommendations.add("Check network connectivity");
            
            long duration = Duration.between(startTime, Instant.now()).toMillis();
            return new TestResult(false, "Error testing server reachability: " + e.getMessage(),
                    diagnostics, startTime, duration, issues, recommendations);
        }
    }
}