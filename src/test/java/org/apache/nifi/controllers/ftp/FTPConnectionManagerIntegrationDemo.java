package org.apache.nifi.controllers.ftp;

import org.apache.nifi.logging.ComponentLog;

/**
 * This class demonstrates how to use the enhanced FTPConnectionManager with
 * the new configuration options. This is not a test that's meant to be run
 * automatically, but rather a guide for integrating with the enhanced manager.
 */
public class FTPConnectionManagerIntegrationDemo {

    /**
     * Demonstrates how to create and use an FTPConnectionManager with standard FTP.
     */
    public static void demonstrateStandardFTP(ComponentLog logger) {
        // Create configuration for standard FTP
        FTPConnectionConfig config = new FTPConnectionConfig.Builder()
                .hostname("ftp.example.com")
                .port(21)
                .username("user")
                .password("password")
                .activeMode(false) // Use passive mode (default and recommended)
                .connectionTimeout(30000) // 30 seconds
                .dataTimeout(60000) // 60 seconds
                .keepAliveInterval(60000) // 60 seconds
                .bufferSize(1024 * 1024) // 1MB buffer
                .controlEncoding("UTF-8")
                .transferMode("Binary") // Use binary transfer mode
                .build();
        
        // Create the connection manager
        FTPConnectionManager manager = new FTPConnectionManager(config, logger);
        
        try {
            // Get a connection from the manager
            FTPConnection connection = manager.createManagedConnection();
            
            // Use the connection...
            
            // Return the connection to the manager
            manager.closeConnection(connection);
        } catch (Exception e) {
            logger.error("Error using FTP connection: {}", new Object[] { e.getMessage() }, e);
        } finally {
            // Shutdown the manager when done
            manager.shutdown();
        }
    }
    
    /**
     * Demonstrates how to create and use an FTPConnectionManager with implicit FTPS.
     */
    public static void demonstrateImplicitFTPS(ComponentLog logger) {
        // Create configuration for implicit FTPS (FTPS on port 990)
        FTPConnectionConfig config = new FTPConnectionConfig.Builder()
                .hostname("ftps.example.com")
                .port(990) // Standard implicit FTPS port
                .username("user")
                .password("password")
                .activeMode(false) // Use passive mode
                .connectionTimeout(30000)
                .dataTimeout(60000)
                .useImplicitSSL(true) // Enable implicit SSL/TLS
                .useClientMode(true)
                // Enable only strong protocols
                .enabledProtocols(new String[] { "TLSv1.2", "TLSv1.3" })
                // Enable only strong cipher suites
                .enabledCipherSuites(new String[] {
                    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
                })
                .validateServerCertificate(true) // Validate server certificate
                .trustStorePath("/path/to/truststore.jks") // Custom truststore
                .trustStorePassword("truststore_password")
                .trustStoreType("JKS")
                .transferMode("Binary")
                .build();
        
        // Create the connection manager
        FTPConnectionManager manager = new FTPConnectionManager(config, logger);
        
        try {
            // Get a connection from the manager
            FTPConnection connection = manager.createManagedConnection();
            
            // Use the connection...
            
            // Return the connection to the manager
            manager.closeConnection(connection);
        } catch (Exception e) {
            logger.error("Error using FTPS connection: {}", new Object[] { e.getMessage() }, e);
        } finally {
            // Shutdown the manager when done
            manager.shutdown();
        }
    }
    
    /**
     * Demonstrates how to create and use an FTPConnectionManager with explicit FTPS.
     */
    public static void demonstrateExplicitFTPS(ComponentLog logger) {
        // Create configuration for explicit FTPS (FTP with STARTTLS)
        FTPConnectionConfig config = new FTPConnectionConfig.Builder()
                .hostname("ftp.example.com")
                .port(21) // Standard FTP port, but will use explicit SSL/TLS
                .username("user")
                .password("password")
                .activeMode(false) // Use passive mode
                .connectionTimeout(30000)
                .dataTimeout(60000)
                .useImplicitSSL(false)
                .useExplicitSSL(true) // Enable explicit SSL/TLS (STARTTLS)
                .useClientMode(true)
                .validateServerCertificate(true)
                // Using system default truststore
                .transferMode("Binary")
                .build();
        
        // Create the connection manager
        FTPConnectionManager manager = new FTPConnectionManager(config, logger);
        
        try {
            // Get a connection from the manager
            FTPConnection connection = manager.createManagedConnection();
            
            // Use the connection...
            
            // Return the connection to the manager
            manager.closeConnection(connection);
        } catch (Exception e) {
            logger.error("Error using explicit FTPS connection: {}", new Object[] { e.getMessage() }, e);
        } finally {
            // Shutdown the manager when done
            manager.shutdown();
        }
    }
    
    /**
     * Demonstrates how to create and use an FTPConnectionManager with active mode.
     */
    public static void demonstrateActiveModeWithPortRange(ComponentLog logger) {
        // Create configuration for FTP with active mode and port range
        FTPConnectionConfig config = new FTPConnectionConfig.Builder()
                .hostname("ftp.example.com")
                .port(21)
                .username("user")
                .password("password")
                .activeMode(true) // Use active mode
                .localActivePortRange(true) // Use specific port range
                .activePortRangeStart(40000) // Start of port range
                .activePortRangeEnd(50000) // End of port range
                .activeExternalIPAddress("203.0.113.10") // External IP for NAT
                .connectionTimeout(30000)
                .dataTimeout(60000)
                .transferMode("Binary")
                .build();
        
        // Create the connection manager
        FTPConnectionManager manager = new FTPConnectionManager(config, logger);
        
        try {
            // Get a connection from the manager
            FTPConnection connection = manager.createManagedConnection();
            
            // Use the connection...
            
            // Return the connection to the manager
            manager.closeConnection(connection);
        } catch (Exception e) {
            logger.error("Error using active mode FTP connection: {}", new Object[] { e.getMessage() }, e);
        } finally {
            // Shutdown the manager when done
            manager.shutdown();
        }
    }
    
    /**
     * Demonstrates how to create and use an FTPConnectionManager with proxy.
     */
    public static void demonstrateProxyConfiguration(ComponentLog logger) {
        // Create configuration for FTP through a proxy
        FTPConnectionConfig config = new FTPConnectionConfig.Builder()
                .hostname("ftp.example.com")
                .port(21)
                .username("user")
                .password("password")
                .activeMode(false) // Use passive mode (active mode usually doesn't work through proxies)
                .connectionTimeout(30000)
                .dataTimeout(60000)
                .proxyHost("proxy.example.com") // Proxy server hostname
                .proxyPort(8080) // Proxy server port
                .proxyUsername("proxy_user") // Optional proxy authentication
                .proxyPassword("proxy_password")
                .proxyType("SOCKS5") // Proxy type: HTTP, SOCKS4, or SOCKS5
                .transferMode("Binary")
                .build();
        
        // Create the connection manager
        FTPConnectionManager manager = new FTPConnectionManager(config, logger);
        
        try {
            // Get a connection from the manager
            FTPConnection connection = manager.createManagedConnection();
            
            // Use the connection...
            
            // Return the connection to the manager
            manager.closeConnection(connection);
        } catch (Exception e) {
            logger.error("Error using proxied FTP connection: {}", new Object[] { e.getMessage() }, e);
        } finally {
            // Shutdown the manager when done
            manager.shutdown();
        }
    }
    
    /**
     * Demonstrates how to test a connection's configuration.
     */
    public static void demonstrateConnectionTesting(ComponentLog logger) {
        // Create a standard configuration
        FTPConnectionConfig config = new FTPConnectionConfig.Builder()
                .hostname("ftp.example.com")
                .port(21)
                .username("user")
                .password("password")
                .build();
        
        // Create the connection manager
        FTPConnectionManager manager = new FTPConnectionManager(config, logger);
        FTPConnectionTester tester = new FTPConnectionTester(logger);
        
        try {
            // First test server reachability
            FTPConnectionTester.TestResult reachabilityResult = 
                    tester.testServerReachability(config.getHostname(), config.getPort(), 5000);
            
            if (reachabilityResult.isSuccess()) {
                logger.info("Server is reachable, proceeding with connection test");
                
                // Get a connection
                FTPConnection connection = manager.createManagedConnection();
                
                // Test the connection
                FTPConnectionTester.TestResult connectionResult = 
                        tester.testConnection(connection, true, 10000);
                
                if (connectionResult.isSuccess()) {
                    logger.info("Connection test successful");
                    
                    // If using SSL/TLS, test SSL configuration
                    if (config.isUseImplicitSSL() || config.isUseExplicitSSL()) {
                        FTPConnectionTester.TestResult sslResult = 
                                tester.testSSLConfiguration(connection);
                        
                        if (sslResult.isSuccess()) {
                            logger.info("SSL/TLS configuration test successful");
                        } else {
                            logger.warn("SSL/TLS configuration test failed: {}", 
                                    sslResult.getMessage());
                            
                            // Log issues and recommendations
                            for (String issue : sslResult.getIssues()) {
                                logger.warn("SSL Issue: {}", issue);
                            }
                            
                            for (String recommendation : sslResult.getRecommendations()) {
                                logger.info("SSL Recommendation: {}", recommendation);
                            }
                        }
                    }
                    
                    // If using active mode, test active mode configuration
                    if (config.isActiveMode()) {
                        FTPConnectionTester.TestResult activeModeResult = 
                                tester.testActiveModeConfiguration(connection);
                        
                        if (activeModeResult.isSuccess()) {
                            logger.info("Active mode configuration test successful");
                        } else {
                            logger.warn("Active mode configuration test failed: {}", 
                                    activeModeResult.getMessage());
                            
                            // Log issues and recommendations
                            for (String issue : activeModeResult.getIssues()) {
                                logger.warn("Active Mode Issue: {}", issue);
                            }
                            
                            for (String recommendation : activeModeResult.getRecommendations()) {
                                logger.info("Active Mode Recommendation: {}", recommendation);
                            }
                        }
                    }
                } else {
                    logger.error("Connection test failed: {}", connectionResult.getMessage());
                    
                    // Log issues and recommendations
                    for (String issue : connectionResult.getIssues()) {
                        logger.warn("Issue: {}", issue);
                    }
                    
                    for (String recommendation : connectionResult.getRecommendations()) {
                        logger.info("Recommendation: {}", recommendation);
                    }
                }
                
                // Close the connection
                manager.closeConnection(connection);
            } else {
                logger.error("Server is not reachable: {}", reachabilityResult.getMessage());
                
                // Log issues and recommendations
                for (String issue : reachabilityResult.getIssues()) {
                    logger.warn("Issue: {}", issue);
                }
                
                for (String recommendation : reachabilityResult.getRecommendations()) {
                    logger.info("Recommendation: {}", recommendation);
                }
            }
        } catch (Exception e) {
            logger.error("Error testing connection: {}", new Object[] { e.getMessage() }, e);
        } finally {
            // Shutdown the manager when done
            manager.shutdown();
        }
    }
}