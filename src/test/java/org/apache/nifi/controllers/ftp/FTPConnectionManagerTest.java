package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test class for FTPConnectionManager, focusing on validating
 * the proper handling of the new configuration options.
 */
public class FTPConnectionManagerTest {

    private ComponentLog mockLogger;
    private FTPConnectionConfig.Builder configBuilder;
    
    @Before
    public void setup() {
        mockLogger = mock(ComponentLog.class);
        
        // Setup a basic configuration builder that we'll customize in each test
        configBuilder = new FTPConnectionConfig.Builder()
                .hostname("test-ftp-server.example.com")
                .port(21)
                .username("testuser")
                .password("testpassword")
                .activeMode(false) // Default to passive mode
                .connectionTimeout(30000)
                .dataTimeout(30000)
                .bufferSize(1024 * 1024)
                .controlEncoding("UTF-8")
                .maxConnections(5)
                .keepAliveInterval(60000);
    }
    
    /**
     * Test creation of standard FTP client.
     */
    @Test
    public void testCreateAppropriateClient_StandardFTP() throws Exception {
        // Configure for standard FTP (no SSL)
        FTPConnectionConfig config = configBuilder
                .useImplicitSSL(false)
                .useExplicitSSL(false)
                .build();
        
        FTPConnectionManager manager = new FTPConnectionManager(config, mockLogger);
        
        // Access the private createAppropriateClient method using reflection
        Method method = FTPConnectionManager.class.getDeclaredMethod("createAppropriateClient");
        method.setAccessible(true);
        
        // Invoke the method and capture the result
        FTPClient client = (FTPClient) method.invoke(manager);
        
        // Verify we got a standard FTPClient, not an FTPSClient
        assertNotNull("Client should not be null", client);
        assertFalse("Client should be a standard FTPClient, not an FTPSClient", 
                client instanceof FTPSClient);
    }
    
    /**
     * Test creation of implicit FTPS client.
     */
    @Test
    public void testCreateAppropriateClient_ImplicitFTPS() throws Exception {
        // Configure for implicit FTPS
        FTPConnectionConfig config = configBuilder
                .useImplicitSSL(true)
                .useExplicitSSL(false)
                .useClientMode(true)
                .build();
        
        FTPConnectionManager manager = new FTPConnectionManager(config, mockLogger);
        
        // Access the private createAppropriateClient method using reflection
        Method method = FTPConnectionManager.class.getDeclaredMethod("createAppropriateClient");
        method.setAccessible(true);
        
        // Invoke the method and capture the result
        FTPClient client = (FTPClient) method.invoke(manager);
        
        // Verify we got an FTPSClient
        assertNotNull("Client should not be null", client);
        assertTrue("Client should be an FTPSClient", client instanceof FTPSClient);
        
        // Verify it's configured for implicit mode
        FTPSClient ftpsClient = (FTPSClient) client;
        
        // Use reflection to check the isImplicit field in FTPSClient
        Field isImplicitField = FTPSClient.class.getDeclaredField("isImplicit");
        isImplicitField.setAccessible(true);
        boolean isImplicit = (boolean) isImplicitField.get(ftpsClient);
        
        assertTrue("FTPSClient should be configured for implicit mode", isImplicit);
        
        // Also verify client mode is set correctly
        Field useClientModeField = FTPSClient.class.getDeclaredField("useClientMode");
        useClientModeField.setAccessible(true);
        boolean useClientMode = (boolean) useClientModeField.get(ftpsClient);
        
        assertTrue("FTPSClient should be configured to use client mode", useClientMode);
    }
    
    /**
     * Test creation of explicit FTPS client.
     */
    @Test
    public void testCreateAppropriateClient_ExplicitFTPS() throws Exception {
        // Configure for explicit FTPS
        FTPConnectionConfig config = configBuilder
                .useImplicitSSL(false)
                .useExplicitSSL(true)
                .useClientMode(true)
                .build();
        
        FTPConnectionManager manager = new FTPConnectionManager(config, mockLogger);
        
        // Access the private createAppropriateClient method using reflection
        Method method = FTPConnectionManager.class.getDeclaredMethod("createAppropriateClient");
        method.setAccessible(true);
        
        // Invoke the method and capture the result
        FTPClient client = (FTPClient) method.invoke(manager);
        
        // Verify we got an FTPSClient
        assertNotNull("Client should not be null", client);
        assertTrue("Client should be an FTPSClient", client instanceof FTPSClient);
        
        // Verify it's configured for explicit mode
        FTPSClient ftpsClient = (FTPSClient) client;
        
        // Use reflection to check the isImplicit field in FTPSClient
        Field isImplicitField = FTPSClient.class.getDeclaredField("isImplicit");
        isImplicitField.setAccessible(true);
        boolean isImplicit = (boolean) isImplicitField.get(ftpsClient);
        
        assertFalse("FTPSClient should be configured for explicit mode", isImplicit);
    }
    
    /**
     * Test SSL configurations for FTPS client.
     */
    @Test
    public void testCreateAppropriateClient_SSLConfiguration() throws Exception {
        // Configure SSL protocols and cipher suites
        String[] protocols = new String[] { "TLSv1.2", "TLSv1.3" };
        String[] cipherSuites = new String[] { 
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" 
        };
        
        FTPConnectionConfig config = configBuilder
                .useImplicitSSL(true)
                .enabledProtocols(protocols)
                .enabledCipherSuites(cipherSuites)
                .validateServerCertificate(true)
                .build();
        
        FTPConnectionManager manager = spy(new FTPConnectionManager(config, mockLogger));
        
        // Mock the actual client to avoid real SSL initialization
        FTPSClient mockFtpsClient = mock(FTPSClient.class);
        doReturn(mockFtpsClient).when(manager).createAppropriateClient();
        
        // Create a connection to trigger client configuration
        try {
            manager.createManagedConnection();
        } catch (Exception e) {
            // Expected since we're not making a real connection
        }
        
        // Verify SSL protocols were set
        verify(mockFtpsClient).setEnabledProtocols(protocols);
        
        // Verify cipher suites were set
        verify(mockFtpsClient).setEnabledCipherSuites(cipherSuites);
    }
    
    /**
     * Test active mode configuration.
     */
    @Test
    public void testActiveModeConfiguration() throws IOException {
        // Configure active mode with port range
        FTPConnectionConfig config = configBuilder
                .activeMode(true)
                .localActivePortRange(true)
                .activePortRangeStart(40000)
                .activePortRangeEnd(50000)
                .activeExternalIPAddress("203.0.113.10")
                .build();
        
        FTPConnectionManager manager = new FTPConnectionManager(config, mockLogger);
        
        // Create mocked FTP client
        FTPClient mockClient = mock(FTPClient.class);
        when(mockClient.login(anyString(), anyString())).thenReturn(true);
        
        // Use reflection to inject the mock client
        try {
            Method createClientMethod = FTPConnectionManager.class.getDeclaredMethod("createAndConnectClient", FTPConnection.class);
            createClientMethod.setAccessible(true);
            
            FTPConnection connection = new FTPConnection(config, mockLogger);
            
            // This will fail with NPE or similar, but we just want to verify the configuration
            try {
                manager.createManagedConnection();
            } catch (Exception e) {
                // Expected due to mocking
            }
            
            // Verify active mode configuration
            verify(mockClient, atLeastOnce()).setActivePortRange(40000, 50000);
            verify(mockClient, atLeastOnce()).setActiveExternalIPAddress("203.0.113.10");
            verify(mockClient, atLeastOnce()).enterLocalActiveMode();
        } catch (Exception e) {
            fail("Failed to invoke createAndConnectClient: " + e.getMessage());
        }
    }
    
    /**
     * Test proxy configuration.
     */
    @Test
    public void testProxyConfiguration() throws Exception {
        // Configure proxy settings
        FTPConnectionConfig config = configBuilder
                .proxyHost("proxy.example.com")
                .proxyPort(8080)
                .proxyUsername("proxyuser")
                .proxyPassword("proxypass")
                .proxyType("HTTP")
                .build();
        
        FTPConnectionManager manager = new FTPConnectionManager(config, mockLogger);
        
        // Verify shouldUseProxy returns true
        Method shouldUseProxyMethod = FTPConnectionManager.class.getDeclaredMethod("shouldUseProxy");
        shouldUseProxyMethod.setAccessible(true);
        boolean shouldUseProxy = (boolean) shouldUseProxyMethod.invoke(manager);
        
        assertTrue("Should use proxy with the given configuration", shouldUseProxy);
    }
    
    /**
     * Test transfer mode configuration (ASCII/Binary).
     */
    @Test
    public void testTransferModeConfiguration() throws Exception {
        // Configure ASCII transfer mode
        FTPConnectionConfig config = configBuilder
                .transferMode("ASCII")
                .build();
        
        FTPConnectionManager manager = new FTPConnectionManager(config, mockLogger);
        
        // Create mocked FTP client
        FTPClient mockClient = mock(FTPClient.class);
        when(mockClient.login(anyString(), anyString())).thenReturn(true);
        
        // Use reflection to inject the mock client
        try {
            Method createClientMethod = FTPConnectionManager.class.getDeclaredMethod("createAndConnectClient", FTPConnection.class);
            createClientMethod.setAccessible(true);
            
            FTPConnection connection = new FTPConnection(config, mockLogger);
            
            // This will fail with NPE or similar, but we just want to verify the configuration
            try {
                manager.createManagedConnection();
            } catch (Exception e) {
                // Expected due to mocking
            }
            
            // Verify ASCII file type was set
            verify(mockClient, atLeastOnce()).setFileType(FTPClient.ASCII_FILE_TYPE);
        } catch (Exception e) {
            fail("Failed to invoke createAndConnectClient: " + e.getMessage());
        }
        
        // Now test Binary transfer mode
        config = configBuilder
                .transferMode("Binary")
                .build();
        
        manager = new FTPConnectionManager(config, mockLogger);
        
        // Create mocked FTP client
        mockClient = mock(FTPClient.class);
        when(mockClient.login(anyString(), anyString())).thenReturn(true);
        
        // Use reflection to inject the mock client
        try {
            Method createClientMethod = FTPConnectionManager.class.getDeclaredMethod("createAndConnectClient", FTPConnection.class);
            createClientMethod.setAccessible(true);
            
            FTPConnection connection = new FTPConnection(config, mockLogger);
            
            // This will fail with NPE or similar, but we just want to verify the configuration
            try {
                manager.createManagedConnection();
            } catch (Exception e) {
                // Expected due to mocking
            }
            
            // Verify Binary file type was set
            verify(mockClient, atLeastOnce()).setFileType(FTPClient.BINARY_FILE_TYPE);
        } catch (Exception e) {
            fail("Failed to invoke createAndConnectClient: " + e.getMessage());
        }
    }
}