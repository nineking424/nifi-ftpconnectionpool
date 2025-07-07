package org.apache.nifi.controllers.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FTPConnectionPoolImpl
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FTP Connection Pool Implementation Tests")
public class FTPConnectionPoolImplTest {

    @Mock
    private Logger mockLogger;
    
    @Mock
    private FTPConnectionManager mockConnectionManager;
    
    @Mock
    private FTPConnectionHealthManager mockHealthManager;
    
    private FTPConnectionPoolConfig poolConfig;
    private FTPConnectionPoolImpl connectionPool;
    
    @BeforeEach
    void setUp() {
        poolConfig = FTPConnectionPoolConfig.builder()
                .maxTotal(10)
                .maxIdle(5)
                .minIdle(2)
                .maxWaitMillis(5000)
                .testOnBorrow(true)
                .testOnReturn(true)
                .testWhileIdle(true)
                .timeBetweenEvictionRunsMillis(30000)
                .minEvictableIdleTimeMillis(60000)
                .numTestsPerEvictionRun(3)
                .build();
    }
    
    @Test
    @DisplayName("Should initialize connection pool with correct configuration")
    void testPoolInitialization() {
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        assertNotNull(connectionPool);
        assertEquals(10, connectionPool.getMaxTotal());
        assertEquals(5, connectionPool.getMaxIdle());
        assertEquals(2, connectionPool.getMinIdle());
    }
    
    @Test
    @DisplayName("Should borrow and return connections successfully")
    void testBorrowAndReturnConnection() throws Exception {
        FTPClient mockClient = mock(FTPClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        when(mockConnectionManager.createConnection()).thenReturn(mockClient);
        when(mockHealthManager.isHealthy(any())).thenReturn(true);
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        // Borrow connection
        FTPClient borrowedClient = connectionPool.borrowConnection();
        assertNotNull(borrowedClient);
        assertEquals(1, connectionPool.getNumActive());
        assertEquals(0, connectionPool.getNumIdle());
        
        // Return connection
        connectionPool.returnConnection(borrowedClient);
        assertEquals(0, connectionPool.getNumActive());
        assertEquals(1, connectionPool.getNumIdle());
    }
    
    @Test
    @DisplayName("Should handle connection validation on borrow")
    void testConnectionValidationOnBorrow() throws Exception {
        FTPClient healthyClient = mock(FTPClient.class);
        FTPClient unhealthyClient = mock(FTPClient.class);
        
        when(mockConnectionManager.createConnection())
                .thenReturn(unhealthyClient)
                .thenReturn(healthyClient);
        
        when(mockHealthManager.isHealthy(unhealthyClient)).thenReturn(false);
        when(mockHealthManager.isHealthy(healthyClient)).thenReturn(true);
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        FTPClient borrowedClient = connectionPool.borrowConnection();
        assertNotNull(borrowedClient);
        assertEquals(healthyClient, borrowedClient);
        
        // Verify unhealthy connection was discarded
        verify(mockConnectionManager, times(1)).destroyConnection(unhealthyClient);
    }
    
    @Test
    @DisplayName("Should timeout when pool is exhausted")
    void testPoolExhaustion() throws Exception {
        poolConfig = FTPConnectionPoolConfig.builder()
                .maxTotal(1)
                .maxWaitMillis(1000)
                .build();
        
        FTPClient mockClient = mock(FTPClient.class);
        when(mockConnectionManager.createConnection()).thenReturn(mockClient);
        when(mockHealthManager.isHealthy(any())).thenReturn(true);
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        // Borrow the only available connection
        FTPClient firstClient = connectionPool.borrowConnection();
        assertNotNull(firstClient);
        
        // Try to borrow another connection (should timeout)
        assertThrows(IOException.class, () -> {
            connectionPool.borrowConnection();
        });
    }
    
    @Test
    @DisplayName("Should handle concurrent access correctly")
    void testConcurrentAccess() throws Exception {
        poolConfig = FTPConnectionPoolConfig.builder()
                .maxTotal(5)
                .maxIdle(5)
                .minIdle(2)
                .build();
        
        when(mockConnectionManager.createConnection()).thenAnswer(invocation -> {
            FTPClient client = mock(FTPClient.class);
            when(client.isConnected()).thenReturn(true);
            return client;
        });
        when(mockHealthManager.isHealthy(any())).thenReturn(true);
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        int numThreads = 10;
        int operationsPerThread = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        try {
                            FTPClient client = connectionPool.borrowConnection();
                            Thread.sleep(10); // Simulate some work
                            connectionPool.returnConnection(client);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completionLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        
        // Verify results
        assertEquals(numThreads * operationsPerThread, successCount.get());
        assertEquals(0, errorCount.get());
        assertTrue(connectionPool.getNumActive() >= 0);
        assertTrue(connectionPool.getNumIdle() >= 0);
        assertTrue(connectionPool.getNumActive() + connectionPool.getNumIdle() <= poolConfig.getMaxTotal());
    }
    
    @Test
    @DisplayName("Should evict idle connections")
    void testIdleConnectionEviction() throws Exception {
        poolConfig = FTPConnectionPoolConfig.builder()
                .maxTotal(5)
                .maxIdle(5)
                .minIdle(1)
                .timeBetweenEvictionRunsMillis(100)
                .minEvictableIdleTimeMillis(200)
                .build();
        
        FTPClient mockClient = mock(FTPClient.class);
        when(mockConnectionManager.createConnection()).thenReturn(mockClient);
        when(mockHealthManager.isHealthy(any())).thenReturn(true);
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        // Create some idle connections
        for (int i = 0; i < 3; i++) {
            FTPClient client = connectionPool.borrowConnection();
            connectionPool.returnConnection(client);
        }
        
        assertEquals(3, connectionPool.getNumIdle());
        
        // Wait for eviction to occur
        Thread.sleep(500);
        
        // Should have evicted down to minIdle
        assertTrue(connectionPool.getNumIdle() <= poolConfig.getMinIdle() + 1);
    }
    
    @Test
    @DisplayName("Should invalidate unhealthy connections on return")
    void testInvalidateUnhealthyConnectionOnReturn() throws Exception {
        FTPClient mockClient = mock(FTPClient.class);
        when(mockConnectionManager.createConnection()).thenReturn(mockClient);
        when(mockHealthManager.isHealthy(mockClient))
                .thenReturn(true)  // Healthy on borrow
                .thenReturn(false); // Unhealthy on return
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        FTPClient borrowedClient = connectionPool.borrowConnection();
        assertNotNull(borrowedClient);
        
        connectionPool.returnConnection(borrowedClient);
        
        // Verify connection was destroyed instead of returned to pool
        verify(mockConnectionManager, times(1)).destroyConnection(mockClient);
        assertEquals(0, connectionPool.getNumIdle());
    }
    
    @Test
    @DisplayName("Should gracefully shutdown pool")
    void testGracefulShutdown() throws Exception {
        FTPClient mockClient1 = mock(FTPClient.class);
        FTPClient mockClient2 = mock(FTPClient.class);
        
        when(mockConnectionManager.createConnection())
                .thenReturn(mockClient1)
                .thenReturn(mockClient2);
        when(mockHealthManager.isHealthy(any())).thenReturn(true);
        
        connectionPool = new FTPConnectionPoolImpl(poolConfig, mockConnectionManager, mockHealthManager, mockLogger);
        
        // Create some connections
        FTPClient active = connectionPool.borrowConnection();
        FTPClient idle = connectionPool.borrowConnection();
        connectionPool.returnConnection(idle);
        
        // Shutdown the pool
        connectionPool.shutdown();
        
        // Return the active connection after shutdown
        connectionPool.returnConnection(active);
        
        // Verify all connections were destroyed
        verify(mockConnectionManager, times(1)).destroyConnection(mockClient1);
        verify(mockConnectionManager, times(1)).destroyConnection(mockClient2);
        
        // Pool should reject new borrows after shutdown
        assertThrows(IllegalStateException.class, () -> {
            connectionPool.borrowConnection();
        });
    }
}