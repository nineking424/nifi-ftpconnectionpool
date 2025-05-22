/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controllers.ftp;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.components.PropertyDescriptor;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class to help locate FTP connection pool services in a NiFi processor context.
 */
public class FTPConnectionPoolServiceLocator {

    /**
     * Finds all FTP connection pool services configured in the process context.
     *
     * @param context The process context
     * @param propertyDescriptors List of property descriptors to search for service references
     * @return Set of FTPConnectionPool services found in the context
     */
    public static Set<FTPConnectionPool> findAllConnectionPools(
            final ProcessContext context, 
            final List<PropertyDescriptor> propertyDescriptors) {
        
        return propertyDescriptors.stream()
                .filter(descriptor -> FTPConnectionPool.class.isAssignableFrom(descriptor.getControllerServiceDefinition()))
                .map(descriptor -> {
                    try {
                        return context.getProperty(descriptor).asControllerService(FTPConnectionPool.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(service -> service != null)
                .collect(Collectors.toSet());
    }

    /**
     * Finds the first FTP connection pool service configured in the process context.
     *
     * @param context The process context
     * @param propertyDescriptors List of property descriptors to search for service references
     * @return Optional containing the first FTPConnectionPool service found, or empty if none found
     */
    public static Optional<FTPConnectionPool> findFirstConnectionPool(
            final ProcessContext context, 
            final List<PropertyDescriptor> propertyDescriptors) {
        
        return propertyDescriptors.stream()
                .filter(descriptor -> FTPConnectionPool.class.isAssignableFrom(descriptor.getControllerServiceDefinition()))
                .map(descriptor -> {
                    try {
                        return context.getProperty(descriptor).asControllerService(FTPConnectionPool.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(service -> service != null)
                .findFirst();
    }
    
    /**
     * Gets a connection pool from a specific property in the process context.
     *
     * @param context The process context
     * @param propertyDescriptor The property descriptor specifying the connection pool service
     * @return The FTPConnectionPool service specified by the property, or null if not found
     */
    public static FTPConnectionPool getConnectionPool(
            final ProcessContext context, 
            final PropertyDescriptor propertyDescriptor) {
        
        if (!FTPConnectionPool.class.isAssignableFrom(propertyDescriptor.getControllerServiceDefinition())) {
            return null;
        }
        
        try {
            return context.getProperty(propertyDescriptor).asControllerService(FTPConnectionPool.class);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Gets a connection from the specified connection pool and makes sure to return it when done.
     * 
     * @param connectionPool The connection pool to borrow from
     * @param session The process session
     * @param callback The callback to execute with the borrowed connection
     * @throws ProcessException if an error occurs
     */
    public static void withConnection(
            final FTPConnectionPool connectionPool,
            final ProcessSession session,
            final ConnectionCallback callback) throws ProcessException {
            
        FTPConnection connection = null;
        try {
            connection = connectionPool.borrowConnection();
            callback.execute(connection);
        } catch (Exception e) {
            throw new ProcessException("Error executing operation with FTP connection", e);
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection);
            }
        }
    }
    
    /**
     * Callback interface for executing operations with a borrowed connection.
     */
    public interface ConnectionCallback {
        /**
         * Executes operations with the borrowed connection.
         * 
         * @param connection The borrowed connection
         * @throws Exception if an error occurs
         */
        void execute(FTPConnection connection) throws Exception;
    }
}