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
package org.apache.nifi.controllers.ftp.processors;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controllers.ftp.FTPConnectionPool;

/**
 * Base class for all FTP processors providing common functionality
 */
public abstract class AbstractFTPProcessor extends AbstractProcessor {

    public static final PropertyDescriptor PERSISTENT_FTP_SERVICE = new PropertyDescriptor.Builder()
            .name("FTP Connection Pool")
            .description("The Controller Service that provides persistent FTP connections")
            .required(true)
            .identifiesControllerService(FTPConnectionPool.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that are successfully processed are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that failed to be processed are transferred to this relationship")
            .build();

    /**
     * Verifies that the FTP connection pool is valid
     * 
     * @param context The process context
     * @throws ProcessException if the pool is not valid
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        final FTPConnectionPool connectionPool = context.getProperty(PERSISTENT_FTP_SERVICE)
                .asControllerService(FTPConnectionPool.class);
                
        if (!connectionPool.isPoolHealthy()) {
            getLogger().warn("FTP Connection Pool is not healthy");
        }
    }
    
    /**
     * Helper method to validate a controller service is the expected type
     * 
     * @param service The controller service
     * @param type The expected type
     * @return true if the service is of the expected type
     */
    protected boolean isServiceType(ControllerService service, Class<?> type) {
        return type.isAssignableFrom(service.getClass());
    }
}