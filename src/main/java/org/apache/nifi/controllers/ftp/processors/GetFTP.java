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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.controllers.ftp.FTPConnection;
import org.apache.nifi.controllers.ftp.FTPConnectionPool;
import org.apache.nifi.controllers.ftp.FTPFile;
import org.apache.nifi.controllers.ftp.FTPOperationException;
import org.apache.nifi.controllers.ftp.FTPOperations;
import org.apache.nifi.controllers.ftp.exception.FTPTransferException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"ftp", "get", "retrieve", "file", "transfer", "ingest", "input", "download"})
@CapabilityDescription("Fetches files from an FTP server using a persistent connection pool")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the FTP server"),
    @WritesAttribute(attribute = "path", description = "The path is set to the path of the file's directory on the FTP server"),
    @WritesAttribute(attribute = "ftp.remote.host", description = "The remote host from which the file was fetched"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The remote port this file was fetched from"),
    @WritesAttribute(attribute = "ftp.last.modified.time", description = "The last modified time of the source file"),
    @WritesAttribute(attribute = "ftp.permissions", description = "The file permissions of the source file"),
    @WritesAttribute(attribute = "ftp.owner", description = "The owner of the source file"),
    @WritesAttribute(attribute = "ftp.group", description = "The group owner of the source file"),
    @WritesAttribute(attribute = "file.size", description = "The size of the file in bytes"),
    @WritesAttribute(attribute = "ftp.transfer.milliseconds", description = "The number of milliseconds spent retrieving the file")
})
@SeeAlso({ListFTP.class, PutFTP.class})
public class GetFTP extends AbstractFTPProcessor {

    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
            .name("Remote Path")
            .description("The fully qualified path to a directory or file on the remote server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELETE_ORIGINAL = new PropertyDescriptor.Builder()
            .name("Delete Original")
            .description("Determines whether to delete the file from the server after successfully fetching it")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Completion Strategy")
            .description("Determines what to do with the original file on the server. Delete will remove the file, "
                    + "Move will transfer to the directory specified by the Move Destination Directory property, "
                    + "None will leave the file as-is")
            .allowableValues("Delete", "Move", "None")
            .defaultValue("None")
            .required(true)
            .build();

    public static final PropertyDescriptor MOVE_DESTINATION_DIR = new PropertyDescriptor.Builder()
            .name("Move Destination Directory")
            .description("The directory to move the original file to once it has been successfully fetched. "
                    + "This property is ignored unless the Completion Strategy is set to 'Move'")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor FILE_FILTER_REGEX = new PropertyDescriptor.Builder()
            .name("File Filter Regex")
            .description("A regular expression that will be used to filter filenames; if a filename does not match this regex, the file will not be fetched")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to fetch in a single execution")
            .required(true)
            .defaultValue("10")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(PERSISTENT_FTP_SERVICE);
        tempDescriptors.add(REMOTE_PATH);
        tempDescriptors.add(COMPLETION_STRATEGY);
        tempDescriptors.add(MOVE_DESTINATION_DIR);
        tempDescriptors.add(FILE_FILTER_REGEX);
        tempDescriptors.add(BATCH_SIZE);
        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        
        final String completionStrategy = validationContext.getProperty(COMPLETION_STRATEGY).getValue();
        final String moveDestinationDirectory = validationContext.getProperty(MOVE_DESTINATION_DIR).getValue();

        if ("Move".equals(completionStrategy) && (moveDestinationDirectory == null || moveDestinationDirectory.trim().isEmpty())) {
            results.add(new ValidationResult.Builder()
                    .subject(MOVE_DESTINATION_DIR.getDisplayName())
                    .explanation("Move Destination Directory must be specified when using Move Completion Strategy")
                    .valid(false)
                    .build());
        }
        
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = null;
        if (context.hasIncomingConnection()) {
            flowFile = session.get();
            if (flowFile == null) {
                return;
            }
        }

        final String remotePath;
        if (flowFile == null) {
            remotePath = context.getProperty(REMOTE_PATH).evaluateAttributeExpressions().getValue();
        } else {
            remotePath = context.getProperty(REMOTE_PATH).evaluateAttributeExpressions(flowFile).getValue();
        }

        final String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        final String moveDestinationDir = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
        final String fileFilterRegex = context.getProperty(FILE_FILTER_REGEX).evaluateAttributeExpressions(flowFile).getValue();
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final FTPConnectionPool connectionPool = context.getProperty(PERSISTENT_FTP_SERVICE).asControllerService(FTPConnectionPool.class);
        FTPConnection connection = null;

        try {
            connection = connectionPool.borrowConnection();
            final FTPOperations ftpOperations = connection.getClientOperations();
            
            // Get list of files to process
            List<FTPFile> filesToFetch;
            try {
                filesToFetch = ftpOperations.listFiles(remotePath);
                
                // Apply regex filter if specified
                if (fileFilterRegex != null && !fileFilterRegex.isEmpty()) {
                    List<FTPFile> filteredFiles = new ArrayList<>();
                    for (FTPFile file : filesToFetch) {
                        if (file.getName().matches(fileFilterRegex)) {
                            filteredFiles.add(file);
                        }
                    }
                    filesToFetch = filteredFiles;
                }
                
                // Limit to batch size
                if (filesToFetch.size() > batchSize) {
                    filesToFetch = filesToFetch.subList(0, batchSize);
                }
            } catch (IOException e) {
                if (flowFile != null) {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                }
                getLogger().error("Failed to retrieve file listing from {}", new Object[]{remotePath}, e);
                context.yield();
                return;
            }
            
            int filesTransferred = 0;
            for (FTPFile file : filesToFetch) {
                // Skip directories
                if (file.isDirectory()) {
                    continue;
                }
                
                final String fullPath = remotePath + "/" + file.getName();
                FlowFile currentFlowFile = (flowFile != null) ? 
                        session.clone(flowFile) : 
                        session.create();
                
                try {
                    final long startNanos = System.nanoTime();
                    
                    // Create the flow file with content from the FTP file
                    currentFlowFile = session.write(currentFlowFile, out -> {
                        try (final InputStream in = ftpOperations.retrieveFileStream(fullPath)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = in.read(buffer)) != -1) {
                                out.write(buffer, 0, bytesRead);
                            }
                        } catch (IOException e) {
                            throw new FTPTransferException("Failed to retrieve file from FTP server", e);
                        }
                    });
                    
                    final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    
                    // Set attributes
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put(CoreAttributes.FILENAME.key(), file.getName());
                    attributes.put("path", remotePath);
                    attributes.put("ftp.remote.host", connection.getHost());
                    attributes.put("ftp.remote.port", String.valueOf(connection.getPort()));
                    attributes.put("ftp.last.modified.time", String.valueOf(file.getLastModified()));
                    attributes.put("ftp.permissions", file.getPermissions());
                    attributes.put("ftp.owner", file.getOwner());
                    attributes.put("ftp.group", file.getGroup());
                    attributes.put("file.size", String.valueOf(file.getSize()));
                    attributes.put("ftp.transfer.milliseconds", String.valueOf(transferMillis));
                    
                    currentFlowFile = session.putAllAttributes(currentFlowFile, attributes);
                    
                    // Handle completion strategy
                    switch (completionStrategy) {
                        case "Delete":
                            ftpOperations.deleteFile(fullPath);
                            break;
                        case "Move":
                            String destinationPath = moveDestinationDir + "/" + file.getName();
                            ftpOperations.rename(fullPath, destinationPath);
                            break;
                        case "None":
                            // No action needed
                            break;
                    }
                    
                    session.transfer(currentFlowFile, REL_SUCCESS);
                    session.getProvenanceReporter().receive(currentFlowFile, 
                            ftpOperations.getConnectionUrl() + fullPath,
                            "Retrieved file from FTP server", transferMillis);
                    
                    filesTransferred++;
                    getLogger().debug("Successfully retrieved file {} from FTP server", new Object[]{file.getName()});
                } catch (Exception e) {
                    getLogger().error("Failed to retrieve file {} from FTP server", new Object[]{file.getName()}, e);
                    session.remove(currentFlowFile);
                }
            }
            
            // If we processed the incoming flow file, remove it
            if (flowFile != null) {
                session.remove(flowFile);
            }
            
            if (filesTransferred == 0) {
                context.yield();
            }
            
        } catch (Exception e) {
            getLogger().error("Error retrieving files from FTP server", e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection);
            }
        }
    }
}