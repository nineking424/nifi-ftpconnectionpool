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
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.controllers.ftp.FTPFileType;
import org.apache.nifi.controllers.ftp.FTPOperations;
import org.apache.nifi.controllers.ftp.exception.FTPTransferException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"ftp", "put", "upload", "file", "transfer", "outgoing"})
@CapabilityDescription("Sends FlowFiles to an FTP server using a persistent connection pool")
@ReadsAttributes({
    @ReadsAttribute(attribute = "filename", description = "The filename to use when uploading the file to the FTP server"),
    @ReadsAttribute(attribute = "path", description = "The path on the FTP server to upload the file to. If not specified, "
            + "the value of the Remote Directory property will be used")
})
@WritesAttributes({
    @WritesAttribute(attribute = "ftp.remote.host", description = "The remote host the file was sent to"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The remote port the file was sent to"),
    @WritesAttribute(attribute = "ftp.upload.path", description = "The full path on the FTP server where the file was uploaded"),
    @WritesAttribute(attribute = "ftp.transfer.milliseconds", description = "The number of milliseconds spent transferring the file"),
    @WritesAttribute(attribute = "ftp.transfer.rate.bytes.per.second", description = "The rate at which the file was transferred in bytes per second")
})
@SeeAlso({ListFTP.class, GetFTP.class})
public class PutFTP extends AbstractFTPProcessor {

    public static final PropertyDescriptor REMOTE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Remote Directory")
            .description("The directory on the FTP server to which files should be uploaded")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .description("The filename to use when uploading the file to the FTP server. If not specified, the filename attribute will be used")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Determines how to handle the situation when a destination file already exists. If 'Fail', the processor will fail. "
                    + "If 'Replace', the destination file will be overwritten. If 'Rename', the processor will append a unique identifier to the filename")
            .allowableValues("Fail", "Replace", "Rename")
            .defaultValue("Fail")
            .required(true)
            .build();

    public static final PropertyDescriptor CREATE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Create Directory")
            .description("If true, then the remote directory will be created if it does not exist")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor DOT_RENAME = new PropertyDescriptor.Builder()
            .name("Temporary Filename")
            .description("If true, the filename on the FTP server will be prefixed with a '.' until the upload is complete")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor TRANSFER_MODE = new PropertyDescriptor.Builder()
            .name("Transfer Mode")
            .description("The FTP transfer mode to use")
            .allowableValues("ASCII", "Binary", "Auto Detect")
            .defaultValue("Binary")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that are successfully sent to the FTP server are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that failed to be sent to the FTP server are transferred to this relationship")
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
        tempDescriptors.add(REMOTE_DIRECTORY);
        tempDescriptors.add(FILENAME);
        tempDescriptors.add(CONFLICT_RESOLUTION);
        tempDescriptors.add(CREATE_DIRECTORY);
        tempDescriptors.add(DOT_RENAME);
        tempDescriptors.add(TRANSFER_MODE);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String remoteDirectory = context.getProperty(REMOTE_DIRECTORY)
                .evaluateAttributeExpressions(flowFile).getValue();
        final String filename = context.getProperty(FILENAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
        final boolean createDirectory = context.getProperty(CREATE_DIRECTORY).asBoolean();
        final boolean dotRename = context.getProperty(DOT_RENAME).asBoolean();
        final String transferMode = context.getProperty(TRANSFER_MODE).getValue();

        // Determine the target filename
        final String targetFilename;
        if (filename != null) {
            targetFilename = filename;
        } else {
            targetFilename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        }

        // Ensure the filename is set
        if (targetFilename == null) {
            getLogger().error("No filename available for FlowFile");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Get the FTP connection
        final FTPConnectionPool connectionPool = context.getProperty(PERSISTENT_FTP_SERVICE)
                .asControllerService(FTPConnectionPool.class);
        FTPConnection connection = null;

        try {
            connection = connectionPool.borrowConnection();
            final FTPOperations ftpOperations = connection.getClientOperations();

            // Determine file type based on transfer mode
            FTPFileType fileType;
            if ("ASCII".equals(transferMode)) {
                fileType = FTPFileType.ASCII;
            } else if ("Binary".equals(transferMode)) {
                fileType = FTPFileType.BINARY;
            } else {
                // Auto-detect based on filename extension
                String extension = targetFilename.lastIndexOf('.') > 0 ? 
                    targetFilename.substring(targetFilename.lastIndexOf('.') + 1).toLowerCase() : "";
                
                if (isAsciiFile(extension)) {
                    fileType = FTPFileType.ASCII;
                } else {
                    fileType = FTPFileType.BINARY;
                }
            }
            ftpOperations.setFileType(fileType);

            // Create directory if needed
            if (createDirectory) {
                try {
                    ftpOperations.createDirectoryHierarchy(remoteDirectory);
                } catch (IOException e) {
                    getLogger().error("Failed to create remote directory {}", new Object[]{remoteDirectory}, e);
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                    return;
                }
            }
            
            // Check if destination exists
            final String fullPath = remoteDirectory + "/" + targetFilename;
            final String temporaryPath = dotRename ? remoteDirectory + "/." + targetFilename : fullPath;
            boolean destinationExists = false;
            
            try {
                destinationExists = ftpOperations.fileExists(fullPath);
            } catch (IOException e) {
                getLogger().error("Failed to check if remote file {} exists", new Object[]{fullPath}, e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
            
            // Handle conflict resolution
            if (destinationExists) {
                switch (conflictResolution) {
                    case "Fail":
                        getLogger().error("Remote file {} already exists", new Object[]{fullPath});
                        session.transfer(session.penalize(flowFile), REL_FAILURE);
                        return;
                    case "Replace":
                        // Will overwrite
                        break;
                    case "Rename":
                        // Generate unique name
                        String uniqueFilename = generateUniqueFilename(ftpOperations, remoteDirectory, targetFilename);
                        if (uniqueFilename == null) {
                            getLogger().error("Failed to generate unique filename for {}", new Object[]{targetFilename});
                            session.transfer(session.penalize(flowFile), REL_FAILURE);
                            return;
                        }
                        break;
                }
            }
            
            // Perform the transfer
            final long startNanos = System.nanoTime();
            long fileSize = flowFile.getSize();
            try (InputStream in = session.read(flowFile)) {
                boolean success = ftpOperations.storeFile(temporaryPath, in);
                
                if (!success) {
                    getLogger().error("Failed to send {} to FTP server", new Object[]{flowFile});
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                    return;
                }
                
                // If we used dot-rename, rename the file
                if (dotRename) {
                    success = ftpOperations.rename(temporaryPath, fullPath);
                    if (!success) {
                        getLogger().error("Failed to rename dot-file {} to {}", new Object[]{temporaryPath, fullPath});
                        session.transfer(session.penalize(flowFile), REL_FAILURE);
                        try {
                            ftpOperations.deleteFile(temporaryPath);
                        } catch (IOException e) {
                            getLogger().warn("Failed to delete temporary file {} after failed rename", new Object[]{temporaryPath}, e);
                        }
                        return;
                    }
                }
            } catch (IOException e) {
                getLogger().error("Failed to send {} to FTP server", new Object[]{flowFile}, e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
            
            // Calculate transfer statistics
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            final double bytesPerSecond = (transferMillis > 0) ? 
                    (fileSize * 1000.0 / transferMillis) : fileSize;
                    
            // Update attributes
            Map<String, String> attributes = new HashMap<>();
            attributes.put("ftp.remote.host", connection.getHost());
            attributes.put("ftp.remote.port", String.valueOf(connection.getPort()));
            attributes.put("ftp.upload.path", fullPath);
            attributes.put("ftp.transfer.milliseconds", String.valueOf(transferMillis));
            attributes.put("ftp.transfer.rate.bytes.per.second", String.valueOf((long) bytesPerSecond));
            
            flowFile = session.putAllAttributes(flowFile, attributes);
            
            // Report success
            session.getProvenanceReporter().send(flowFile, ftpOperations.getConnectionUrl() + fullPath, transferMillis);
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Successfully transferred {} to {}", new Object[]{flowFile, fullPath});
            
        } catch (Exception e) {
            getLogger().error("Failed to transfer {} to FTP server", new Object[]{flowFile}, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        } finally {
            if (connection != null) {
                connectionPool.returnConnection(connection);
            }
        }
    }
    
    /**
     * Generates a unique filename for a file in a directory
     * 
     * @param ftpOperations The FTP client
     * @param directory The directory
     * @param filename The original filename
     * @return A unique filename, or null if one couldn't be generated
     */
    private String generateUniqueFilename(FTPOperations ftpOperations, String directory, String filename) {
        final String baseName = filename.contains(".") ? 
                filename.substring(0, filename.lastIndexOf('.')) : filename;
        final String extension = filename.contains(".") ?
                filename.substring(filename.lastIndexOf('.')) : "";
                
        // Try up to 1000 unique filenames
        for (int i = 1; i <= 1000; i++) {
            final String uniqueFilename = baseName + "_" + i + extension;
            final String fullPath = directory + "/" + uniqueFilename;
            
            try {
                if (!ftpOperations.fileExists(fullPath)) {
                    return uniqueFilename;
                }
            } catch (IOException e) {
                getLogger().warn("Error checking for file existence: {}", new Object[]{fullPath});
                continue;
            }
        }
        
        return null;
    }
    
    /**
     * Determines if a file with the given extension should be transferred in ASCII mode
     * 
     * @param extension The file extension (lowercase)
     * @return true if this is a known ASCII file type
     */
    private boolean isAsciiFile(String extension) {
        Set<String> asciiExtensions = new HashSet<>();
        asciiExtensions.add("txt");
        asciiExtensions.add("htm");
        asciiExtensions.add("html");
        asciiExtensions.add("xml");
        asciiExtensions.add("csv");
        asciiExtensions.add("json");
        asciiExtensions.add("properties");
        asciiExtensions.add("conf");
        asciiExtensions.add("cfg");
        asciiExtensions.add("ini");
        asciiExtensions.add("md");
        asciiExtensions.add("yml");
        asciiExtensions.add("yaml");
        
        return asciiExtensions.contains(extension);
    }
}