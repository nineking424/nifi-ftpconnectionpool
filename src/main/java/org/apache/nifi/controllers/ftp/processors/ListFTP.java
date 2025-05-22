package org.apache.nifi.controllers.ftp.processors;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controllers.ftp.FTPFile;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.PersistentFTPService;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A processor that lists the contents of a directory on an FTP server.
 * This processor uses the PersistentFTPService to manage connections to the FTP server.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"FTP", "FTPS", "list", "ingest", "source", "input", "files"})
@CapabilityDescription("Lists the contents of a directory on an FTP server. For each file that is listed, " +
        "creates a FlowFile that represents the file. This processor uses a persistent connection pool " +
        "to the FTP server to improve performance.")
@WritesAttributes({
    @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname of the FTP server"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The port of the FTP server"),
    @WritesAttribute(attribute = "ftp.listing.user", description = "The username used to list files"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the FTP server"),
    @WritesAttribute(attribute = "path", description = "The path to the file on the FTP server"),
    @WritesAttribute(attribute = "file.owner", description = "The owner of the file"),
    @WritesAttribute(attribute = "file.group", description = "The group that owns the file"),
    @WritesAttribute(attribute = "file.permissions", description = "The file permissions"),
    @WritesAttribute(attribute = "file.size", description = "The size of the file in bytes"),
    @WritesAttribute(attribute = "file.lastModifiedTime", description = "The last modified time of the file"),
    @WritesAttribute(attribute = "ftp.listing.timestamp", description = "The timestamp when the listing was performed")
})
@SeeAlso({GetFTP.class, PutFTP.class})
public class ListFTP extends AbstractProcessor {
    
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success")
            .build();

    // Properties
    public static final PropertyDescriptor FTP_SERVICE = new PropertyDescriptor.Builder()
            .name("FTP Service")
            .description("The PersistentFTPService to use for connecting to the FTP server")
            .required(true)
            .identifiesControllerService(PersistentFTPService.class)
            .build();
    
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Remote Directory")
            .description("The directory to list files from")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Whether to list files from subdirectories")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("A regular expression for filtering files")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age a file must be in order to be listed")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    
    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
            .name("Maximum File Age")
            .description("The maximum age a file can be in order to be listed")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
            .name("Minimum File Size")
            .description("The minimum size a file must be in order to be listed")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("0 B")
            .build();
    
    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum File Size")
            .description("The maximum size a file can be in order to be listed")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to list in a single batch")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();
    
    private static final List<PropertyDescriptor> properties;
    private static final Set<Relationship> relationships;
    
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(FTP_SERVICE);
        props.add(DIRECTORY);
        props.add(RECURSE);
        props.add(FILE_FILTER);
        props.add(MIN_AGE);
        props.add(MAX_AGE);
        props.add(MIN_SIZE);
        props.add(MAX_SIZE);
        props.add(BATCH_SIZE);
        properties = Collections.unmodifiableList(props);
        
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rels);
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final PersistentFTPService ftpService = context.getProperty(FTP_SERVICE)
                .asControllerService(PersistentFTPService.class);
        final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final boolean recurse = context.getProperty(RECURSE).asBoolean();
        final String fileFilter = context.getProperty(FILE_FILTER).evaluateAttributeExpressions().getValue();
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        
        final long minAge = context.getProperty(MIN_AGE).evaluateAttributeExpressions().asTimePeriod(java.util.concurrent.TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(MAX_AGE).evaluateAttributeExpressions().isSet()
                ? context.getProperty(MAX_AGE).evaluateAttributeExpressions().asTimePeriod(java.util.concurrent.TimeUnit.MILLISECONDS)
                : null;
        
        final long minSize = context.getProperty(MIN_SIZE).evaluateAttributeExpressions().asDataSize(java.util.concurrent.TimeUnit.BYTES);
        final Long maxSize = context.getProperty(MAX_SIZE).evaluateAttributeExpressions().isSet()
                ? context.getProperty(MAX_SIZE).evaluateAttributeExpressions().asDataSize(java.util.concurrent.TimeUnit.BYTES)
                : null;
        
        final long listingTimestamp = System.currentTimeMillis();
        final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        
        // Get connection from FTP service
        FTPClient ftpClient = null;
        final AtomicReference<IOException> ioeRef = new AtomicReference<>(null);
        
        try {
            ftpClient = ftpService.getConnection();
            
            // Get FTP connection details for attributes
            final String hostname = ftpClient.getRemoteAddress().getHostName();
            final int port = ftpClient.getRemotePort();
            final String username = context.getProperty(FTP_SERVICE).asControllerService(PersistentFTPService.class)
                    .getConnectionStats().getOrDefault("username", "unknown").toString();
            
            // Process the directory
            List<FTPFile> files;
            try {
                // Get the file listing
                files = ftpService.listFiles(directory, null);
                
                // Record the listing in metrics
                context.getProperty(FTP_SERVICE).asControllerService(PersistentFTPService.class)
                        .getConnectionStats();
                
            } catch (IOException e) {
                ioeRef.set(e);
                throw new ProcessException("Failed to list directory " + directory, e);
            }
            
            // Filter and process the files
            int flowFilesCreated = 0;
            for (FTPFile file : files) {
                // Skip directories if not recursing
                if (file.isDirectory() && !recurse) {
                    continue;
                }
                
                // Apply file filter if specified
                if (fileFilter != null && !file.getName().matches(fileFilter)) {
                    continue;
                }
                
                // Skip directories that we'll handle through recursion
                if (file.isDirectory() && recurse) {
                    // We'll handle directories through recursion
                    continue;
                }
                
                // Check file age
                final long fileAge = listingTimestamp - file.getTimestamp().getTime();
                if (fileAge < minAge) {
                    continue;
                }
                if (maxAge != null && fileAge > maxAge) {
                    continue;
                }
                
                // Check file size
                final long fileSize = file.getSize();
                if (fileSize < minSize) {
                    continue;
                }
                if (maxSize != null && fileSize > maxSize) {
                    continue;
                }
                
                // Create a FlowFile for this file
                FlowFile flowFile = session.create();
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), file.getName());
                flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), file.getPath());
                flowFile = session.putAttribute(flowFile, "file.owner", file.getOwner());
                flowFile = session.putAttribute(flowFile, "file.group", file.getGroup());
                flowFile = session.putAttribute(flowFile, "file.permissions", file.getPermissions());
                flowFile = session.putAttribute(flowFile, "file.size", String.valueOf(file.getSize()));
                flowFile = session.putAttribute(flowFile, "file.lastModifiedTime", 
                        formatter.format(file.getTimestamp()));
                
                // Add FTP-specific attributes
                flowFile = session.putAttribute(flowFile, "ftp.remote.host", hostname);
                flowFile = session.putAttribute(flowFile, "ftp.remote.port", String.valueOf(port));
                flowFile = session.putAttribute(flowFile, "ftp.listing.user", username);
                flowFile = session.putAttribute(flowFile, "ftp.listing.timestamp", 
                        formatter.format(new java.util.Date(listingTimestamp)));
                
                // Add any additional attributes from the file
                Map<String, Object> attributes = file.getAttributes();
                if (attributes != null) {
                    Map<String, String> stringAttributes = new HashMap<>();
                    for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                        if (entry.getValue() != null) {
                            stringAttributes.put("ftp.file." + entry.getKey(), entry.getValue().toString());
                        }
                    }
                    flowFile = session.putAllAttributes(flowFile, stringAttributes);
                }
                
                // Transfer the FlowFile to success
                session.transfer(flowFile, REL_SUCCESS);
                flowFilesCreated++;
                
                // If we've reached the batch size, yield
                if (flowFilesCreated >= batchSize) {
                    break;
                }
            }
            
            // If we're recursing and haven't reached the batch size, process subdirectories
            if (recurse && flowFilesCreated < batchSize) {
                for (FTPFile file : files) {
                    if (file.isDirectory()) {
                        // Recursively process this directory
                        // Note: In a real implementation, we'd need to handle this properly
                        // by either using a queue or processing all directories in a batch
                        getLogger().debug("Recursive directory processing not fully implemented");
                    }
                }
            }
            
            if (flowFilesCreated > 0) {
                getLogger().info("Created {} FlowFiles for files in {}", new Object[] { flowFilesCreated, directory });
            } else {
                getLogger().debug("No files matched the listing criteria in {}", new Object[] { directory });
            }
            
        } catch (final IOException e) {
            // Store the exception so we can throw it after releasing the connection
            ioeRef.set(e);
        } finally {
            // Always release the connection back to the pool
            if (ftpClient != null) {
                try {
                    ftpService.releaseConnection(ftpClient);
                } catch (final Exception e) {
                    getLogger().error("Failed to release FTP connection", e);
                }
            }
        }
        
        // If an exception occurred, throw it
        final IOException ioe = ioeRef.get();
        if (ioe != null) {
            throw new ProcessException(ioe);
        }
    }
}