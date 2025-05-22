# FTP Connection Pool Configuration Guide

This guide provides detailed instructions for configuring and using the FTP Connection Pool in Apache NiFi.

## Controller Service Configuration

### Basic Configuration

1. In the NiFi UI, navigate to the Controller Services configuration tab
2. Click the "+" button to add a new controller service
3. Search for "PersistentFTPConnectionService" and select it
4. Configure the basic properties:

| Property | Description | Default | Recommended |
|----------|-------------|---------|------------|
| **Host** | FTP server hostname or IP | (required) | Your FTP server |
| **Port** | FTP server port | 21 | 21 for standard FTP |
| **Username** | FTP username | (required) | Your FTP username |
| **Password** | FTP password | (required) | Your FTP password |
| **Connection Pool Size** | Number of connections to maintain | 5 | 5-10 for moderate loads |
| **Connection Timeout** | Timeout for establishing connections (ms) | 30000 | 30000 |
| **Data Timeout** | Timeout for data transfers (ms) | 30000 | 30000-60000 |
| **Connection Mode** | Active or Passive | Passive | Passive (recommended) |

### Advanced Configuration

Expand the "Advanced" section to configure additional options:

| Property | Description | Default | Recommended |
|----------|-------------|---------|------------|
| **Buffer Size** | Buffer size for transfers (bytes) | 8192 | 16384-32768 for faster transfers |
| **Control Encoding** | Character encoding | UTF-8 | UTF-8 (usually) |
| **Transfer Mode** | ASCII or Binary | Binary | Binary for most files |
| **Keep-Alive Interval** | Time between keep-alive probes (ms) | 60000 | 60000 |
| **Proxy Host** | Proxy server hostname | (optional) | Only if using a proxy |
| **Proxy Port** | Proxy server port | (optional) | Only if using a proxy |
| **Use Implicit SSL** | Use Implicit SSL/TLS | false | true for FTPS |
| **SSL Protocol** | SSL/TLS protocol | TLS | TLS |
| **SSL Trusted Certificate** | Path to trusted certificate | (optional) | For server validation |
| **SSL Client Certificate** | Path to client certificate | (optional) | For client authentication |
| **SSL Client Key** | Path to client key | (optional) | For client authentication |
| **SSL Key Password** | Password for client key | (optional) | For client authentication |

## Processor Configuration

### ListFTP Processor

| Property | Description | Default | Recommended |
|----------|-------------|---------|------------|
| **FTP Connection Pool** | The controller service | (required) | Your FTP connection pool |
| **Remote Directory** | Directory to list | (required) | Path to files to list |
| **Recurse Subdirectories** | Whether to list subdirectories | false | Based on your needs |
| **File Filter** | Regex for filtering files | (optional) | Useful patterns: .*\\.csv, .*\\.xml |
| **Path Filter** | Regex for filtering directories | (optional) | Typically leave empty |
| **Minimum Age** | Minimum age for files | 0 sec | 10-30 sec to avoid in-use files |
| **Maximum Age** | Maximum age for files | (optional) | Only if you need to filter older files |
| **Batch Size** | Max files per execution | 100 | 50-200 based on file size |

### GetFTP Processor

| Property | Description | Default | Recommended |
|----------|-------------|---------|------------|
| **FTP Connection Pool** | The controller service | (required) | Your FTP connection pool |
| **Remote Path** | Path on FTP server | (required) | Typically ${path} from ListFTP |
| **Completion Strategy** | What to do after fetch | None | Move for most cases |
| **Move Destination Directory** | Where to move files | (required for Move) | /archive or similar |
| **File Filter Regex** | Regex for filtering files | (optional) | Typically not needed with ListFTP |
| **Batch Size** | Max files per execution | 10 | 5-20 based on file size |

### PutFTP Processor

| Property | Description | Default | Recommended |
|----------|-------------|---------|------------|
| **FTP Connection Pool** | The controller service | (required) | Your FTP connection pool |
| **Remote Directory** | Upload directory | (required) | Path for uploads |
| **Filename** | Remote filename to use | (optional) | Use FlowFile filename attribute |
| **Conflict Resolution Strategy** | How to handle existing files | Fail | Rename for safety |
| **Create Directory** | Create dir if missing | false | true in most cases |
| **Temporary Filename** | Use dot-prefix during upload | true | true (recommended) |
| **Transfer Mode** | ASCII, Binary, Auto | Binary | Auto Detect for mixed content |

## Error Handling

Enable comprehensive error handling with these configuration tips:

1. **Circuit Breaker Settings**
   - Lower the failure threshold for unstable servers
   - Set a reasonable recovery timeout (5-15 minutes)

2. **Retry Policy**
   - Configure reasonable retry counts (3-5)
   - Use exponential backoff for network issues

3. **Health Monitoring**
   - Set appropriate health check intervals
   - Configure meaningful health thresholds

## Performance Tuning

Optimize performance using these guidelines:

1. **Connection Pool Size**
   - Start with 5 connections
   - Increase if you see wait times in metrics
   - Don't exceed 20 connections per pool

2. **Buffer Sizes**
   - Increase buffer size for large files
   - 32KB works well for most cases

3. **Socket Parameters**
   - Enable TCP keep-alive
   - Adjust timeouts for slower networks

4. **Processor Scheduling**
   - Run ListFTP less frequently than GetFTP
   - Use appropriate batch sizes

## JMX Monitoring

Monitor the FTP connection pool using JMX:

1. Enable JMX in NiFi (if not already enabled)
2. Connect with a JMX client like JConsole or VisualVM
3. Navigate to the FTPConnectionJMXMonitor MBean
4. Key metrics to monitor:
   - ConnectionPoolUtilization
   - AverageOperationLatency
   - ErrorRate
   - ConnectionHealthScore

## Troubleshooting

### Common Issues and Solutions

1. **Connection Failures**
   - Check network connectivity
   - Verify firewall settings
   - Validate hostname resolution

2. **Authentication Errors**
   - Verify username and password
   - Check for expired credentials
   - Ensure user has correct permissions

3. **Transfer Failures**
   - Check disk space on server
   - Verify file permissions
   - Increase timeouts for large files

4. **Passive Mode Issues**
   - Try using active mode
   - Check firewall for passive port range
   - Ensure server allows passive connections

5. **Performance Problems**
   - Increase connection pool size
   - Adjust buffer sizes
   - Monitor system resources

### Logging

Enable detailed logging by setting the following log levels in logback.xml:

```xml
<logger name="org.apache.nifi.controllers.ftp" level="DEBUG"/>
<logger name="org.apache.nifi.controllers.ftp.FTPConnectionPool" level="DEBUG"/>
<logger name="org.apache.nifi.controllers.ftp.FTPOperations" level="DEBUG"/>
```

## Secure Configuration

Follow these guidelines for secure configurations:

1. **Use FTPS where possible**
   - Enable "Use Implicit SSL" option
   - Set appropriate SSL Protocol
   - Configure trusted certificates

2. **Credential Handling**
   - Use NiFi's sensitive properties for passwords
   - Consider using credential providers

3. **Network Security**
   - Use private networks where possible
   - Restrict IP ranges in firewalls
   - Secure passive port ranges