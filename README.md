# Apache NiFi FTP Connection Pool

A connection pooling service for Apache NiFi that maintains persistent connections to FTP servers. This improves performance and reliability when dealing with FTP servers by reusing connections rather than establishing new connections for each operation.

## Features

- **Connection Pooling**: Efficiently manage and reuse FTP connections
- **Health Monitoring**: Automatically detect and handle unhealthy connections
- **Advanced Configuration**: Fine-grained control over connection parameters
- **Error Management**: Comprehensive error handling with recovery strategies
- **Metrics Collection**: Detailed metrics for monitoring performance
- **Circuit Breaker Pattern**: Prevent cascading failures with automatic circuit breaking
- **Retry Mechanisms**: Configurable retry policies with exponential backoff
- **Keep-Alive Management**: Keep connections alive with configurable probes
- **NiFi Integration**: Ready-to-use processors for listing, getting, and putting files

## Installation

1. Build the NAR file using Maven:
   ```
   mvn clean install
   ```

2. Copy the resulting NAR file from `target/nifi-ftp-connection-pool-nar-1.0.0.nar` to your NiFi `lib` directory.

3. Restart NiFi.

## Usage

### Controller Service Configuration

1. In the NiFi UI, add a new Controller Service.
2. Select the "PersistentFTPConnectionService" type.
3. Configure the service with your FTP server details:
   - **Host**: The FTP server hostname or IP address
   - **Port**: The FTP server port (default: 21)
   - **Username**: FTP username
   - **Password**: FTP password
   - **Connection Pool Size**: Number of connections to maintain in the pool
   - **Connection Timeout**: Timeout for establishing connections
   - **Data Timeout**: Timeout for data transfers
   - **Connection Mode**: Active or Passive connection mode
   - **Transfer Mode**: ASCII or Binary transfer mode

### Using the Provided Processors

#### ListFTP

Lists files from an FTP server using the persistent connection pool.

**Properties**:
- **FTP Connection Pool**: The controller service providing FTP connections
- **Remote Directory**: The directory to list files from
- **Recurse Subdirectories**: Whether to recursively list files in subdirectories
- **File Filter**: Regular expression to filter files
- **Path Filter**: Regular expression to filter directories
- **Minimum Age**: Minimum age of files to list
- **Maximum Age**: Maximum age of files to list
- **Batch Size**: Maximum number of files to list in a single execution

#### GetFTP

Fetches files from an FTP server using the persistent connection pool.

**Properties**:
- **FTP Connection Pool**: The controller service providing FTP connections
- **Remote Path**: The path on the FTP server to fetch files from
- **Completion Strategy**: What to do with the file after it's fetched (Delete, Move, None)
- **Move Destination Directory**: Directory to move files to if using Move completion strategy
- **File Filter Regex**: Regular expression to filter files to fetch
- **Batch Size**: Maximum number of files to fetch in a single execution

#### PutFTP

Uploads files to an FTP server using the persistent connection pool.

**Properties**:
- **FTP Connection Pool**: The controller service providing FTP connections
- **Remote Directory**: The directory on the FTP server to upload files to
- **Filename**: The filename to use (defaults to the flow file's filename attribute)
- **Conflict Resolution Strategy**: How to handle existing files (Fail, Replace, Rename)
- **Create Directory**: Whether to create the remote directory if it doesn't exist
- **Temporary Filename**: Whether to use a dot-prefix during upload
- **Transfer Mode**: ASCII, Binary, or Auto Detect

## Error Handling and Recovery

The FTP Connection Pool includes comprehensive error handling with several strategies:

1. **Circuit Breaker Pattern**: Automatically prevents requests to failing servers
2. **Categorized Error Handling**: Different strategies for different error types
3. **Exponential Backoff**: Increasingly longer delays between retry attempts
4. **Connection Health Checking**: Proactive detection of failing connections
5. **Bulletins**: Operational errors are reported as bulletins for visibility

## Metrics and Monitoring

The service collects and exposes detailed metrics through JMX:

- **Connection Statistics**: Connection pool utilization, borrow time, wait time
- **Performance Metrics**: Operation latency, throughput, success/failure rates
- **Error Metrics**: Error counts by type, recovery success rate
- **Health Metrics**: Connection health status, availability percentage

## Advanced Configuration

Advanced options can be configured as needed:

- **Buffer Size**: Control the buffer size for data transfers
- **Keep-Alive Interval**: Time between keep-alive probes
- **Socket Parameters**: Configure TCP socket parameters
- **Proxy Settings**: Connect through a proxy server
- **SSL/TLS Options**: Configure secure connections with various options

## Example Flow

Here's a simple example flow that lists, gets, and processes files from an FTP server:

1. **ListFTP** processor configured with the PersistentFTPConnectionService
2. Connect the success relationship to a **GetFTP** processor
3. Connect the success relationship to your processing processors
4. Optionally, connect processing results to a **PutFTP** processor to upload processed files

## Troubleshooting

### Common Issues

1. **Connection Refused**: Verify your FTP server is running and accessible
2. **Authentication Failed**: Check your username and password
3. **Passive Mode Issues**: Try switching to active mode if behind a firewall
4. **Performance Problems**: Increase the connection pool size

### Logging

Enable DEBUG logging for the following components to troubleshoot issues:

```
org.apache.nifi.controllers.ftp
```

## License

Apache License, Version 2.0