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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to validate version compatibility between FTP Connection Pool components
 * and the running NiFi instance.
 */
public class FTPVersionValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(FTPVersionValidator.class);
    private static final String VERSION_RESOURCE = "/org/apache/nifi/controllers/ftp/version.properties";
    private static final String VERSION_PROPERTY = "version";
    private static final String BUILD_DATE_PROPERTY = "buildDate";
    private static final String MINIMUM_NIFI_VERSION_PROPERTY = "minimumNifiVersion";
    private static final String MAXIMUM_NIFI_VERSION_PROPERTY = "maximumNifiVersion";
    
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(?:[-.](.+))?");
    
    private static String componentVersion;
    private static String componentBuildDate;
    private static String minimumNifiVersion;
    private static String maximumNifiVersion;
    
    static {
        loadVersionProperties();
    }
    
    private FTPVersionValidator() {
        // Utility class, do not instantiate
    }
    
    /**
     * Load version properties from the version.properties resource file
     */
    private static void loadVersionProperties() {
        Properties props = new Properties();
        try (InputStream is = FTPVersionValidator.class.getResourceAsStream(VERSION_RESOURCE)) {
            if (is != null) {
                props.load(is);
                componentVersion = props.getProperty(VERSION_PROPERTY);
                componentBuildDate = props.getProperty(BUILD_DATE_PROPERTY);
                minimumNifiVersion = props.getProperty(MINIMUM_NIFI_VERSION_PROPERTY);
                maximumNifiVersion = props.getProperty(MAXIMUM_NIFI_VERSION_PROPERTY);
            } else {
                LOGGER.warn("Could not find version properties resource: {}", VERSION_RESOURCE);
            }
        } catch (IOException e) {
            LOGGER.warn("Error loading version properties", e);
        }
    }
    
    /**
     * Get the component version
     * 
     * @return the component version string
     */
    public static String getComponentVersion() {
        return componentVersion;
    }
    
    /**
     * Get the component build date
     * 
     * @return the component build date string
     */
    public static String getComponentBuildDate() {
        return componentBuildDate;
    }
    
    /**
     * Get the minimum compatible NiFi version
     * 
     * @return the minimum compatible NiFi version string
     */
    public static String getMinimumNifiVersion() {
        return minimumNifiVersion;
    }
    
    /**
     * Get the maximum compatible NiFi version
     * 
     * @return the maximum compatible NiFi version string
     */
    public static String getMaximumNifiVersion() {
        return maximumNifiVersion;
    }
    
    /**
     * Check if the current NiFi version is compatible with this component
     * 
     * @param nifiVersion the NiFi version to check
     * @return true if the NiFi version is compatible, false otherwise
     */
    public static boolean isCompatibleVersion(String nifiVersion) {
        if (minimumNifiVersion == null || maximumNifiVersion == null) {
            // If we can't determine compatibility requirements, assume compatible
            return true;
        }
        
        try {
            Version current = parseVersion(nifiVersion);
            Version min = parseVersion(minimumNifiVersion);
            
            // Max version is optional (null or "none" means any future version is fine)
            Version max = null;
            if (maximumNifiVersion != null && !maximumNifiVersion.equalsIgnoreCase("none")) {
                max = parseVersion(maximumNifiVersion);
            }
            
            // Check if current version is >= minimum
            if (current.compareTo(min) < 0) {
                LOGGER.warn("NiFi version {} is less than minimum required version {}", nifiVersion, minimumNifiVersion);
                return false;
            }
            
            // Check if current version is <= maximum (if specified)
            if (max != null && current.compareTo(max) > 0) {
                LOGGER.warn("NiFi version {} is greater than maximum supported version {}", nifiVersion, maximumNifiVersion);
                return false;
            }
            
            return true;
        } catch (Exception e) {
            LOGGER.warn("Error checking version compatibility: {}", e.getMessage());
            // If we can't parse versions, assume compatible
            return true;
        }
    }
    
    /**
     * Parse a version string into a Version object
     * 
     * @param versionString the version string to parse
     * @return a Version object representing the parsed version
     */
    private static Version parseVersion(String versionString) {
        if (versionString == null || versionString.trim().isEmpty()) {
            throw new IllegalArgumentException("Version string cannot be null or empty");
        }
        
        Matcher matcher = VERSION_PATTERN.matcher(versionString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid version format: " + versionString);
        }
        
        int major = Integer.parseInt(matcher.group(1));
        int minor = Integer.parseInt(matcher.group(2));
        int patch = Integer.parseInt(matcher.group(3));
        String qualifier = matcher.group(4);
        
        return new Version(major, minor, patch, qualifier);
    }
    
    /**
     * Represents a semantic version for comparison
     */
    private static class Version implements Comparable<Version> {
        private final int major;
        private final int minor;
        private final int patch;
        private final String qualifier;
        
        public Version(int major, int minor, int patch, String qualifier) {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
            this.qualifier = qualifier;
        }
        
        @Override
        public int compareTo(Version other) {
            // Compare major version
            int result = Integer.compare(this.major, other.major);
            if (result != 0) {
                return result;
            }
            
            // Compare minor version
            result = Integer.compare(this.minor, other.minor);
            if (result != 0) {
                return result;
            }
            
            // Compare patch version
            result = Integer.compare(this.patch, other.patch);
            if (result != 0) {
                return result;
            }
            
            // If everything else is equal, compare qualifiers
            // No qualifier is "higher" than any qualifier
            if (this.qualifier == null && other.qualifier == null) {
                return 0;
            } else if (this.qualifier == null) {
                return 1;
            } else if (other.qualifier == null) {
                return -1;
            } else {
                return this.qualifier.compareTo(other.qualifier);
            }
        }
    }
}