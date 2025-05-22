package org.apache.nifi.controllers.ftp;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.security.util.crypto.CipherProvider;
import org.apache.nifi.security.util.crypto.CipherProviderFactory;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.apache.nifi.security.util.KeyStoreUtils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles secure storage and retrieval of credentials for FTP connections.
 * This class is used to securely handle sensitive information like passwords.
 */
public class SecureCredentialHandler {
    private final ComponentLog logger;
    private final Map<String, String> secureCredentials = new ConcurrentHashMap<>();
    private final Map<String, byte[]> saltMap = new ConcurrentHashMap<>();
    
    // Improved encryption settings
    private static final String DEFAULT_KEY_SEED = "FTPConnectionPoolSecureKey";
    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.AES_GCM;
    private static final int SALT_LENGTH = 16; // 128 bits
    private static final int KEY_LENGTH = 32; // 256 bits
    private static final int ITERATIONS = 10000;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    
    // Credential types for enhanced type safety
    public enum CredentialType {
        PASSWORD("password"),
        API_KEY("apiKey"),
        TOKEN("token"),
        PROXY_PASSWORD("proxyPassword"),
        CERTIFICATE_PASSWORD("certificatePassword"),
        KEYSTORE_PASSWORD("keystorePassword"),
        TRUSTSTORE_PASSWORD("truststorePassword");
        
        private final String key;
        
        CredentialType(String key) {
            this.key = key;
        }
        
        public String getKey() {
            return key;
        }
    }
    
    /**
     * Creates a new SecureCredentialHandler with the given logger.
     * 
     * @param logger the logger to use
     */
    public SecureCredentialHandler(ComponentLog logger) {
        this.logger = logger;
    }
    
    /**
     * Stores a credential securely using an enum type for improved type safety.
     * 
     * @param type the credential type to store
     * @param value the value to store
     */
    public void storeCredential(CredentialType type, String value) {
        storeCredential(type.getKey(), value);
    }
    
    /**
     * Stores a credential securely.
     * 
     * @param key the key to store the credential under
     * @param value the value to store
     */
    public void storeCredential(String key, String value) {
        try {
            if (value == null || value.isEmpty()) {
                secureCredentials.remove(key);
                saltMap.remove(key);
                return;
            }
            
            // Generate a unique salt for this credential
            byte[] salt = new byte[SALT_LENGTH];
            SECURE_RANDOM.nextBytes(salt);
            saltMap.put(key, salt);
            
            // Encrypt the credential before storing it
            String encryptedValue = encrypt(value, salt);
            secureCredentials.put(key, encryptedValue);
            
            logger.debug("Stored credential securely: {}", key);
            
        } catch (Exception e) {
            logger.error("Failed to store credential securely: {}", key, e);
            throw new ProcessException("Failed to store credential securely: " + key, e);
        }
    }
    
    /**
     * Retrieves a credential securely using an enum type for improved type safety.
     * 
     * @param type the credential type to retrieve
     * @return the decrypted credential value
     */
    public String retrieveCredential(CredentialType type) {
        return retrieveCredential(type.getKey());
    }
    
    /**
     * Retrieves a credential securely.
     * 
     * @param key the key to retrieve the credential for
     * @return the decrypted credential value
     */
    public String retrieveCredential(String key) {
        try {
            String encryptedValue = secureCredentials.get(key);
            if (encryptedValue == null || encryptedValue.isEmpty()) {
                return null;
            }
            
            byte[] salt = saltMap.get(key);
            if (salt == null) {
                logger.warn("No salt found for credential: {}", key);
                return null;
            }
            
            // Decrypt the credential before returning it
            return decrypt(encryptedValue, salt);
            
        } catch (Exception e) {
            logger.error("Failed to retrieve credential securely: {}", key, e);
            throw new ProcessException("Failed to retrieve credential securely: " + key, e);
        }
    }
    
    /**
     * Loads credentials from a configuration context.
     * 
     * @param context the configuration context
     * @param serviceClass the service class to get the property descriptors from
     */
    public void loadCredentialsFromContext(ConfigurationContext context, Class<?> serviceClass) {
        try {
            // Get all property descriptors from the service class
            List<PropertyDescriptor> descriptors = findSensitivePropertyDescriptors(serviceClass);
            
            for (PropertyDescriptor descriptor : descriptors) {
                PropertyValue propertyValue = context.getProperty(descriptor);
                if (propertyValue != null && propertyValue.isSet()) {
                    String propertyName = descriptor.getName().toLowerCase().replace(" ", "");
                    storeCredential(propertyName, propertyValue.getValue());
                    logger.debug("Loaded credential from context: {}", propertyName);
                }
            }
            
            // Specifically handle password for backward compatibility
            PropertyValue passwordProperty = context.getProperty(PersistentFTPConnectionService.PASSWORD);
            if (passwordProperty != null && passwordProperty.isSet()) {
                storeCredential(CredentialType.PASSWORD, passwordProperty.getValue());
            }
            
            // Handle proxy password if present
            if (context.getPropertyKeys().contains("Proxy Password")) {
                PropertyValue proxyPasswordProperty = context.getProperty("Proxy Password");
                if (proxyPasswordProperty != null && proxyPasswordProperty.isSet()) {
                    storeCredential(CredentialType.PROXY_PASSWORD, proxyPasswordProperty.getValue());
                }
            }
            
        } catch (Exception e) {
            logger.error("Failed to load credentials from context", e);
            throw new ProcessException("Failed to load credentials from context", e);
        }
    }
    
    /**
     * Finds all sensitive property descriptors in a service class.
     * 
     * @param serviceClass the service class to search
     * @return a list of sensitive property descriptors
     */
    private List<PropertyDescriptor> findSensitivePropertyDescriptors(Class<?> serviceClass) {
        List<PropertyDescriptor> sensitiveProperties = new ArrayList<>();
        
        try {
            // Find all static fields of type PropertyDescriptor
            Field[] fields = serviceClass.getDeclaredFields();
            for (Field field : fields) {
                if (field.getType().equals(PropertyDescriptor.class)) {
                    field.setAccessible(true);
                    PropertyDescriptor descriptor = (PropertyDescriptor) field.get(null);
                    
                    // If the property is sensitive, add it to the list
                    if (descriptor != null && descriptor.isSensitive()) {
                        sensitiveProperties.add(descriptor);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Error finding sensitive property descriptors", e);
        }
        
        return sensitiveProperties;
    }
    
    /**
     * Clears all stored credentials.
     */
    public void clearCredentials() {
        secureCredentials.clear();
        saltMap.clear();
        logger.debug("Cleared all stored credentials");
    }
    
    /**
     * Checks if a credential exists.
     * 
     * @param key the key to check
     * @return true if the credential exists, false otherwise
     */
    public boolean hasCredential(String key) {
        return secureCredentials.containsKey(key) && saltMap.containsKey(key);
    }
    
    /**
     * Checks if a credential exists using an enum type.
     * 
     * @param type the credential type to check
     * @return true if the credential exists, false otherwise
     */
    public boolean hasCredential(CredentialType type) {
        return hasCredential(type.getKey());
    }
    
    /**
     * Gets a list of all stored credential keys.
     * 
     * @return an unmodifiable list of credential keys
     */
    public List<String> getStoredCredentialKeys() {
        return Collections.unmodifiableList(new ArrayList<>(secureCredentials.keySet()));
    }
    
    /**
     * Removes a specific credential.
     * 
     * @param key the key of the credential to remove
     */
    public void removeCredential(String key) {
        secureCredentials.remove(key);
        saltMap.remove(key);
        logger.debug("Removed credential: {}", key);
    }
    
    /**
     * Removes a specific credential using an enum type.
     * 
     * @param type the credential type to remove
     */
    public void removeCredential(CredentialType type) {
        removeCredential(type.getKey());
    }
    
    /**
     * Encrypts a value with a salt for enhanced security.
     * 
     * @param value the value to encrypt
     * @param salt the salt to use for key derivation
     * @return the encrypted value
     */
    private String encrypt(String value, byte[] salt) {
        try {
            // Derive a key from the default key using the salt
            SecretKey secretKey = deriveKey(salt);
            
            final CipherProvider cipherProvider = CipherProviderFactory.getCipherProvider(ENCRYPTION_METHOD);
            
            Cipher cipher = cipherProvider.getCipher(
                    ENCRYPTION_METHOD, 
                    secretKey.getEncoded(), 
                    KeyDerivationFunction.PBKDF2, 
                    ITERATIONS, 
                    salt, 
                    true);
            
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            byte[] encryptedBytes = cipher.doFinal(valueBytes);
            
            return CipherUtility.encodeBase64NoPadding(encryptedBytes);
            
        } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException e) {
            logger.error("Failed to encrypt value", e);
            throw new ProcessException("Failed to encrypt value", e);
        }
    }
    
    /**
     * Decrypts a value with a salt.
     * 
     * @param encryptedValue the encrypted value
     * @param salt the salt used for key derivation
     * @return the decrypted value
     */
    private String decrypt(String encryptedValue, byte[] salt) {
        try {
            // Derive the same key using the stored salt
            SecretKey secretKey = deriveKey(salt);
            
            final CipherProvider cipherProvider = CipherProviderFactory.getCipherProvider(ENCRYPTION_METHOD);
            
            Cipher cipher = cipherProvider.getCipher(
                    ENCRYPTION_METHOD, 
                    secretKey.getEncoded(), 
                    KeyDerivationFunction.PBKDF2, 
                    ITERATIONS, 
                    salt, 
                    false);
            
            byte[] encryptedBytes = CipherUtility.decodeBase64NoPadding(encryptedValue);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
            
            return new String(decryptedBytes, StandardCharsets.UTF_8);
            
        } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException e) {
            logger.error("Failed to decrypt value", e);
            throw new ProcessException("Failed to decrypt value", e);
        }
    }
    
    /**
     * Derives a key from the default key seed using the provided salt.
     * 
     * @param salt the salt to use for key derivation
     * @return the derived secret key
     */
    private SecretKey deriveKey(byte[] salt) {
        try {
            // Use PBKDF2 to derive a strong key
            byte[] derivedKey = KeyStoreUtils.deriveKeyFromPassword(
                    DEFAULT_KEY_SEED.toCharArray(), 
                    salt, 
                    ITERATIONS, 
                    KEY_LENGTH);
            
            return new SecretKeySpec(derivedKey, "AES");
            
        } catch (NoSuchAlgorithmException e) {
            logger.error("Failed to derive key", e);
            throw new ProcessException("Failed to derive key", e);
        }
    }
    
    /**
     * Generates a cryptographically secure salt.
     * 
     * @return a new random salt
     */
    private byte[] generateSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }
}