// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectionProperties;
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.fs.FileSystemFactory;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class StorageProperties extends ConnectionProperties {

    public static final String FS_HDFS_SUPPORT = "fs.hdfs.support";
    public static final String FS_S3_SUPPORT = "fs.s3.support";
    public static final String FS_GCS_SUPPORT = "fs.gcs.support";
    public static final String FS_MINIO_SUPPORT = "fs.minio.support";
    public static final String FS_OZONE_SUPPORT = "fs.ozone.support";
    public static final String FS_BROKER_SUPPORT = "fs.broker.support";
    public static final String FS_AZURE_SUPPORT = "fs.azure.support";
    public static final String FS_OSS_SUPPORT = "fs.oss.support";
    public static final String FS_OBS_SUPPORT = "fs.obs.support";
    public static final String FS_COS_SUPPORT = "fs.cos.support";
    public static final String FS_OSS_HDFS_SUPPORT = "fs.oss-hdfs.support";
    public static final String FS_LOCAL_SUPPORT = "fs.local.support";
    public static final String FS_HTTP_SUPPORT = "fs.http.support";

    public static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";
    protected static final String URI_KEY = "uri";

    public static final String FS_PROVIDER = FileSystemPropertyKeys.FS_PROVIDER;
    public static final String FS_PROVIDER_KEY = FileSystemPropertyKeys.LEGACY_PROVIDER;

    protected final String userFsPropsPrefix = "fs.";

    public enum Type {
        HDFS,
        S3,
        OSS,
        OBS,
        COS,
        GCS,
        OSS_HDFS,
        MINIO,
        OZONE,
        AZURE,
        BROKER,
        LOCAL,
        HTTP,
        UNKNOWN
    }

    public abstract Map<String, String> getBackendConfigProperties();

    /**
     * Hadoop storage configuration used for interacting with HDFS-based systems.
     * <p>
     * Currently, some underlying APIs in Hive and Iceberg still rely on the HDFS protocol directly.
     * Because of this, we must introduce an additional storage layer conversion here to adapt
     * our system's storage abstraction to the HDFS protocol.
     * <p>
     * In the future, once we have unified the storage access layer by implementing our own
     * FileIO abstraction (a custom, unified interface for file system access),
     * this conversion layer will no longer be necessary. The FileIO abstraction
     * will provide seamless and consistent access to different storage backends,
     * eliminating the need to rely on HDFS protocol specifics.
     * <p>
     * This approach will simplify the integration and improve maintainability
     * by standardizing the way storage systems are accessed.
     */
    @Getter
    public Configuration hadoopStorageConfig;

    private transient FileSystemProvider fileSystemProvider;

    @Getter
    private FileSystemProperties fileSystemProperties;

    /**
     * Get backend configuration properties with optional runtime properties.
     * This method allows passing runtime properties (like vended credentials)
     * that should be merged with the base configuration.
     *
     * @param runtimeProperties additional runtime properties to merge, can be null
     * @return Map of backend properties including runtime properties
     */
    public Map<String, String> getBackendConfigProperties(Map<String, String> runtimeProperties) {
        Map<String, String> properties = new HashMap<>(getBackendConfigProperties());
        if (runtimeProperties != null && !runtimeProperties.isEmpty()) {
            properties.putAll(runtimeProperties);
        }
        return properties;
    }

    @Getter
    protected Type type;


    /**
     * Creates a list of StorageProperties instances based on the provided properties.
     * <p>
     * This method iterates through all registered storage providers and constructs one
     * {@link StorageProperties} instance for each provider that recognizes the given properties.
     * <p>
     * If no HDFSProperties is explicitly configured, a default HDFSProperties will be added
     * automatically. The default HDFSProperties is inserted at index 0 to ensure that:
     * <ul>
     *   <li>The list preserves a deterministic order (it is an ordered List).</li>
     *   <li>The default HDFS configuration does not override or shadow explicitly configured
     *       object storage providers, which are appended after detection.</li>
     * </ul>
     *
     * @param origProps the raw property map used to initialize each StorageProperties instance
     * @return an ordered list of StorageProperties instances
     */
    public static List<StorageProperties> createAll(Map<String, String> origProps) throws UserException {
        List<StorageProperties> fileSystemProviderProperties = createAllFromFileSystemProviders(origProps);
        if (!fileSystemProviderProperties.isEmpty()) {
            return fileSystemProviderProperties;
        }
        if (hasAnyExplicitProvider(origProps)) {
            throw new StoragePropertiesException("No FileSystemProvider found for explicit storage configuration: "
                    + origProps.keySet());
        }

        List<StorageProperties> result = new ArrayList<>();
        if (!hasAnyExplicitProvider(origProps)) {
            result.add(0, new HdfsProperties(origProps, false));
        }
        for (StorageProperties storageProperties : result) {
            storageProperties.initNormalizeAndCheckProps();
            storageProperties.buildHadoopStorageConfig();
        }
        return result;
    }

    /**
     * Creates a primary StorageProperties instance based on the provided properties.
     * <p>
     * This method iterates through the list of supported storage types and returns the first
     * matching StorageProperties instance. If no supported type is found, an exception is thrown.
     *
     * @param origProps the original properties map to create the StorageProperties instance
     * @return a StorageProperties instance for the primary storage type
     * @throws RuntimeException if no supported storage type is found
     */
    public static StorageProperties createPrimary(Map<String, String> origProps) {
        StorageProperties fileSystemProviderProperties = createPrimaryFromFileSystemProvider(origProps);
        if (fileSystemProviderProperties != null) {
            return fileSystemProviderProperties;
        }

        throw new StoragePropertiesException("No supported storage type found. Please check your configuration.");
    }

    protected StorageProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }

    public String getFileSystemProviderName() {
        if (fileSystemProvider != null) {
            return fileSystemProvider.name();
        }
        return getStorageName();
    }

    private void bindFileSystemProperties(FileSystemProvider provider, FileSystemProperties properties) {
        this.fileSystemProvider = provider;
        this.fileSystemProperties = properties;
    }

    private static List<StorageProperties> createAllFromFileSystemProviders(Map<String, String> origProps)
            throws UserException {
        List<FileSystemProvider> providers = FileSystemFactory.resolveProviders(origProps);
        if (providers.isEmpty()) {
            return new ArrayList<>();
        }

        List<StorageProperties> result = new ArrayList<>();
        boolean allowDefaultHdfs = !hasAnyExplicitProvider(origProps);
        for (FileSystemProvider provider : providers) {
            StorageProperties storageProperties = createFromFileSystemProvider(provider, origProps);
            if (storageProperties != null) {
                result.add(storageProperties);
            }
        }
        if (allowDefaultHdfs && result.stream().noneMatch(HdfsProperties.class::isInstance)) {
            StorageProperties hdfsProperties = new HdfsProperties(origProps, false);
            hdfsProperties.initNormalizeAndCheckProps();
            hdfsProperties.buildHadoopStorageConfig();
            result.add(0, hdfsProperties);
        }
        return result;
    }

    private static StorageProperties createPrimaryFromFileSystemProvider(Map<String, String> origProps) {
        List<FileSystemProvider> providers = FileSystemFactory.resolveProviders(origProps);
        for (FileSystemProvider provider : providers) {
            StorageProperties storageProperties = createFromFileSystemProvider(provider, origProps);
            if (storageProperties != null) {
                return storageProperties;
            }
        }
        return null;
    }

    private static StorageProperties createFromFileSystemProvider(
            FileSystemProvider provider, Map<String, String> origProps) {
        FileSystemProperties fileSystemProperties = provider.bind(origProps);
        fileSystemProperties.validate();
        Map<String, String> boundProps = provider.toStoragePropertiesKv(origProps, fileSystemProperties);
        String storageType = boundProps.getOrDefault(FileSystemPropertyKeys.STORAGE_TYPE, provider.storageType());
        StorageProperties storageProperties = createProviderBackedStorageProperties(provider, storageType, boundProps);
        storageProperties.bindFileSystemProperties(provider, fileSystemProperties);
        storageProperties.initNormalizeAndCheckProps();
        storageProperties.buildHadoopStorageConfig();
        return storageProperties;
    }

    private static StorageProperties createProviderBackedStorageProperties(
            FileSystemProvider provider, String storageType, Map<String, String> props) {
        Type type = findStorageType(storageType);
        if (type == null) {
            type = Type.UNKNOWN;
        }
        if (isObjectStorageProperties(type, props)) {
            return new ProviderBackedObjectStorageProperties(type, provider.name(), props);
        }
        if (type == Type.HDFS) {
            return new HdfsProperties(props);
        }
        return new ProviderBackedStorageProperties(type, provider.name(), props);
    }

    private static Type findStorageType(String storageType) {
        if (StringUtils.isBlank(storageType)) {
            return null;
        }
        String normalizedStorageType = normalizeProviderName(storageType);
        for (Type type : Type.values()) {
            if (type.name().equals(normalizedStorageType)) {
                return type;
            }
        }
        return null;
    }

    private static boolean isObjectStorageProperties(Type type, Map<String, String> props) {
        switch (type) {
            case S3:
            case OSS:
            case OBS:
            case COS:
            case GCS:
            case MINIO:
            case OZONE:
                return true;
            default:
                return StringUtils.isNotBlank(props.get("AWS_ENDPOINT"))
                        || StringUtils.isNotBlank(props.get("AWS_REGION"));
        }
    }

    private static boolean hasAnyExplicitProvider(Map<String, String> props) {
        return StringUtils.isNotBlank(props.get(FileSystemPropertyKeys.STORAGE_TYPE))
                || StringUtils.isNotBlank(props.get(FS_PROVIDER))
                || StringUtils.isNotBlank(props.get(FS_PROVIDER_KEY))
                || hasAnyExplicitFsSupport(props);
    }

    private static String normalizeProviderName(String providerName) {
        String normalized = providerName.replace('-', '_').toUpperCase();
        if ("GCP".equals(normalized)) {
            return Type.GCS.name();
        }
        return normalized;
    }

    /**
     * Checks whether the user has explicitly set any {@code fs.xx.support=true} property.
     * <p>
     * Provider matching is owned by {@link FileSystemFactory} and
     * {@link FileSystemProvider#supports(Map)}. This flag is only used here to decide
     * whether the legacy default-HDFS fallback may be injected after provider binding.
     *
     * @param props the raw property map from user configuration
     * @return {@code true} if any {@code fs.xx.support} property is explicitly set to "true"
     */
    private static boolean hasAnyExplicitFsSupport(Map<String, String> props) {
        return FileSystemPropertyKeys.hasAnyExplicitFileSystemSupport(props);
    }

    private static class ProviderBackedStorageProperties extends StorageProperties {
        protected final String providerName;

        private ProviderBackedStorageProperties(Type type, String providerName, Map<String, String> props) {
            super(type, props);
            this.providerName = providerName;
        }

        @Override
        public void initNormalizeAndCheckProps() {
            // Provider-owned FileSystemProperties has already been bound and validated.
        }

        @Override
        public Map<String, String> getBackendConfigProperties() {
            return origProps;
        }

        @Override
        public String validateAndNormalizeUri(String url) throws UserException {
            return url;
        }

        @Override
        public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
            return loadProps.get(URI_KEY);
        }

        @Override
        public String getStorageName() {
            return providerName;
        }

        @Override
        protected void initializeHadoopStorageConfig() {
            hadoopStorageConfig = new Configuration();
            origProps.forEach(hadoopStorageConfig::set);
        }

        @Override
        protected Set<String> schemas() {
            return ImmutableSet.of();
        }
    }

    private static class ProviderBackedObjectStorageProperties extends AbstractS3CompatibleProperties {
        private static final Set<Pattern> ENDPOINT_PATTERNS = ImmutableSet.of(
                Pattern.compile("^(?:https?://)?s3[.-]([a-z0-9-]+)\\.amazonaws\\.com(?:/.*)?$",
                        Pattern.CASE_INSENSITIVE),
                Pattern.compile("^(?:https?://)?oss-([a-z0-9-]+?)(?:-internal)?\\.aliyuncs\\.com(?:/.*)?$",
                        Pattern.CASE_INSENSITIVE),
                Pattern.compile("^(?:https?://)?cos\\.([a-z0-9-]+)\\.myqcloud\\.com(?:/.*)?$",
                        Pattern.CASE_INSENSITIVE),
                Pattern.compile("^(?:https?://)?obs\\.([a-z0-9-]+)\\.myhuaweicloud\\.com(?:/.*)?$",
                        Pattern.CASE_INSENSITIVE));

        private final String providerName;
        private String endpoint;
        private String region;
        private String accessKey;
        private String secretKey;
        private String sessionToken;
        private String maxConnections;
        private String requestTimeoutS;
        private String connectionTimeoutS;
        private String usePathStyle;
        private String forceParsingByStandardUrl;

        private ProviderBackedObjectStorageProperties(Type type, String providerName, Map<String, String> props) {
            super(type, props);
            this.providerName = providerName;
            hydrateFromProperties();
        }

        @Override
        public void initNormalizeAndCheckProps() {
            hydrateFromProperties();
            super.initNormalizeAndCheckProps();
        }

        private void hydrateFromProperties() {
            String prefix = storagePrefix();
            endpoint = firstNonBlank(origProps.get("AWS_ENDPOINT"), origProps.get(prefix + ".endpoint"),
                    origProps.get("s3.endpoint"), origProps.get("endpoint"));
            region = firstNonBlank(origProps.get("AWS_REGION"), origProps.get(prefix + ".region"),
                    origProps.get("s3.region"), defaultRegion());
            accessKey = firstNonBlank(origProps.get("AWS_ACCESS_KEY"), origProps.get(prefix + ".access_key"),
                    origProps.get(prefix + ".access-key-id"), origProps.get("s3.access_key"),
                    origProps.get("s3.access-key-id"));
            secretKey = firstNonBlank(origProps.get("AWS_SECRET_KEY"), origProps.get(prefix + ".secret_key"),
                    origProps.get(prefix + ".secret-access-key"), origProps.get("s3.secret_key"),
                    origProps.get("s3.secret-access-key"));
            sessionToken = firstNonBlank(origProps.get("AWS_TOKEN"), origProps.get(prefix + ".session_token"),
                    origProps.get(prefix + ".session-token"), origProps.get("s3.session_token"),
                    origProps.get("s3.session-token"));
            maxConnections = firstNonBlank(origProps.get("AWS_MAX_CONNECTIONS"),
                    origProps.get(prefix + ".connection.maximum"), origProps.get("s3.connection.maximum"), "50");
            requestTimeoutS = firstNonBlank(origProps.get("AWS_REQUEST_TIMEOUT_MS"),
                    origProps.get(prefix + ".connection.request.timeout"),
                    origProps.get("s3.connection.request.timeout"), "3000");
            connectionTimeoutS = firstNonBlank(origProps.get("AWS_CONNECTION_TIMEOUT_MS"),
                    origProps.get(prefix + ".connection.timeout"), origProps.get("s3.connection.timeout"), "1000");
            usePathStyle = firstNonBlank(origProps.get("use_path_style"), origProps.get(prefix + ".use_path_style"),
                    origProps.get(prefix + ".path-style-access"), origProps.get("s3.path-style-access"), "false");
            forceParsingByStandardUrl = firstNonBlank(origProps.get("force_parsing_by_standard_uri"), "false");
        }

        private String storagePrefix() {
            switch (type) {
                case OSS:
                    return "oss";
                case COS:
                    return "cos";
                case OBS:
                    return "obs";
                case GCS:
                    return "gs";
                case MINIO:
                    return "minio";
                case OZONE:
                    return "ozone";
                default:
                    return "s3";
            }
        }

        private String defaultRegion() {
            if (type == Type.GCS) {
                return "us-east1";
            }
            if (type == Type.MINIO || type == Type.OZONE) {
                return "us-east-1";
            }
            return "";
        }

        private static String firstNonBlank(String... values) {
            for (String value : values) {
                if (StringUtils.isNotBlank(value)) {
                    return value;
                }
            }
            return "";
        }

        @Override
        public Map<String, String> getBackendConfigProperties() {
            Map<String, String> backendProperties = generateBackendS3Configuration();
            putIfPresent(backendProperties, "AWS_ROLE_ARN", origProps.get("AWS_ROLE_ARN"));
            putIfPresent(backendProperties, "AWS_EXTERNAL_ID", origProps.get("AWS_EXTERNAL_ID"));
            return backendProperties;
        }

        private static void putIfPresent(Map<String, String> properties, String key, String value) {
            if (StringUtils.isNotBlank(value)) {
                properties.put(key, value);
            }
        }

        @Override
        protected Set<Pattern> endpointPatterns() {
            return ENDPOINT_PATTERNS;
        }

        @Override
        protected String getEndpointFromRegion() {
            if (StringUtils.isNotBlank(endpoint)) {
                return endpoint;
            }
            if (type == Type.GCS) {
                return "https://storage.googleapis.com";
            }
            if (type == Type.S3 && StringUtils.isNotBlank(region)) {
                return "https://s3." + region + ".amazonaws.com";
            }
            return "";
        }

        @Override
        public String getStorageName() {
            return providerName;
        }

        @Override
        protected Set<String> schemas() {
            switch (type) {
                case OSS:
                    return ImmutableSet.of("oss");
                case COS:
                    return ImmutableSet.of("cos", "cosn");
                case OBS:
                    return ImmutableSet.of("obs");
                case GCS:
                    return ImmutableSet.of("gs");
                default:
                    return ImmutableSet.of("s3", "s3a", "s3n");
            }
        }

        @Override
        public String getEndpoint() {
            return endpoint;
        }

        @Override
        public String getRegion() {
            return region;
        }

        @Override
        public String getAccessKey() {
            return accessKey;
        }

        @Override
        public String getSecretKey() {
            return secretKey;
        }

        @Override
        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void setRegion(String region) {
            this.region = region;
        }

        @Override
        public String getSessionToken() {
            return sessionToken;
        }

        @Override
        public String getMaxConnections() {
            return maxConnections;
        }

        @Override
        public String getRequestTimeoutS() {
            return requestTimeoutS;
        }

        @Override
        public String getConnectionTimeoutS() {
            return connectionTimeoutS;
        }

        @Override
        public String getUsePathStyle() {
            return usePathStyle;
        }

        @Override
        public String getForceParsingByStandardUrl() {
            return forceParsingByStandardUrl;
        }
    }

    protected static boolean checkIdentifierKey(Map<String, String> origProps, List<Field> fields) {
        for (Field field : fields) {
            field.setAccessible(true);
            ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
            for (String key : annotation.names()) {
                if (origProps.containsKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Validates the given URL string and returns a normalized URI in the format: scheme://authority/path.
     * <p>
     * This method checks that the input is non-empty, the scheme is present and supported (e.g., hdfs, viewfs),
     * and converts it into a canonical URI string.
     *
     * @param url the raw URL string to validate and normalize
     * @return a normalized URI string with validated scheme and authority
     * @throws UserException if the URL is empty, lacks a valid scheme, or contains an unsupported scheme
     */
    public abstract String validateAndNormalizeUri(String url) throws UserException;

    /**
     * Extracts the URI string from the provided properties map, validates it, and returns the normalized URI.
     * <p>
     * This method checks that the 'uri' key exists in the property map, retrieves the value,
     * and then delegates to {@link #validateAndNormalizeUri(String)} for further validation and normalization.
     *
     * @param loadProps the map containing load-related properties, including the URI under the key 'uri'
     * @return a normalized and validated URI string
     * @throws UserException if the 'uri' property is missing, empty, or invalid
     */
    public abstract String validateAndGetUri(Map<String, String> loadProps) throws UserException;

    public abstract String getStorageName();

    private void buildHadoopStorageConfig() {
        initializeHadoopStorageConfig();
        if (null == hadoopStorageConfig) {
            return;
        }
        appendUserFsConfig(origProps);
        ensureDisableCache(hadoopStorageConfig, origProps);
    }

    private void appendUserFsConfig(Map<String, String> userProps) {
        userProps.forEach((k, v) -> {
            if (k.startsWith(userFsPropsPrefix) && StringUtils.isNotBlank(v)) {
                hadoopStorageConfig.set(k, v);
            }
        });
    }

    protected abstract void initializeHadoopStorageConfig();

    protected abstract Set<String> schemas();

    /**
     * By default, Hadoop caches FileSystem instances per scheme and authority (e.g. s3a://bucket/), meaning that all
     * subsequent calls using the same URI will reuse the same FileSystem object.
     * In multi-tenant or dynamic credential environments — where different users may access the same bucket using
     * different access keys or tokens — this cache reuse can lead to cross-credential contamination.
     * <p>
     * Specifically, if the cache is not disabled, a FileSystem instance initialized with one set of credentials may
     * be reused by another session targeting the same bucket but with a different AK/SK. This results in:
     * <p>
     * Incorrect authentication (using stale credentials)
     * <p>
     * Unexpected permission errors or access denial
     * <p>
     * Potential data leakage between users
     * <p>
     * To avoid such risks, the configuration property
     * fs.<schema>.impl.disable.cache
     * must be set to true for all object storage backends (e.g., S3A, OSS, COS, OBS), ensuring that each new access
     * creates an isolated FileSystem instance with its own credentials and configuration context.
     */
    private void ensureDisableCache(Configuration conf, Map<String, String> origProps) {
        for (String schema : schemas()) {
            String key = "fs." + schema + ".impl.disable.cache";
            String userValue = origProps.get(key);
            if (StringUtils.isNotBlank(userValue)) {
                conf.setBoolean(key, BooleanUtils.toBoolean(userValue));
            } else {
                conf.setBoolean(key, true);
            }
        }
    }
}
