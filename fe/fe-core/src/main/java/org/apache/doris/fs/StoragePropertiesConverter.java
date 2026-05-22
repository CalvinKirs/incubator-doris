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

package org.apache.doris.fs;

import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Converts legacy {@link StorageProperties} objects to the {@code Map<String, String>} format
 * expected by {@link org.apache.doris.filesystem.FileSystemProvider#supports} and
 * {@link org.apache.doris.filesystem.FileSystemProvider#create}.
 *
 * <p>This class is the ONLY place in fe-core that knows about the mapping between
 * StorageProperties subtypes and filesystem property keys. All other code in fe-core
 * should use {@link FileSystemFactory#get(Map)} or {@link FileSystemFactory#get(StorageProperties)}.
 */
public final class StoragePropertiesConverter {

    private StoragePropertiesConverter() {}

    /**
     * Converts a {@link StorageProperties} instance to a flat {@code Map<String, String>}
     * suitable for passing to {@code FileSystemProvider.supports()} and
     * {@code FileSystemProvider.create()}.
     *
     * <p>Object storage properties are converted to FE filesystem keys directly: S3-compatible
     * storage uses {@code AWS_*}, while native COS/OBS/OSS storage uses {@code COS_*},
     * {@code OBS_*}, or {@code OSS_*}. Non-object-storage types use
     * {@code getBackendConfigProperties()} as the base map plus a type marker.
     */
    public static Map<String, String> toMap(StorageProperties props) {
        if (props instanceof AbstractS3CompatibleProperties) {
            Map<String, String> map = new HashMap<>();
            AbstractS3CompatibleProperties s3Props = (AbstractS3CompatibleProperties) props;
            addS3CompatibleProperties(map, s3Props);
            return map;
        }

        Map<String, String> map = new HashMap<>(props.getBackendConfigProperties());

        if (props instanceof AzureProperties) {
            AzureProperties azureProps = (AzureProperties) props;
            map.put("AZURE_ACCOUNT_NAME", azureProps.getAccountName());
            if (StringUtils.isNotBlank(azureProps.getAccountKey())) {
                map.put("AZURE_ACCOUNT_KEY", azureProps.getAccountKey());
            }
            if (StringUtils.isNotBlank(azureProps.getEndpoint())) {
                map.put("AZURE_ENDPOINT", azureProps.getEndpoint());
            }
            map.put("_STORAGE_TYPE_", "AZURE");
        } else if (props instanceof HdfsCompatibleProperties) {
            map.put("_STORAGE_TYPE_", "HDFS");
        } else if (props instanceof BrokerProperties) {
            map.put("_STORAGE_TYPE_", "BROKER");
        }

        return map;
    }

    private static void addS3CompatibleProperties(Map<String, String> map,
            AbstractS3CompatibleProperties s3Props) {
        String storageType = toFileSystemStorageType(s3Props);
        String prefix = toFileSystemPrefix(s3Props);
        map.put("_STORAGE_TYPE_", storageType);
        putIfNotBlank(map, prefix + "_ENDPOINT", s3Props.getEndpoint());
        putIfNotBlank(map, prefix + "_REGION", s3Props.getRegion());
        putIfNotBlank(map, prefix + "_ACCESS_KEY", s3Props.getAccessKey());
        putIfNotBlank(map, prefix + "_SECRET_KEY", s3Props.getSecretKey());
        putIfNotBlank(map, prefix + "_BUCKET", s3Props.getBucket());
        putIfNotBlank(map, prefix + "_MAX_CONNECTIONS", s3Props.getMaxConnections());
        putIfNotBlank(map, prefix + "_REQUEST_TIMEOUT_MS", s3Props.getRequestTimeoutS());
        putIfNotBlank(map, prefix + "_CONNECTION_TIMEOUT_MS", s3Props.getConnectionTimeoutS());
        putIfNotBlank(map, "use_path_style", s3Props.getUsePathStyle());

        String sessionToken = s3Props.getSessionToken();
        if (StringUtils.isNotBlank(sessionToken)) {
            map.put(prefix + "_TOKEN", sessionToken);
            if (!"AWS".equals(prefix)) {
                map.put(prefix + "_SESSION_TOKEN", sessionToken);
            }
        }
        addStsProperties(map, s3Props, prefix);
    }

    private static String toFileSystemStorageType(AbstractS3CompatibleProperties s3Props) {
        switch (s3Props.getType()) {
            case COS:
                return "COS";
            case OBS:
                return "OBS";
            case OSS:
                return "OSS";
            default:
                return "S3";
        }
    }

    private static String toFileSystemPrefix(AbstractS3CompatibleProperties s3Props) {
        switch (s3Props.getType()) {
            case COS:
                return "COS";
            case OBS:
                return "OBS";
            case OSS:
                return "OSS";
            default:
                return "AWS";
        }
    }

    private static void addStsProperties(Map<String, String> map,
            AbstractS3CompatibleProperties s3Props, String prefix) {
        if ("AWS".equals(prefix)) {
            putIfNotBlank(map, "AWS_ROLE_ARN", firstNonBlank(s3Props, "sts.role_arn", "AWS_ROLE_ARN"));
            putIfNotBlank(map, "AWS_EXTERNAL_ID", firstNonBlank(s3Props, "sts.external_id", "AWS_EXTERNAL_ID"));
            return;
        }

        String provider = prefix.toLowerCase(Locale.ROOT);
        putIfNotBlank(map, prefix + "_ROLE_ARN",
                firstNonBlank(s3Props, provider + ".role_arn", prefix + "_ROLE_ARN", "sts.role_arn"));
        if ("OBS".equals(prefix)) {
            putIfNotBlank(map, "OBS_AGENCY_NAME",
                    firstNonBlank(s3Props, "obs.agency_name", "OBS_AGENCY_NAME"));
            putIfNotBlank(map, "OBS_DOMAIN_NAME",
                    firstNonBlank(s3Props, "obs.domain_name", "OBS_DOMAIN_NAME"));
        }
    }

    private static String firstNonBlank(AbstractS3CompatibleProperties s3Props, String... keys) {
        for (String key : keys) {
            String value = s3Props.getOrigProps().get(key);
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static void putIfNotBlank(Map<String, String> map, String key, String value) {
        if (StringUtils.isNotBlank(value)) {
            map.put(key, value);
        }
    }
}
