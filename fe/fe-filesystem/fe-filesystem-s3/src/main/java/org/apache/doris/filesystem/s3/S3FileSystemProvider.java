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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for AWS S3 and S3-compatible storage (MinIO, etc.).
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by presence of AWS_ACCESS_KEY with either AWS_ENDPOINT or AWS_REGION.
 * S3 is intentionally the last-resort provider; cloud-specific providers (OSS, COS, OBS)
 * should match first via their endpoint domain patterns.
 */
public class S3FileSystemProvider implements FileSystemProvider {

    private static final Set<String> S3_COMPATIBLE_STORAGE_TYPES =
            new HashSet<>(Arrays.asList("S3", "MINIO", "GCS"));

    @Override
    public boolean supports(Map<String, String> properties) {
        String storageType = explicitStorageType(properties);
        if (storageType != null) {
            return S3_COMPATIBLE_STORAGE_TYPES.contains(storageType.toUpperCase());
        }
        if ("true".equalsIgnoreCase(properties.get("fs.s3.support"))
                || "true".equalsIgnoreCase(properties.get("fs.minio.support"))
                || "true".equalsIgnoreCase(properties.get("fs.gcs.support"))) {
            return true;
        }
        S3FileSystemProperties boundProperties = S3FileSystemProperties.bind(properties);
        if (isKnownCloudSpecificEndpoint(boundProperties.toFileSystemKv().get(S3ObjStorage.PROP_ENDPOINT))) {
            return false;
        }
        return boundProperties.hasLocation();
    }

    @Override
    public FileSystemProperties bind(Map<String, String> properties) {
        return S3FileSystemProperties.bind(properties);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public FileSystem create(FileSystemProperties properties) throws IOException {
        properties.validate();
        S3ObjStorage storage = new S3ObjStorage(properties.toFileSystemKv());
        return new S3FileSystem(storage);
    }

    @Override
    public String name() {
        return "S3";
    }

    private static boolean isKnownCloudSpecificEndpoint(String endpoint) {
        return endpoint != null
                && (endpoint.contains("aliyuncs.com")
                || endpoint.contains("myqcloud.com")
                || endpoint.contains("myhuaweicloud.com"));
    }

    private static String explicitStorageType(Map<String, String> properties) {
        return FileSystemPropertyKeys.explicitProvider(properties);
    }
}
