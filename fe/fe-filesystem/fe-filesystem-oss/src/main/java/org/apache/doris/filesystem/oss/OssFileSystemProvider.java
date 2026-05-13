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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for Alibaba Cloud OSS.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by an endpoint containing {@code aliyuncs.com}. Binds OSS-specific
 * property keys through the OSS property model and delegates core I/O to {@link S3FileSystem},
 * while {@link OssObjStorage} overrides cloud-specific operations (presigned URL, STS)
 * using the Alibaba Cloud native SDK.
 */
public class OssFileSystemProvider implements FileSystemProvider {

    private static final String STORAGE_TYPE = "OSS";
    private static final String KEY_STORAGE_TYPE = "_STORAGE_TYPE_";
    private static final String KEY_FS_PROVIDER = "fs.provider";
    private static final String KEY_LEGACY_PROVIDER = "provider";

    @Override
    public boolean supports(Map<String, String> properties) {
        String storageType = explicitStorageType(properties);
        if (storageType != null) {
            return STORAGE_TYPE.equalsIgnoreCase(storageType);
        }
        if ("true".equalsIgnoreCase(properties.get("fs.oss.support"))) {
            return true;
        }
        String endpoint = properties.get("OSS_ENDPOINT");
        if (endpoint == null) {
            endpoint = properties.get("AWS_ENDPOINT");
        }
        return endpoint != null && endpoint.contains("aliyuncs.com");
    }

    @Override
    public FileSystemProperties bind(Map<String, String> properties) {
        return OssFileSystemProperties.bind(properties);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public FileSystem create(FileSystemProperties properties) throws IOException {
        properties.validate();
        return new S3FileSystem(new OssObjStorage(properties.toFileSystemKv()));
    }

    @Override
    public String name() {
        return STORAGE_TYPE;
    }

    private static String explicitStorageType(Map<String, String> properties) {
        String storageType = properties.get(KEY_STORAGE_TYPE);
        if (storageType != null) {
            return storageType;
        }
        storageType = properties.get(KEY_FS_PROVIDER);
        if (storageType != null) {
            return storageType;
        }
        return properties.get(KEY_LEGACY_PROVIDER);
    }
}
