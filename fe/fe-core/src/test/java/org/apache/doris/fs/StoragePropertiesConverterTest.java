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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class StoragePropertiesConverterTest {

    @Test
    void toMap_keepsConcreteObjectStorageTypeForProviderSelection() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new CanonicalCosProvider());
        FileSystemFactory.initPluginManager(manager);
        try {
            Map<String, String> props = new HashMap<>();
            props.put("fs.cos.support", "true");
            props.put("cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com");
            props.put("cos.region", "ap-guangzhou");
            props.put("cos.access_key", "ak");
            props.put("cos.secret_key", "sk");

            StorageProperties storageProperties = StorageProperties.createPrimary(props);
            Map<String, String> fileSystemProperties = StoragePropertiesConverter.toMap(storageProperties);

            Assertions.assertEquals("COS", fileSystemProperties.get(FileSystemPropertyKeys.STORAGE_TYPE));
            Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com",
                    fileSystemProperties.get("AWS_ENDPOINT"));
        } finally {
            FileSystemFactory.initPluginManager(null);
        }
    }

    @Test
    void toMap_usesProviderBoundPropertiesWithoutRebuildingObjectStorageKeys() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new CanonicalS3Provider());
        FileSystemFactory.initPluginManager(manager);
        try {
            Map<String, String> props = new HashMap<>();
            props.put(StorageProperties.FS_PROVIDER, "S3-CUSTOM");
            props.put("custom.endpoint", "https://custom.example.com");

            StorageProperties storageProperties = StorageProperties.createPrimary(props);
            Map<String, String> fileSystemProperties = StoragePropertiesConverter.toMap(storageProperties);

            Assertions.assertEquals("https://custom.example.com", fileSystemProperties.get("s3.endpoint"));
            Assertions.assertEquals("S3-CUSTOM", fileSystemProperties.get(FileSystemPropertyKeys.FS_PROVIDER));
            Assertions.assertEquals("S3", fileSystemProperties.get(FileSystemPropertyKeys.STORAGE_TYPE));
            Assertions.assertFalse(fileSystemProperties.containsKey("AWS_ACCESS_KEY"));
        } finally {
            FileSystemFactory.initPluginManager(null);
        }
    }

    private static class CanonicalCosProvider implements FileSystemProvider {
        @Override
        public boolean supports(Map<String, String> properties) {
            return "true".equalsIgnoreCase(properties.get("fs.cos.support"))
                    || "COS".equalsIgnoreCase(properties.get(FileSystemPropertyKeys.STORAGE_TYPE));
        }

        @Override
        public FileSystemProperties bind(Map<String, String> properties) {
            Map<String, String> kv = new HashMap<>(properties);
            kv.put(FileSystemPropertyKeys.STORAGE_TYPE, "COS");
            kv.put("AWS_ENDPOINT", properties.get("cos.endpoint"));
            kv.put("AWS_REGION", properties.get("cos.region"));
            kv.put("AWS_ACCESS_KEY", properties.get("cos.access_key"));
            kv.put("AWS_SECRET_KEY", properties.get("cos.secret_key"));
            return FileSystemProperties.of(kv);
        }

        @Override
        public FileSystem create(Map<String, String> properties) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String name() {
            return "COS";
        }
    }

    private static class CanonicalS3Provider implements FileSystemProvider {
        @Override
        public boolean supports(Map<String, String> properties) {
            return "S3-CUSTOM".equalsIgnoreCase(properties.get(StorageProperties.FS_PROVIDER));
        }

        @Override
        public FileSystemProperties bind(Map<String, String> properties) {
            Map<String, String> kv = new HashMap<>();
            kv.put("s3.endpoint", properties.get("custom.endpoint"));
            kv.put("s3.region", "us-east-1");
            return FileSystemProperties.of(kv);
        }

        @Override
        public FileSystem create(Map<String, String> properties) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String name() {
            return "S3-CUSTOM";
        }

        @Override
        public String storageType() {
            return "S3";
        }
    }
}
