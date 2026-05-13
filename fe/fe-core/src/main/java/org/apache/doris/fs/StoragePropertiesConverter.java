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
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts legacy {@link StorageProperties} objects to the {@code Map<String, String>} format
 * expected by {@link FileSystemProvider#supports} and {@link FileSystemProvider#create}.
 *
 * <p>This class is a compatibility adapter for callers that still hold {@link StorageProperties}.
 * Concrete filesystem parameters are owned by {@link FileSystemProvider#bind(Map)} and
 * {@link FileSystemProperties#toFileSystemKv()}, not by this adapter.
 */
public final class StoragePropertiesConverter {

    private StoragePropertiesConverter() {}

    /**
     * Converts a {@link StorageProperties} instance to provider-bound filesystem properties.
     */
    public static Map<String, String> toMap(StorageProperties props) {
        FileSystemProperties boundProperties = props.getFileSystemProperties();
        if (boundProperties != null) {
            return withProviderKeys(boundProperties.toFileSystemKv(), props.getType().name(),
                    props.getFileSystemProviderName());
        }

        Map<String, String> rawProperties = new HashMap<>(props.getOrigProps());
        rawProperties.putIfAbsent(FileSystemPropertyKeys.STORAGE_TYPE, props.getType().name());
        String providerName = props.getFileSystemProviderName();
        if (StringUtils.isNotBlank(providerName) && !props.getType().name().equalsIgnoreCase(providerName)) {
            rawProperties.putIfAbsent(FileSystemPropertyKeys.FS_PROVIDER, providerName);
        }

        List<FileSystemProvider> providers = FileSystemFactory.resolveProviders(rawProperties);
        if (!providers.isEmpty()) {
            FileSystemProvider provider = providers.get(0);
            FileSystemProperties fileSystemProperties = provider.bind(rawProperties);
            fileSystemProperties.validate();
            return withProviderKeys(fileSystemProperties.toFileSystemKv(), provider.storageType(), provider.name());
        }

        Map<String, String> legacyProperties = new HashMap<>(props.getBackendConfigProperties());
        legacyProperties.putIfAbsent(FileSystemPropertyKeys.STORAGE_TYPE, props.getType().name());
        return legacyProperties;
    }

    private static Map<String, String> withProviderKeys(
            Map<String, String> fileSystemProperties, String storageType, String providerName) {
        Map<String, String> result = new HashMap<>(fileSystemProperties);
        result.putIfAbsent(FileSystemPropertyKeys.STORAGE_TYPE, storageType);
        if (StringUtils.isNotBlank(providerName) && !storageType.equalsIgnoreCase(providerName)) {
            result.putIfAbsent(FileSystemPropertyKeys.FS_PROVIDER, providerName);
        }
        return result;
    }
}
