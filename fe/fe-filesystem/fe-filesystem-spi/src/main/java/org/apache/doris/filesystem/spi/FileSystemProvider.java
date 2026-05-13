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

package org.apache.doris.filesystem.spi;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SPI interface for filesystem provider discovery via Java ServiceLoader.
 *
 * <p>Extends {@link PluginFactory} to allow {@link
 * org.apache.doris.extension.loader.DirectoryPluginRuntimeManager} to load filesystem
 * providers from plugin directories at runtime, decoupling fe-core from concrete
 * storage backend implementations at the Maven dependency level.
 *
 * <p>Implementations must:
 * 1. Have a public no-arg constructor.
 * 2. Register in META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 * 3. Have NO dependency on fe-core, fe-common, or fe-catalog.
 */
public interface FileSystemProvider extends PluginFactory {

    /**
     * Returns true if this provider can handle the given properties.
     * Must be cheap (no network calls) and deterministic.
     *
     * @param properties key-value storage configuration
     * @return true if this provider supports the configuration
     */
    boolean supports(Map<String, String> properties);

    /**
     * Binds raw key-value properties into provider-owned filesystem properties.
     *
     * @param properties raw key-value storage configuration
     * @return provider-bound filesystem properties
     */
    default FileSystemProperties bind(Map<String, String> properties) {
        return FileSystemProperties.of(properties);
    }

    /**
     * StorageProperties-compatible storage type exposed by this provider.
     *
     * <p>This is intentionally separated from {@link #name()}: a custom provider may expose
     * a unique factory identity such as "S3-CUSTOM" while still reusing the S3 storage
     * property model by returning "S3" here.
     */
    default String storageType() {
        return name();
    }

    /**
     * Converts provider-bound filesystem properties to the key-value form consumed by
     * StorageProperties.
     *
     * <p>Provider implementations own this conversion. FE core must not infer a concrete
     * StorageProperties implementation from the provider name.
     */
    default Map<String, String> toStoragePropertiesKv(
            Map<String, String> rawProperties, FileSystemProperties fileSystemProperties) {
        Map<String, String> result = new HashMap<>(rawProperties);
        result.putAll(fileSystemProperties.toFileSystemKv());
        result.put(FileSystemPropertyKeys.STORAGE_TYPE, storageType());
        return result;
    }

    /**
     * Creates a FileSystem instance from the given properties.
     * Called only after {@link #supports(Map)} returns true.
     *
     * @param properties key-value storage configuration
     * @return a ready-to-use FileSystem
     * @throws IOException if the filesystem cannot be initialized
     */
    FileSystem create(Map<String, String> properties) throws IOException;

    /**
     * Creates a FileSystem instance from already bound provider properties.
     */
    default FileSystem create(FileSystemProperties properties) throws IOException {
        properties.validate();
        return create(properties.toFileSystemKv());
    }

    /**
     * Human-readable name for logging/diagnostics (e.g., "S3", "HDFS", "Azure").
     */
    @Override
    default String name() {
        return getClass().getSimpleName().replace("FileSystemProvider", "");
    }

    /**
     * Not used by DirectoryPluginRuntimeManager (it only discovers factories via ServiceLoader).
     * Provided to satisfy {@link PluginFactory} contract.
     */
    @Override
    default Plugin create() {
        throw new UnsupportedOperationException(
                "FileSystemProvider does not support no-arg create(). Use create(Map) instead.");
    }
}
