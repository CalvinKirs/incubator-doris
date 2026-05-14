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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FileSystemPluginManagerTest {

    @Test
    void createFileSystem_bindsAndValidatesProviderPropertiesBeforeCreate() throws IOException {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        BindingProvider provider = new BindingProvider();
        manager.registerProvider(provider);
        Map<String, String> properties = new HashMap<>();
        properties.put("k", "v");

        FileSystem fileSystem = manager.createFileSystem(properties);

        Assertions.assertSame(provider.fileSystem, fileSystem);
        Assertions.assertTrue(provider.bound);
        Assertions.assertTrue(provider.validated);
        Assertions.assertTrue(provider.createdFromBoundProperties);
    }

    @Test
    void resolveProviders_usesExplicitFactoryProviderWithoutCallingSupports() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        TrackingProvider s3 = new TrackingProvider("S3", false);
        TrackingProvider custom = new TrackingProvider("S3-CUSTOM", false);
        manager.registerProvider(s3);
        manager.registerProvider(custom);
        Map<String, String> properties = new HashMap<>();
        properties.put(FileSystemPropertyKeys.FS_PROVIDER, "S3-CUSTOM");

        List<FileSystemProvider> providers = manager.resolveProviders(properties);

        Assertions.assertEquals(1, providers.size());
        Assertions.assertSame(custom, providers.get(0));
        Assertions.assertFalse(s3.supportsCalled);
        Assertions.assertFalse(custom.supportsCalled);
    }

    @Test
    void resolveProviders_withoutFactoryProviderFallsBackToSupports() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        TrackingProvider unsupported = new TrackingProvider("S3", false);
        TrackingProvider supported = new TrackingProvider("OSS", true);
        manager.registerProvider(unsupported);
        manager.registerProvider(supported);
        Map<String, String> properties = new HashMap<>();

        List<FileSystemProvider> providers = manager.resolveProviders(properties);

        Assertions.assertEquals(1, providers.size());
        Assertions.assertSame(supported, providers.get(0));
        Assertions.assertTrue(unsupported.supportsCalled);
        Assertions.assertTrue(supported.supportsCalled);
    }

    private static class BindingProvider implements FileSystemProvider {
        private final FileSystem fileSystem = Mockito.mock(FileSystem.class);
        private boolean bound;
        private boolean validated;
        private boolean createdFromBoundProperties;

        @Override
        public boolean supports(Map<String, String> properties) {
            return true;
        }

        @Override
        public FileSystemProperties bind(Map<String, String> properties) {
            bound = true;
            return new FileSystemProperties() {
                @Override
                public void validate() {
                    validated = true;
                }

                @Override
                public Map<String, String> toFileSystemKv() {
                    return properties;
                }
            };
        }

        @Override
        public FileSystem create(Map<String, String> properties) {
            throw new AssertionError("raw create should not be used after binding provider properties");
        }

        @Override
        public FileSystem create(FileSystemProperties properties) {
            createdFromBoundProperties = true;
            return fileSystem;
        }
    }

    private static class TrackingProvider implements FileSystemProvider {
        private final String name;
        private final boolean supported;
        private boolean supportsCalled;

        private TrackingProvider(String name, boolean supported) {
            this.name = name;
            this.supported = supported;
        }

        @Override
        public boolean supports(Map<String, String> properties) {
            supportsCalled = true;
            return supported;
        }

        @Override
        public FileSystem create(Map<String, String> properties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String name() {
            return name;
        }
    }
}
