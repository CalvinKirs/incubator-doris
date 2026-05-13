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
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
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
}
