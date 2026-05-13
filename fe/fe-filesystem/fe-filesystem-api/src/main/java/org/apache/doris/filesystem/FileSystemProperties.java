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

package org.apache.doris.filesystem;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provider-bound filesystem properties.
 *
 * <p>A {@link org.apache.doris.filesystem.spi.FileSystemProvider} owns how raw key-value
 * properties are bound, normalized, validated, and finally converted to the concrete
 * key-value form used by its FileSystem implementation.
 */
public interface FileSystemProperties {

    /**
     * Validates provider-specific required fields and semantic constraints.
     */
    default void validate() {
    }

    /**
     * Returns the normalized key-value properties used to create the concrete FileSystem.
     */
    Map<String, String> toFileSystemKv();

    static FileSystemProperties of(Map<String, String> properties) {
        return new BasicFileSystemProperties(properties);
    }

    final class BasicFileSystemProperties implements FileSystemProperties {
        private final Map<String, String> properties;

        private BasicFileSystemProperties(Map<String, String> properties) {
            this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        }

        @Override
        public Map<String, String> toFileSystemKv() {
            return properties;
        }
    }
}
