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

import java.util.Map;

public final class FileSystemPropertyKeys {

    public static final String STORAGE_TYPE = "_STORAGE_TYPE_";
    public static final String FS_PROVIDER = "fs.provider";
    public static final String LEGACY_PROVIDER = "provider";

    private FileSystemPropertyKeys() {
    }

    public static String explicitStorageType(Map<String, String> properties) {
        return properties.get(STORAGE_TYPE);
    }

    public static String explicitFactoryProvider(Map<String, String> properties) {
        String provider = properties.get(FS_PROVIDER);
        if (hasText(provider)) {
            return provider;
        }
        return properties.get(LEGACY_PROVIDER);
    }

    public static boolean hasAnyExplicitFileSystemSupport(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("fs.") && key.endsWith(".support")
                    && "true".equalsIgnoreCase(entry.getValue())) {
                return true;
            }
        }
        return "true".equalsIgnoreCase(properties.get("oss.hdfs.enabled"));
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
