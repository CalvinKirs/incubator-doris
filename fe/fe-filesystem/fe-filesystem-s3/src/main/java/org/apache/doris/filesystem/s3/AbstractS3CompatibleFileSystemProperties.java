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

import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.filesystem.FileSystemPropertyKeys;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractS3CompatibleFileSystemProperties implements FileSystemProperties {

    private final String storageType;
    private final Map<String, String> properties;

    protected AbstractS3CompatibleFileSystemProperties(Map<String, String> rawProperties, String storageType) {
        this.storageType = storageType;
        ConnectorPropertiesUtils.bindConnectorProperties(this, rawProperties);
        this.properties = Collections.unmodifiableMap(buildCanonicalProperties(rawProperties));
    }

    protected abstract String endpoint();

    protected abstract String region();

    protected abstract String accessKey();

    protected abstract String secretKey();

    protected abstract String token();

    protected abstract String bucket();

    protected abstract String roleArn();

    protected abstract String externalId();

    protected abstract String pathStyle();

    @Override
    public void validate() {
        if (hasPartialStaticCredential()) {
            throw new IllegalArgumentException(storageName() + " requires both access key and secret key.");
        }
        if (hasText(properties.get(S3ObjStorage.PROP_EXTERNAL_ID)) && !hasRoleCredential()) {
            throw new IllegalArgumentException(storageName() + " external id must be used with role arn.");
        }
        if (!hasLocation()) {
            throw new IllegalArgumentException(storageName() + " requires endpoint or region.");
        }
    }

    public boolean hasCredential() {
        return hasStaticCredential() || hasRoleCredential();
    }

    public boolean hasLocation() {
        return hasText(properties.get(S3ObjStorage.PROP_ENDPOINT))
                || hasText(properties.get(S3ObjStorage.PROP_REGION));
    }

    @Override
    public Map<String, String> toFileSystemKv() {
        return properties;
    }

    private Map<String, String> buildCanonicalProperties(Map<String, String> rawProperties) {
        Map<String, String> result = new HashMap<>(rawProperties);
        putIfPresent(result, S3ObjStorage.PROP_ENDPOINT, endpoint());
        putIfPresent(result, S3ObjStorage.PROP_REGION, region());
        putIfPresent(result, S3ObjStorage.PROP_ACCESS_KEY, accessKey());
        putIfPresent(result, S3ObjStorage.PROP_SECRET_KEY, secretKey());
        putIfPresent(result, S3ObjStorage.PROP_TOKEN, token());
        putIfPresent(result, S3ObjStorage.PROP_BUCKET, bucket());
        putIfPresent(result, S3ObjStorage.PROP_ROLE_ARN, roleArn());
        putIfPresent(result, S3ObjStorage.PROP_EXTERNAL_ID, externalId());
        if (hasText(pathStyle())) {
            result.put(S3ObjStorage.PROP_PATH_STYLE, pathStyle());
        } else if (hasText(storageType)) {
            result.putIfAbsent(S3ObjStorage.PROP_PATH_STYLE, "false");
        }
        if (hasText(storageType)) {
            result.put(FileSystemPropertyKeys.STORAGE_TYPE, storageType);
        }
        return result;
    }

    private String storageName() {
        return hasText(storageType) ? storageType : "S3";
    }

    private static void putIfPresent(Map<String, String> properties, String key, String value) {
        if (hasText(value)) {
            properties.put(key, value);
        }
    }

    protected static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private boolean hasStaticCredential() {
        return hasText(properties.get(S3ObjStorage.PROP_ACCESS_KEY))
                && hasText(properties.get(S3ObjStorage.PROP_SECRET_KEY));
    }

    private boolean hasRoleCredential() {
        return hasText(properties.get(S3ObjStorage.PROP_ROLE_ARN));
    }

    private boolean hasPartialStaticCredential() {
        return hasText(properties.get(S3ObjStorage.PROP_ACCESS_KEY))
                ^ hasText(properties.get(S3ObjStorage.PROP_SECRET_KEY));
    }
}
