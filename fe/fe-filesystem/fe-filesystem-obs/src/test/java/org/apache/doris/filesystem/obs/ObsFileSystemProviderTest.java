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

package org.apache.doris.filesystem.obs;

import org.apache.doris.filesystem.FileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ObsFileSystemProviderTest {

    private final ObsFileSystemProvider provider = new ObsFileSystemProvider();

    @Test
    void supports_acceptsExplicitObsStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "OBS");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsOtherExplicitStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void bind_translatesObsPropertiesToCanonicalFileSystemProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("obs.access_key", "ak");
        props.put("obs.secret_key", "sk");
        props.put("obs.region", "cn-north-4");
        props.put("obs.bucket", "bucket");
        props.put("OBS_AGENCY_NAME", "agency");
        props.put("OBS_DOMAIN_NAME", "domain");

        FileSystemProperties fileSystemProperties = provider.bind(props);
        fileSystemProperties.validate();
        Map<String, String> normalized = fileSystemProperties.toFileSystemKv();

        Assertions.assertInstanceOf(ObsFileSystemProperties.class, fileSystemProperties);
        Assertions.assertEquals("OBS", normalized.get("_STORAGE_TYPE_"));
        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", normalized.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("cn-north-4", normalized.get("AWS_REGION"));
        Assertions.assertEquals("bucket", normalized.get("AWS_BUCKET"));
        Assertions.assertEquals("agency", normalized.get("OBS_AGENCY_NAME"));
        Assertions.assertEquals("domain", normalized.get("OBS_DOMAIN_NAME"));
        Assertions.assertEquals("false", normalized.get("use_path_style"));
    }

    @Test
    void bind_preservesCanonicalAwsKeysWhenBothFormsPresent() {
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");
        props.put("OBS_ACCESS_KEY", "obs-ak");
        props.put("AWS_ACCESS_KEY", "aws-ak");
        props.put("OBS_SECRET_KEY", "obs-sk");
        props.put("AWS_SECRET_KEY", "aws-sk");
        props.put("AWS_REGION", "cn-north-4");

        Map<String, String> normalized = provider.bind(props).toFileSystemKv();

        Assertions.assertEquals("https://custom.endpoint", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("aws-ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("aws-sk", normalized.get("AWS_SECRET_KEY"));
    }

    @Test
    void validate_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("OBS_REGION", "cn-north-4");

        FileSystemProperties fileSystemProperties = provider.bind(props);

        Assertions.assertDoesNotThrow(fileSystemProperties::validate);
    }
}
