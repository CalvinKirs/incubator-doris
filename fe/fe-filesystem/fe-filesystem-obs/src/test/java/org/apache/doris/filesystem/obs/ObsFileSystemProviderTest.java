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
import org.apache.doris.foundation.property.ConnectorProperty;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
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
    void supports_usesBoundEndpointAliasPriority() {
        Map<String, String> props = new HashMap<>();
        props.put("obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");

        Assertions.assertTrue(provider.supports(props));
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
    void bind_keepsLegacyAliasPriorityBeforeNewAliases() {
        Map<String, String> props = new HashMap<>();
        props.put("obs.endpoint", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");
        props.put("obs.access_key", "obs-ak");
        props.put("AWS_ACCESS_KEY", "aws-ak");
        props.put("obs.secret_key", "obs-sk");
        props.put("AWS_SECRET_KEY", "aws-sk");
        props.put("obs.region", "cn-north-4");
        props.put("AWS_REGION", "us-east-1");

        Map<String, String> normalized = provider.bind(props).toFileSystemKv();

        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("obs-ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("obs-sk", normalized.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("cn-north-4", normalized.get("AWS_REGION"));
    }

    @Test
    void connectorPropertyAliasOrder_keepsLegacyPrefix() throws Exception {
        assertAliasPrefix("endpoint", "obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT");
        assertAliasPrefix("region", "obs.region", "s3.region", "AWS_REGION", "region", "REGION");
        assertAliasPrefix("accessKey", "obs.access_key", "s3.access_key", "s3.access-key-id",
                "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY");
        assertAliasPrefix("secretKey", "obs.secret_key", "s3.secret_key", "s3.secret-access-key",
                "AWS_SECRET_KEY", "secret_key", "SECRET_KEY");
        assertAliasPrefix("token", "obs.session_token", "s3.session_token", "s3.session-token", "session_token");
        assertAliasPrefix("bucket", "s3.bucket", "AWS_BUCKET");
        assertAliasPrefix("roleArn", "s3.role_arn", "AWS_ROLE_ARN", "glue.role_arn");
        assertAliasPrefix("externalId", "s3.external_id", "AWS_EXTERNAL_ID", "glue.external_id");
        assertAliasPrefix("pathStyle", "obs.use_path_style", "use_path_style", "s3.path-style-access");
    }

    @Test
    void validate_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("OBS_REGION", "cn-north-4");

        FileSystemProperties fileSystemProperties = provider.bind(props);

        Assertions.assertDoesNotThrow(fileSystemProperties::validate);
    }

    private static void assertAliasPrefix(String fieldName, String... expectedPrefix) throws Exception {
        Field field = ObsFileSystemProperties.class.getDeclaredField(fieldName);
        ConnectorProperty property = field.getAnnotation(ConnectorProperty.class);
        Assertions.assertArrayEquals(expectedPrefix, Arrays.copyOf(property.names(), expectedPrefix.length));
    }
}
