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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class OssFileSystemProviderTest {

    private final OssFileSystemProvider provider = new OssFileSystemProvider();

    @Test
    void supports_acceptsExplicitOssStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "OSS");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsOtherExplicitStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void bind_translatesOssPropertiesToCanonicalFileSystemProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");
        props.put("oss.region", "cn-hangzhou");
        props.put("oss.bucket", "bucket");
        props.put("oss.role_arn", "acs:ram::123456789012:role/DorisRole");

        FileSystemProperties fileSystemProperties = provider.bind(props);
        fileSystemProperties.validate();
        Map<String, String> normalized = fileSystemProperties.toFileSystemKv();

        Assertions.assertInstanceOf(OssFileSystemProperties.class, fileSystemProperties);
        Assertions.assertEquals("OSS", normalized.get("_STORAGE_TYPE_"));
        Assertions.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", normalized.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("cn-hangzhou", normalized.get("AWS_REGION"));
        Assertions.assertEquals("bucket", normalized.get("AWS_BUCKET"));
        Assertions.assertEquals("acs:ram::123456789012:role/DorisRole", normalized.get("AWS_ROLE_ARN"));
        Assertions.assertEquals("false", normalized.get("use_path_style"));
    }

    @Test
    void bind_preservesCanonicalAwsKeysWhenBothFormsPresent() {
        Map<String, String> props = new HashMap<>();
        props.put("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");
        props.put("OSS_ACCESS_KEY", "oss-ak");
        props.put("AWS_ACCESS_KEY", "aws-ak");
        props.put("OSS_SECRET_KEY", "oss-sk");
        props.put("AWS_SECRET_KEY", "aws-sk");
        props.put("AWS_REGION", "cn-hangzhou");

        Map<String, String> normalized = provider.bind(props).toFileSystemKv();

        Assertions.assertEquals("https://custom.endpoint", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("aws-ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("aws-sk", normalized.get("AWS_SECRET_KEY"));
    }

    @Test
    void bind_keepsLegacyAliasPriorityBeforeNewAliases() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");
        props.put("oss.access_key", "oss-ak");
        props.put("AWS_ACCESS_KEY", "aws-ak");
        props.put("oss.secret_key", "oss-sk");
        props.put("AWS_SECRET_KEY", "aws-sk");
        props.put("oss.region", "cn-hangzhou");
        props.put("AWS_REGION", "us-east-1");

        Map<String, String> normalized = provider.bind(props).toFileSystemKv();

        Assertions.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("oss-ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("oss-sk", normalized.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("cn-hangzhou", normalized.get("AWS_REGION"));
    }

    @Test
    void connectorPropertyAliasOrder_keepsLegacyPrefix() throws Exception {
        assertAliasPrefix("endpoint", "oss.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
                "dlf.endpoint", "dlf.catalog.endpoint", "fs.oss.endpoint");
        assertAliasPrefix("region", "oss.region", "s3.region", "AWS_REGION", "region", "REGION", "dlf.region",
                "iceberg.rest.signing-region");
        assertAliasPrefix("accessKey", "oss.access_key", "s3.access_key", "s3.access-key-id",
                "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY", "dlf.access_key", "dlf.catalog.accessKeyId",
                "fs.oss.accessKeyId");
        assertAliasPrefix("secretKey", "oss.secret_key", "s3.secret_key", "s3.secret-access-key",
                "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "dlf.secret_key", "dlf.catalog.secret_key",
                "fs.oss.accessKeySecret");
        assertAliasPrefix("token", "oss.session_token", "s3.session_token", "s3.session-token", "session_token",
                "fs.oss.securityToken", "AWS_TOKEN");
        assertAliasPrefix("bucket", "s3.bucket", "AWS_BUCKET");
        assertAliasPrefix("roleArn", "s3.role_arn", "AWS_ROLE_ARN", "glue.role_arn");
        assertAliasPrefix("externalId", "s3.external_id", "AWS_EXTERNAL_ID", "glue.external_id");
        assertAliasPrefix("pathStyle", "oss.use_path_style", "use_path_style", "s3.path-style-access");
    }

    @Test
    void validate_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        props.put("OSS_REGION", "cn-hangzhou");

        FileSystemProperties fileSystemProperties = provider.bind(props);

        Assertions.assertDoesNotThrow(fileSystemProperties::validate);
    }

    private static void assertAliasPrefix(String fieldName, String... expectedPrefix) throws Exception {
        Field field = OssFileSystemProperties.class.getDeclaredField(fieldName);
        ConnectorProperty property = field.getAnnotation(ConnectorProperty.class);
        Assertions.assertArrayEquals(expectedPrefix, Arrays.copyOf(property.names(), expectedPrefix.length));
    }
}
