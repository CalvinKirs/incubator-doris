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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.FileSystemProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class CosFileSystemProviderTest {

    private final CosFileSystemProvider provider = new CosFileSystemProvider();

    @Test
    void supports_acceptsExplicitCosStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "COS");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsOtherExplicitStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_usesBoundEndpointAliasPriority() {
        Map<String, String> props = new HashMap<>();
        props.put("cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void bind_translatesCosPropertiesToCanonicalFileSystemProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("cos.access_key", "ak");
        props.put("cos.secret_key", "sk");
        props.put("cos.region", "ap-guangzhou");
        props.put("cos.bucket", "bucket-1234567890");
        props.put("cos.role_arn", "qcs::cam::uin/100000:roleName/DorisRole");

        FileSystemProperties fileSystemProperties = provider.bind(props);
        fileSystemProperties.validate();
        Map<String, String> normalized = fileSystemProperties.toFileSystemKv();

        Assertions.assertInstanceOf(CosFileSystemProperties.class, fileSystemProperties);
        Assertions.assertEquals("COS", normalized.get("_STORAGE_TYPE_"));
        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", normalized.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("ap-guangzhou", normalized.get("AWS_REGION"));
        Assertions.assertEquals("bucket-1234567890", normalized.get("AWS_BUCKET"));
        Assertions.assertEquals("qcs::cam::uin/100000:roleName/DorisRole", normalized.get("AWS_ROLE_ARN"));
        Assertions.assertEquals("false", normalized.get("use_path_style"));
    }

    @Test
    void bind_preservesCanonicalAwsKeysWhenBothFormsPresent() {
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");
        props.put("COS_ACCESS_KEY", "cos-ak");
        props.put("AWS_ACCESS_KEY", "aws-ak");
        props.put("COS_SECRET_KEY", "cos-sk");
        props.put("AWS_SECRET_KEY", "aws-sk");
        props.put("AWS_REGION", "ap-guangzhou");

        Map<String, String> normalized = provider.bind(props).toFileSystemKv();

        Assertions.assertEquals("https://custom.endpoint", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("aws-ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("aws-sk", normalized.get("AWS_SECRET_KEY"));
    }

    @Test
    void bind_keepsLegacyAliasPriorityBeforeNewAliases() {
        Map<String, String> props = new HashMap<>();
        props.put("cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("AWS_ENDPOINT", "https://custom.endpoint");
        props.put("cos.access_key", "cos-ak");
        props.put("AWS_ACCESS_KEY", "aws-ak");
        props.put("cos.secret_key", "cos-sk");
        props.put("AWS_SECRET_KEY", "aws-sk");
        props.put("cos.region", "ap-guangzhou");
        props.put("AWS_REGION", "us-east-1");

        Map<String, String> normalized = provider.bind(props).toFileSystemKv();

        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com", normalized.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cos-ak", normalized.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("cos-sk", normalized.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("ap-guangzhou", normalized.get("AWS_REGION"));
    }

    @Test
    void connectorPropertyAliasOrder_keepsLegacyPrefix() throws Exception {
        assertAliasPrefix("endpoint", "cos.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT");
        assertAliasPrefix("region", "cos.region", "s3.region", "AWS_REGION", "region", "REGION");
        assertAliasPrefix("accessKey", "cos.access_key", "s3.access_key", "s3.access-key-id",
                "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY");
        assertAliasPrefix("secretKey", "cos.secret_key", "s3.secret_key", "s3.secret-access-key",
                "AWS_SECRET_KEY", "secret_key", "SECRET_KEY");
        assertAliasPrefix("token", "cos.session_token", "s3.session_token", "s3.session-token", "session_token");
        assertAliasPrefix("bucket", "s3.bucket", "AWS_BUCKET");
        assertAliasPrefix("roleArn", "s3.role_arn", "AWS_ROLE_ARN", "glue.role_arn");
        assertAliasPrefix("externalId", "s3.external_id", "AWS_EXTERNAL_ID", "glue.external_id");
        assertAliasPrefix("pathStyle", "cos.use_path_style", "use_path_style", "s3.path-style-access");
    }

    @Test
    void validate_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("COS_REGION", "ap-guangzhou");

        FileSystemProperties fileSystemProperties = provider.bind(props);

        Assertions.assertDoesNotThrow(fileSystemProperties::validate);
    }

    private static void assertAliasPrefix(String fieldName, String... expectedPrefix) throws Exception {
        Field field = CosFileSystemProperties.class.getDeclaredField(fieldName);
        ConnectorProperty property = field.getAnnotation(ConnectorProperty.class);
        Assertions.assertArrayEquals(expectedPrefix, Arrays.copyOf(property.names(), expectedPrefix.length));
    }
}
