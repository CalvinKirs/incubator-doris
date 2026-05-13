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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    void validate_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("COS_REGION", "ap-guangzhou");

        FileSystemProperties fileSystemProperties = provider.bind(props);

        Assertions.assertDoesNotThrow(fileSystemProperties::validate);
    }
}
