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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3FileSystemPropertiesTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Test
    void bind_normalizesAliasesToCanonicalFileSystemProperties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.region", "us-west-2");
        raw.put("s3.session_token", "token");

        FileSystemProperties properties = provider.bind(raw);
        properties.validate();

        Map<String, String> kv = properties.toFileSystemKv();
        Assertions.assertEquals("ak", kv.get(S3ObjStorage.PROP_ACCESS_KEY));
        Assertions.assertEquals("sk", kv.get(S3ObjStorage.PROP_SECRET_KEY));
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com", kv.get(S3ObjStorage.PROP_ENDPOINT));
        Assertions.assertEquals("us-west-2", kv.get(S3ObjStorage.PROP_REGION));
        Assertions.assertEquals("token", kv.get(S3ObjStorage.PROP_TOKEN));
    }

    @Test
    void bind_doesNotNormalizeVendorAliasesThroughS3Properties() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com");
        raw.put("fs.cos.accessKeyId", "ak");
        raw.put("fs.cos.accessKeySecret", "sk");
        raw.put("fs.cos.region", "ap-guangzhou");
        raw.put("cos.bucket", "bucket-1234567890");

        FileSystemProperties properties = S3FileSystemProperties.bind(raw);

        Map<String, String> kv = properties.toFileSystemKv();
        Assertions.assertNull(kv.get(S3ObjStorage.PROP_ENDPOINT));
        Assertions.assertNull(kv.get(S3ObjStorage.PROP_ACCESS_KEY));
        Assertions.assertNull(kv.get(S3ObjStorage.PROP_SECRET_KEY));
        Assertions.assertNull(kv.get(S3ObjStorage.PROP_REGION));
        Assertions.assertNull(kv.get(S3ObjStorage.PROP_BUCKET));
        Assertions.assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void validate_acceptsRoleBasedConfigurationWithoutStaticAccessKey() {
        Map<String, String> raw = new HashMap<>();
        raw.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        raw.put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/snapshot-role");

        FileSystemProperties properties = provider.bind(raw);

        Assertions.assertDoesNotThrow(properties::validate);
    }

    @Test
    void validate_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> raw = new HashMap<>();
        raw.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        raw.put("AWS_REGION", "us-west-2");

        FileSystemProperties properties = provider.bind(raw);

        Assertions.assertDoesNotThrow(properties::validate);
    }

    @Test
    void validate_rejectsIncompleteStaticCredential() {
        Map<String, String> raw = new HashMap<>();
        raw.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        raw.put("AWS_REGION", "us-west-2");
        raw.put("AWS_ACCESS_KEY", "ak");

        FileSystemProperties properties = provider.bind(raw);

        Assertions.assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void validate_rejectsExternalIdWithoutRoleArn() {
        Map<String, String> raw = new HashMap<>();
        raw.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        raw.put("AWS_REGION", "us-west-2");
        raw.put("AWS_EXTERNAL_ID", "external-id");

        FileSystemProperties properties = provider.bind(raw);

        Assertions.assertThrows(IllegalArgumentException.class, properties::validate);
    }
}
