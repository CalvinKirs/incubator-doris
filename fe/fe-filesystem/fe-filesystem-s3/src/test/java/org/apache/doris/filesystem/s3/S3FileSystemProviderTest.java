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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3FileSystemProviderTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Test
    void supports_acceptsRoleBasedS3Configuration() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");
        props.put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/snapshot-role");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsDefaultCredentialChainWithoutStaticCredentialOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsCloudSpecificStorageTypeEvenWithS3CompatibleKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "COS");
        props.put("AWS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("AWS_REGION", "ap-guangzhou");
        props.put("AWS_ACCESS_KEY", "ak");
        props.put("AWS_SECRET_KEY", "sk");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_rejectsKnownCloudSpecificEndpointWithoutStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com");
        props.put("AWS_REGION", "ap-guangzhou");
        props.put("AWS_ACCESS_KEY", "ak");
        props.put("AWS_SECRET_KEY", "sk");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_acceptsExplicitS3CompatibleStorageTypes() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "MINIO");
        props.put("AWS_ENDPOINT", "http://minio.local:9000");
        props.put("AWS_REGION", "us-east-1");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void toStoragePropertiesKv_preservesS3CompatibleStorageType() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.minio.support", "true");
        props.put("minio.endpoint", "http://minio.local:9000");
        props.put("minio.region", "us-east-1");

        FileSystemProperties boundProperties = provider.bind(props);
        Map<String, String> storageProperties = provider.toStoragePropertiesKv(props, boundProperties);

        Assertions.assertEquals("MINIO", storageProperties.get(FileSystemPropertyKeys.STORAGE_TYPE));
        Assertions.assertEquals("http://minio.local:9000", storageProperties.get(S3ObjStorage.PROP_ENDPOINT));
    }
}
