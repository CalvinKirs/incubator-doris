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

package org.apache.doris.common.util;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.storage.RemoteBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SensitiveDataMaskUtilsTest {

    @Test
    void testSanitizeObjectStoreInfoMasksSecrets() {
        ObjectStoreInfoPB info = ObjectStoreInfoPB.newBuilder()
                .setProvider(Provider.COS)
                .setBucket("tmp-bucket")
                .setEndpoint("cos.ap-beijing.myqcloud.com")
                .setRegion("ap-beijing")
                .setPrefix("tmp_prefix")
                .setAk("tmp_ak")
                .setSk("tmp_sk")
                .build();

        String sanitized = SensitiveDataMaskUtils.sanitizeObjectStoreInfoPB(info);

        Assertions.assertTrue(sanitized.contains("provider=COS"));
        Assertions.assertTrue(sanitized.contains("bucket=tmp-bucket"));
        Assertions.assertTrue(sanitized.contains("endpoint=cos.ap-beijing.myqcloud.com"));
        Assertions.assertTrue(sanitized.contains("region=ap-beijing"));
        Assertions.assertTrue(sanitized.contains("prefix=tmp_prefix"));
        Assertions.assertTrue(sanitized.contains("ak=******"));
        Assertions.assertTrue(sanitized.contains("sk=******"));
        Assertions.assertFalse(sanitized.contains("tmp_ak"));
        Assertions.assertFalse(sanitized.contains("tmp_sk"));
    }

    @Test
    void testMaskTokenReplacesRawValue() {
        Assertions.assertEquals("******", SensitiveDataMaskUtils.maskToken("flight-token"));
    }

    @Test
    void testObjectInfoToStringMasksSecrets() {
        RemoteBase.ObjectInfo info = new RemoteBase.ObjectInfo(
                Provider.S3, "test_ak", "test_sk", "test_bucket", "test_endpoint", "test_region", "test_prefix");

        String result = info.toString();

        Assertions.assertTrue(result.contains("provider=S3"));
        Assertions.assertTrue(result.contains("bucket='test_bucket'"));
        Assertions.assertTrue(result.contains("endpoint='test_endpoint'"));
        Assertions.assertTrue(result.contains("ak='******'"));
        Assertions.assertTrue(result.contains("sk='******'"));
        Assertions.assertFalse(result.contains("test_ak"));
        Assertions.assertFalse(result.contains("test_sk"));
    }
}
