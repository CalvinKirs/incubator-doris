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

import org.apache.doris.filesystem.s3.S3ObjStorage;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link OssObjStorage}.
 *
 * <p>Cloud calls are replaced with mocks; no real OSS credentials are required.
 */
class OssObjStorageTest {

    @Test
    void ossStorageKeepsOssPropertiesWithoutAwsTranslation() {
        Map<String, String> ossProps = new HashMap<>();
        ossProps.put("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        ossProps.put("OSS_ACCESS_KEY", "myAK");
        ossProps.put("OSS_SECRET_KEY", "mySK");
        ossProps.put("OSS_BUCKET", "my-bucket");
        ossProps.put("OSS_REGION", "cn-hangzhou");
        ossProps.put("OSS_ROLE_ARN", "acs:ram::12345:role/DorisRole");

        OssObjStorage storage = new OssObjStorage(ossProps);

        Assertions.assertFalse(S3ObjStorage.class.isAssignableFrom(storage.getClass()));
        Assertions.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", storage.getProperties().get("OSS_ENDPOINT"));
        Assertions.assertFalse(storage.getProperties().containsKey("AWS_ENDPOINT"));
        Assertions.assertFalse(storage.getProperties().containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    void ossStorageDoesNotAcceptAwsFallbackKeys() {
        Map<String, String> ossProps = new HashMap<>();
        ossProps.put("AWS_ENDPOINT", "https://custom.endpoint");
        ossProps.put("AWS_ACCESS_KEY", "awsAK");

        OssObjStorage storage = new OssObjStorage(ossProps);

        Assertions.assertThrows(IOException.class,
                () -> storage.getPresignedUrl("stage/f1"));
    }

    // ------------------------------------------------------------------
    // getPresignedUrl() with mocked OSS client
    // ------------------------------------------------------------------

    @Test
    void getPresignedUrl_returnsSignedUrlFromOssClient() throws Exception {
        String expectedUrl = "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/stage/f1?sig=abc";
        OSS mockOss = Mockito.mock(OSS.class);
        Mockito.when(mockOss.generatePresignedUrl(Mockito.any(GeneratePresignedUrlRequest.class)))
                .thenReturn(new URL(expectedUrl));

        Map<String, String> props = buildBasicProps();
        OssObjStorage storage = new TestableOssObjStorage(props, mockOss);

        String result = storage.getPresignedUrl("stage/f1");

        Assertions.assertEquals(expectedUrl, result);
        ArgumentCaptor<GeneratePresignedUrlRequest> captor =
                ArgumentCaptor.forClass(GeneratePresignedUrlRequest.class);
        Mockito.verify(mockOss).generatePresignedUrl(captor.capture());
        Assertions.assertEquals("my-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f1", captor.getValue().getKey());
    }

    @Test
    void getPresignedUrl_missingBucketThrowsIOException() {
        OSS mockOss = Mockito.mock(OSS.class);
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://oss.aliyuncs.com");
        props.put("OSS_ACCESS_KEY", "ak");
        props.put("OSS_SECRET_KEY", "sk");
        // no bucket

        OssObjStorage storage = new TestableOssObjStorage(props, mockOss);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when bucket is missing");
    }

    @Test
    void getPresignedUrl_expiryInFuture() throws Exception {
        long beforeMs = System.currentTimeMillis();
        OSS mockOss = Mockito.mock(OSS.class);
        Mockito.when(mockOss.generatePresignedUrl(Mockito.any(GeneratePresignedUrlRequest.class)))
                .thenAnswer(inv -> {
                    GeneratePresignedUrlRequest req = inv.getArgument(0);
                    Assertions.assertTrue(req.getExpiration().getTime() > beforeMs,
                            "Expiry must be in the future");
                    long diffSec = (req.getExpiration().getTime() - beforeMs) / 1000;
                    Assertions.assertTrue(diffSec >= 3500 && diffSec <= 3700,
                            "Expiry diff should be ~3600s, was " + diffSec);
                    return new URL("https://bucket.oss.aliyuncs.com/key?sig=x");
                });

        OssObjStorage storage = new TestableOssObjStorage(buildBasicProps(), mockOss);
        storage.getPresignedUrl("key");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildBasicProps() {
        Map<String, String> props = new HashMap<>();
        props.put("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        props.put("OSS_ACCESS_KEY", "testAK");
        props.put("OSS_SECRET_KEY", "testSK");
        props.put("OSS_BUCKET", "my-bucket");
        props.put("OSS_REGION", "cn-hangzhou");
        return props;
    }

    /** Subclass that injects a mock OSS client, bypassing real credential requirements. */
    private static class TestableOssObjStorage extends OssObjStorage {
        private final OSS mockOss;

        TestableOssObjStorage(Map<String, String> props, OSS mockOss) {
            super(props);
            this.mockOss = mockOss;
        }

        @Override
        protected OSS buildOssClient() {
            return mockOss;
        }
    }
}
