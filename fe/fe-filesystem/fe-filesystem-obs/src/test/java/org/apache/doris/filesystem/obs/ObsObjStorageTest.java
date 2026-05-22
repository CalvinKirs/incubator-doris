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

import org.apache.doris.filesystem.s3.S3ObjStorage;

import com.obs.services.ObsClient;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link ObsObjStorage}.
 *
 * <p>Cloud calls are replaced with mocks; no real OBS credentials are required.
 */
class ObsObjStorageTest {

    @Test
    void obsStorageKeepsObsPropertiesWithoutAwsTranslation() {
        Map<String, String> obsProps = new HashMap<>();
        obsProps.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        obsProps.put("OBS_ACCESS_KEY", "myAK");
        obsProps.put("OBS_SECRET_KEY", "mySK");
        obsProps.put("OBS_BUCKET", "my-obs-bucket");
        obsProps.put("OBS_REGION", "cn-north-4");

        ObsObjStorage storage = new ObsObjStorage(obsProps);

        Assertions.assertFalse(S3ObjStorage.class.isAssignableFrom(storage.getClass()));
        Assertions.assertEquals("https://obs.cn-north-4.myhuaweicloud.com", storage.getProperties().get("OBS_ENDPOINT"));
        Assertions.assertFalse(storage.getProperties().containsKey("AWS_ENDPOINT"));
        Assertions.assertFalse(storage.getProperties().containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    void obsStorageDoesNotAcceptAwsFallbackKeys() {
        Map<String, String> obsProps = new HashMap<>();
        obsProps.put("AWS_ENDPOINT", "https://custom.endpoint");
        obsProps.put("AWS_ACCESS_KEY", "awsAK");

        ObsObjStorage storage = new ObsObjStorage(obsProps);

        Assertions.assertThrows(IOException.class,
                () -> storage.getPresignedUrl("stage/f1"));
    }

    // ------------------------------------------------------------------
    // getPresignedUrl() with mocked OBS client
    // ------------------------------------------------------------------

    @Test
    void getPresignedUrl_returnsSignedUrlFromObsClient() throws Exception {
        String expectedUrl = "https://my-obs-bucket.obs.cn-north-4.myhuaweicloud.com/stage/f1?sig=abc";
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        TemporarySignatureResponse mockResp = Mockito.mock(TemporarySignatureResponse.class);
        Mockito.when(mockResp.getSignedUrl()).thenReturn(expectedUrl);
        Mockito.when(mockObs.createTemporarySignature(Mockito.any(TemporarySignatureRequest.class)))
                .thenReturn(mockResp);

        Map<String, String> props = buildBasicProps();
        ObsObjStorage storage = new TestableObsObjStorage(props, mockObs);

        String result = storage.getPresignedUrl("stage/f1");

        Assertions.assertEquals(expectedUrl, result);
        ArgumentCaptor<TemporarySignatureRequest> captor =
                ArgumentCaptor.forClass(TemporarySignatureRequest.class);
        Mockito.verify(mockObs).createTemporarySignature(captor.capture());
        Assertions.assertEquals("my-obs-bucket", captor.getValue().getBucketName());
        Assertions.assertEquals("stage/f1", captor.getValue().getObjectKey());
        Assertions.assertEquals(HttpMethodEnum.PUT, captor.getValue().getMethod());
    }

    @Test
    void getPresignedUrl_missingBucketThrowsIOException() {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.myhuaweicloud.com");
        props.put("OBS_ACCESS_KEY", "ak");
        props.put("OBS_SECRET_KEY", "sk");
        // no bucket

        ObsObjStorage storage = new TestableObsObjStorage(props, mockObs);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when bucket is missing");
    }

    @Test
    void getPresignedUrl_missingEndpointThrowsIOException() {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ACCESS_KEY", "ak");
        props.put("OBS_SECRET_KEY", "sk");
        props.put("OBS_BUCKET", "my-bucket");
        // no endpoint

        ObsObjStorage storage = new TestableObsObjStorage(props, mockObs);

        Assertions.assertThrows(IOException.class, () -> storage.getPresignedUrl("some/key"),
                "Should throw when endpoint is missing");
    }

    @Test
    void getPresignedUrl_expireSecondsIs3600() throws Exception {
        ObsClient mockObs = Mockito.mock(ObsClient.class);
        TemporarySignatureResponse mockResp = Mockito.mock(TemporarySignatureResponse.class);
        Mockito.when(mockResp.getSignedUrl())
                .thenReturn("https://bucket.obs.cn-north-4.myhuaweicloud.com/k?sig=x");
        Mockito.when(mockObs.createTemporarySignature(Mockito.any(TemporarySignatureRequest.class)))
                .thenAnswer(inv -> {
                    TemporarySignatureRequest req = inv.getArgument(0);
                    Assertions.assertEquals(3600L, req.getExpires(),
                            "OBS presigned URL must expire in 3600s");
                    return mockResp;
                });

        ObsObjStorage storage = new TestableObsObjStorage(buildBasicProps(), mockObs);
        storage.getPresignedUrl("key");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Map<String, String> buildBasicProps() {
        Map<String, String> props = new HashMap<>();
        props.put("OBS_ENDPOINT", "https://obs.cn-north-4.myhuaweicloud.com");
        props.put("OBS_ACCESS_KEY", "testAK");
        props.put("OBS_SECRET_KEY", "testSK");
        props.put("OBS_BUCKET", "my-obs-bucket");
        props.put("OBS_REGION", "cn-north-4");
        return props;
    }

    /** Subclass that injects a mock ObsClient, bypassing real credential requirements. */
    private static class TestableObsObjStorage extends ObsObjStorage {
        private final ObsClient mockObs;

        TestableObsObjStorage(Map<String, String> props, ObsClient mockObs) {
            super(props);
            this.mockObs = mockObs;
        }

        @Override
        protected ObsClient buildObsClient(String endpoint, String accessKey, String secretKey) {
            return mockObs;
        }
    }
}
