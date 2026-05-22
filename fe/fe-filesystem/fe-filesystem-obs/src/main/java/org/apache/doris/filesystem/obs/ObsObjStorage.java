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

import org.apache.doris.filesystem.s3.S3Uri;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;
import org.apache.doris.filesystem.spi.UploadPartResult;

import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.core.auth.ICredential;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuth;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuthIdentity;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.huaweicloud.sdk.iam.v3.model.IdentityAssumerole;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.CopyObjectRequest;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadResult;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PartEtag;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import com.obs.services.model.UploadPartRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Huawei Cloud OBS implementation of {@link ObjStorage} backed by the OBS native SDK.
 *
 * <p>This class consumes OBS-scoped properties only. It does not translate OBS properties
 * to {@code AWS_*} keys and does not use the AWS S3 SDK.
 */
public class ObsObjStorage implements ObjStorage<ObsClient> {

    private static final Logger LOG = LogManager.getLogger(ObsObjStorage.class);
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> obsProperties;
    private volatile ObsClient obsClient;

    public ObsObjStorage(Map<String, String> properties) {
        this.obsProperties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public ObsClient getClient() throws IOException {
        if (obsClient == null) {
            synchronized (this) {
                if (obsClient == null) {
                    obsClient = buildObsClient(
                            resolveRequired("OBS_ENDPOINT", "OBS endpoint"),
                            resolveRequired("OBS_ACCESS_KEY", "OBS access key"),
                            resolveRequired("OBS_SECRET_KEY", "OBS secret key"));
                }
            }
        }
        return obsClient;
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        return listObjects(remotePath, continuationToken, 0);
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken, int maxKeys) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        ListObjectsRequest request = new ListObjectsRequest(uri.bucket());
        request.setPrefix(uri.key());
        if (continuationToken != null && !continuationToken.isEmpty()) {
            request.setMarker(continuationToken);
        }
        if (maxKeys > 0) {
            request.setMaxKeys(maxKeys);
        }
        return doListObjects(request, uri.key());
    }

    @Override
    public RemoteObjects listObjectsNonRecursive(String remotePath, String continuationToken) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        ListObjectsRequest request = new ListObjectsRequest(uri.bucket());
        request.setPrefix(uri.key());
        request.setDelimiter("/");
        if (continuationToken != null && !continuationToken.isEmpty()) {
            request.setMarker(continuationToken);
        }
        return doListObjects(request, uri.key());
    }

    private RemoteObjects doListObjects(ListObjectsRequest request, String prefix) throws IOException {
        try {
            ObjectListing listing = getClient().listObjects(request);
            List<RemoteObject> objects = listing.getObjects().stream()
                    .map(obj -> toRemoteObject(prefix, obj))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, listing.isTruncated(), listing.getNextMarker());
        } catch (ObsException e) {
            throw toIOException("Failed to list OBS objects", e);
        }
    }

    private RemoteObject toRemoteObject(String prefix, ObsObject obj) {
        ObjectMetadata metadata = obj.getMetadata();
        long size = metadata == null || metadata.getContentLength() == null ? 0L : metadata.getContentLength();
        long lastModified = metadata == null || metadata.getLastModified() == null
                ? 0L : metadata.getLastModified().getTime();
        String etag = metadata == null ? null : metadata.getEtag();
        return new RemoteObject(obj.getObjectKey(), relativePath(prefix, obj.getObjectKey()),
                etag, size, lastModified);
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(uri.bucket(), uri.key());
            return new RemoteObject(uri.key(), uri.key(), metadata.getEtag(),
                    metadata.getContentLength() == null ? 0L : metadata.getContentLength(),
                    metadata.getLastModified() == null ? 0L : metadata.getLastModified().getTime());
        } catch (ObsException e) {
            throw toIOException("Object not found: " + remotePath, e);
        }
    }

    @Override
    public InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key());
            if (fromByte > 0) {
                request.setRangeStart(fromByte);
            }
            return getClient().getObject(request).getObjectContent();
        } catch (ObsException e) {
            throw toIOException("Failed to open OBS object: " + remotePath, e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try (InputStream content = requestBody.content()) {
            PutObjectRequest request = new PutObjectRequest(uri.bucket(), uri.key(), content);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(requestBody.contentLength());
            request.setMetadata(metadata);
            getClient().putObject(request);
        } catch (ObsException e) {
            throw toIOException("putObject failed for " + remotePath, e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            getClient().deleteObject(uri.bucket(), uri.key());
        } catch (ObsException e) {
            if (!isNotFound(e)) {
                throw toIOException("deleteObject failed for " + remotePath, e);
            }
        }
    }

    @Override
    public void copyObject(String srcPath, String dstPath) throws IOException {
        S3Uri src = S3Uri.parse(srcPath, false);
        S3Uri dst = S3Uri.parse(dstPath, false);
        try {
            getClient().copyObject(new CopyObjectRequest(src.bucket(), src.key(), dst.bucket(), dst.key()));
        } catch (ObsException e) {
            throw toIOException("copyObject failed from " + srcPath + " to " + dstPath, e);
        }
    }

    @Override
    public String initiateMultipartUpload(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            InitiateMultipartUploadResult result = getClient().initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(uri.bucket(), uri.key()));
            return result.getUploadId();
        } catch (ObsException e) {
            throw toIOException("initiateMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try (InputStream content = body.content()) {
            UploadPartRequest request = new UploadPartRequest(uri.bucket(), uri.key());
            request.setUploadId(uploadId);
            request.setPartNumber(partNum);
            request.setInput(content);
            request.setPartSize(body.contentLength());
            com.obs.services.model.UploadPartResult result = getClient().uploadPart(request);
            return new UploadPartResult(partNum, result.getEtag());
        } catch (ObsException e) {
            throw toIOException("uploadPart failed for " + remotePath, e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        List<PartEtag> eTags = parts.stream()
                .map(part -> new PartEtag(part.etag(), part.partNumber()))
                .collect(Collectors.toList());
        try {
            getClient().completeMultipartUpload(
                    new CompleteMultipartUploadRequest(uri.bucket(), uri.key(), uploadId, eTags));
        } catch (ObsException e) {
            throw toIOException("completeMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            getClient().abortMultipartUpload(new AbortMultipartUploadRequest(uri.bucket(), uri.key(), uploadId));
        } catch (ObsException e) {
            throw toIOException("abortMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
        keys.forEach(request::addKeyAndVersion);
        request.setQuiet(true);
        try {
            getClient().deleteObjects(request);
        } catch (ObsException e) {
            throw toIOException("deleteObjects failed for bucket=" + bucket, e);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = resolveRequired("OBS_BUCKET", "OBS bucket for presigned URL");
        try {
            TemporarySignatureRequest request = new TemporarySignatureRequest(
                    HttpMethodEnum.PUT, SESSION_EXPIRE_SECONDS);
            request.setBucketName(bucket);
            request.setObjectKey(objectKey);
            request.setHeaders(new HashMap<>());
            TemporarySignatureResponse response = getClient().createTemporarySignature(request);
            LOG.info("Generated OBS temporary signature URL for key={}", objectKey);
            return response.getSignedUrl();
        } catch (ObsException e) {
            throw toIOException("Failed to generate OBS temporary signature URL", e);
        }
    }

    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = resolveRequired("OBS_REGION", "OBS region for STS");
        String accessKey = resolveRequired("OBS_ACCESS_KEY", "OBS access key");
        String secretKey = resolveRequired("OBS_SECRET_KEY", "OBS secret key");
        String agencyName = resolveRequired("OBS_AGENCY_NAME", "OBS agency name for STS");
        String domainName = resolveRequired("OBS_DOMAIN_NAME", "OBS domain name for STS");
        try {
            ICredential auth = new GlobalCredentials().withAk(accessKey).withSk(secretKey);
            IamClient client = IamClient.newBuilder()
                    .withEndpoint("iam." + region + ".myhuaweicloud.com")
                    .withCredential(auth)
                    .build();
            IdentityAssumerole assumeRoleIdentity = new IdentityAssumerole();
            assumeRoleIdentity.withAgencyName(agencyName)
                    .withDomainName(domainName)
                    .withDurationSeconds(SESSION_EXPIRE_SECONDS);
            List<AgencyAuthIdentity.MethodsEnum> methods = new ArrayList<>();
            methods.add(AgencyAuthIdentity.MethodsEnum.fromValue("assume_role"));
            AgencyAuthIdentity identityAuth = new AgencyAuthIdentity();
            identityAuth.withMethods(methods).withAssumeRole(assumeRoleIdentity);
            AgencyAuth authBody = new AgencyAuth();
            authBody.withIdentity(identityAuth);
            CreateTemporaryAccessKeyByAgencyRequestBody body =
                    new CreateTemporaryAccessKeyByAgencyRequestBody().withAuth(authBody);
            CreateTemporaryAccessKeyByAgencyRequest request =
                    new CreateTemporaryAccessKeyByAgencyRequest().withBody(body);
            CreateTemporaryAccessKeyByAgencyResponse response =
                    client.createTemporaryAccessKeyByAgency(request);
            Credential credential = response.getCredential();
            return new StsCredentials(
                    credential.getAccess(),
                    credential.getSecret(),
                    credential.getSecuritytoken());
        } catch (Exception e) {
            LOG.warn("Failed to get OBS STS token, agencyName={}", agencyName, e);
            throw new IOException("Failed to get OBS STS token: " + e.getMessage(), e);
        }
    }

    protected ObsClient buildObsClient(String endpoint, String accessKey, String secretKey) {
        String sessionToken = resolveOptional("OBS_TOKEN", "OBS_SESSION_TOKEN");
        if (!sessionToken.isEmpty()) {
            return new ObsClient(accessKey, secretKey, sessionToken, endpoint);
        }
        return new ObsClient(accessKey, secretKey, endpoint);
    }

    @Override
    public Map<String, String> getProperties() {
        return obsProperties;
    }

    @Override
    public void close() throws IOException {
        if (obsClient != null) {
            obsClient.close();
            obsClient = null;
        }
    }

    private String resolveRequired(String primaryKey, String description) throws IOException {
        String value = obsProperties.get(primaryKey);
        if (value == null || value.isEmpty()) {
            throw new IOException(description + " is required; set " + primaryKey + " in properties");
        }
        return value;
    }

    private String resolveOptional(String... keys) {
        for (String key : keys) {
            String value = obsProperties.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private IOException toIOException(String message, ObsException e) {
        if (isNotFound(e)) {
            return new FileNotFoundException(message);
        }
        return new IOException(message + ": " + e.getMessage(), e);
    }

    private boolean isNotFound(ObsException e) {
        return e.getResponseCode() == 404 || "NoSuchKey".equals(e.getErrorCode());
    }

    private static String relativePath(String prefix, String key) {
        String normalized = prefix == null || prefix.isEmpty() ? "" : prefix.endsWith("/") ? prefix : prefix + "/";
        if (!key.startsWith(normalized)) {
            return key;
        }
        return key.substring(normalized.length());
    }
}
