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

import org.apache.doris.filesystem.s3.S3Uri;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;
import org.apache.doris.filesystem.spi.UploadPartResult;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.AbortMultipartUploadRequest;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.UploadPartRequest;
import com.qcloud.cos.region.Region;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleRequest;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleResponse;
import com.tencentcloudapi.sts.v20180813.models.Credentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tencent Cloud COS implementation of {@link ObjStorage} backed by the COS native SDK.
 *
 * <p>This class consumes COS-scoped properties only. It does not translate COS properties
 * to {@code AWS_*} keys and does not depend on AWS S3 credentials or clients.
 */
public class CosObjStorage implements ObjStorage<COSClient> {

    private static final Logger LOG = LogManager.getLogger(CosObjStorage.class);
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> cosProperties;
    private volatile COSClient cosClient;

    public CosObjStorage(Map<String, String> properties) {
        this.cosProperties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public COSClient getClient() throws IOException {
        if (cosClient == null) {
            synchronized (this) {
                if (cosClient == null) {
                    cosClient = buildCosClient(resolveRequired("COS_REGION", "COS region"));
                }
            }
        }
        return cosClient;
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException {
        return listObjects(remotePath, continuationToken, 0);
    }

    @Override
    public RemoteObjects listObjects(String remotePath, String continuationToken, int maxKeys) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(uri.bucket());
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
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(uri.bucket());
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
            List<RemoteObject> objects = listing.getObjectSummaries().stream()
                    .map(obj -> toRemoteObject(prefix, obj))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, listing.isTruncated(), listing.getNextMarker());
        } catch (CosClientException e) {
            throw toIOException("Failed to list COS objects", e);
        }
    }

    private RemoteObject toRemoteObject(String prefix, COSObjectSummary obj) {
        return new RemoteObject(obj.getKey(), relativePath(prefix, obj.getKey()), obj.getETag(),
                obj.getSize(), obj.getLastModified() == null ? 0L : obj.getLastModified().getTime());
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(uri.bucket(), uri.key());
            return new RemoteObject(uri.key(), uri.key(), metadata.getETag(),
                    metadata.getContentLength(),
                    metadata.getLastModified() == null ? 0L : metadata.getLastModified().getTime());
        } catch (CosClientException e) {
            throw toIOException("Object not found: " + remotePath, e);
        }
    }

    @Override
    public InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            GetObjectRequest request = new GetObjectRequest(uri.bucket(), uri.key());
            if (fromByte > 0) {
                request.setRange(fromByte, Long.MAX_VALUE);
            }
            COSObject object = getClient().getObject(request);
            return object.getObjectContent();
        } catch (CosClientException e) {
            throw toIOException("Failed to open COS object: " + remotePath, e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(requestBody.contentLength());
        try (InputStream content = requestBody.content()) {
            getClient().putObject(uri.bucket(), uri.key(), content, metadata);
        } catch (CosClientException e) {
            throw toIOException("putObject failed for " + remotePath, e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            getClient().deleteObject(uri.bucket(), uri.key());
        } catch (CosClientException e) {
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
        } catch (CosClientException e) {
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
        } catch (CosClientException e) {
            throw toIOException("initiateMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try (InputStream content = body.content()) {
            UploadPartRequest request = new UploadPartRequest()
                    .withBucketName(uri.bucket())
                    .withKey(uri.key())
                    .withUploadId(uploadId)
                    .withPartNumber(partNum)
                    .withInputStream(content)
                    .withPartSize(body.contentLength());
            com.qcloud.cos.model.UploadPartResult result = getClient().uploadPart(request);
            return new UploadPartResult(partNum, result.getETag());
        } catch (CosClientException e) {
            throw toIOException("uploadPart failed for " + remotePath, e);
        }
    }

    @Override
    public void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        List<PartETag> eTags = parts.stream()
                .map(part -> new PartETag(part.partNumber(), part.etag()))
                .collect(Collectors.toList());
        try {
            getClient().completeMultipartUpload(
                    new CompleteMultipartUploadRequest(uri.bucket(), uri.key(), uploadId, eTags));
        } catch (CosClientException e) {
            throw toIOException("completeMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            getClient().abortMultipartUpload(new AbortMultipartUploadRequest(uri.bucket(), uri.key(), uploadId));
        } catch (CosClientException e) {
            throw toIOException("abortMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
        request.setQuiet(true);
        request.withKeys(keys.toArray(new String[0]));
        try {
            getClient().deleteObjects(request);
        } catch (CosClientException e) {
            throw toIOException("deleteObjects failed for bucket=" + bucket, e);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = resolveRequired("COS_BUCKET", "COS bucket for presigned URL");
        try {
            COSClient cos = getClient();
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            URL url = cos.generatePresignedUrl(bucket, objectKey, expiration, HttpMethodName.PUT,
                    new HashMap<>(), new HashMap<>());
            LOG.info("Generated COS presigned URL for key={}", objectKey);
            return url.toString();
        } catch (CosClientException e) {
            throw toIOException("Failed to generate COS presigned URL", e);
        }
    }

    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = resolveRequired("COS_REGION", "COS region for STS");
        String accessKey = resolveRequired("COS_ACCESS_KEY", "COS access key");
        String secretKey = resolveRequired("COS_SECRET_KEY", "COS secret key");
        String roleArn = resolveRequired("COS_ROLE_ARN", "COS role ARN");
        try {
            Credential credential = new Credential(accessKey, secretKey);
            StsClient stsClient = new StsClient(credential, region);
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
            request.setDurationSeconds((long) SESSION_EXPIRE_SECONDS);
            AssumeRoleResponse response = stsClient.AssumeRole(request);
            Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getTmpSecretId(),
                    credentials.getTmpSecretKey(),
                    credentials.getToken());
        } catch (Exception e) {
            LOG.warn("Failed to get COS STS token, roleArn={}", roleArn, e);
            throw new IOException("Failed to get COS STS token: " + e.getMessage(), e);
        }
    }

    protected COSClient buildCosClient(String region) throws IOException {
        String accessKey = resolveRequired("COS_ACCESS_KEY", "COS access key");
        String secretKey = resolveRequired("COS_SECRET_KEY", "COS secret key");
        String sessionToken = resolveOptional("COS_TOKEN", "COS_SESSION_TOKEN");
        COSCredentials cred = sessionToken.isEmpty()
                ? new BasicCOSCredentials(accessKey, secretKey)
                : new BasicSessionCredentials(accessKey, secretKey, sessionToken);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRegion(new Region(region));
        clientConfig.setHttpProtocol(HttpProtocol.https);
        return new COSClient(cred, clientConfig);
    }

    @Override
    public Map<String, String> getProperties() {
        return cosProperties;
    }

    @Override
    public void close() {
        if (cosClient != null) {
            cosClient.shutdown();
            cosClient = null;
        }
    }

    private String resolveRequired(String primaryKey, String description) throws IOException {
        String value = cosProperties.get(primaryKey);
        if (value == null || value.isEmpty()) {
            throw new IOException(description + " is required; set " + primaryKey + " in properties");
        }
        return value;
    }

    private String resolveOptional(String... keys) {
        for (String key : keys) {
            String value = cosProperties.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private IOException toIOException(String message, CosClientException e) {
        if (isNotFound(e)) {
            return new FileNotFoundException(message);
        }
        return new IOException(message + ": " + e.getMessage(), e);
    }

    private boolean isNotFound(CosClientException e) {
        return e instanceof CosServiceException
                && ((CosServiceException) e).getStatusCode() == 404;
    }

    private static String relativePath(String prefix, String key) {
        String normalized = prefix == null || prefix.isEmpty() ? "" : prefix.endsWith("/") ? prefix : prefix + "/";
        if (!key.startsWith(normalized)) {
            return key;
        }
        return key.substring(normalized.length());
    }
}
