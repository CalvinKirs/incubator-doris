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

import org.apache.doris.filesystem.s3.S3Uri;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.filesystem.spi.StsCredentials;
import org.apache.doris.filesystem.spi.UploadPartResult;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.profile.DefaultProfile;
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
 * Alibaba Cloud OSS implementation of {@link ObjStorage} backed by the OSS native SDK.
 *
 * <p>This class consumes OSS-scoped properties only. It does not translate OSS properties
 * to {@code AWS_*} keys and does not use the AWS S3 SDK.
 */
public class OssObjStorage implements ObjStorage<OSS> {

    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> ossProperties;
    private volatile OSS ossClient;

    public OssObjStorage(Map<String, String> properties) {
        this.ossProperties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public OSS getClient() throws IOException {
        if (ossClient == null) {
            synchronized (this) {
                if (ossClient == null) {
                    ossClient = buildOssClient();
                }
            }
        }
        return ossClient;
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
            List<RemoteObject> objects = listing.getObjectSummaries().stream()
                    .map(obj -> toRemoteObject(prefix, obj))
                    .collect(Collectors.toList());
            return new RemoteObjects(objects, listing.isTruncated(), listing.getNextMarker());
        } catch (ClientException | ServiceException e) {
            throw toIOException("Failed to list OSS objects", e);
        }
    }

    private RemoteObject toRemoteObject(String prefix, OSSObjectSummary obj) {
        return new RemoteObject(obj.getKey(), relativePath(prefix, obj.getKey()), obj.getETag(),
                obj.getSize(), obj.getLastModified() == null ? 0L : obj.getLastModified().getTime());
    }

    @Override
    public RemoteObject headObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            ObjectMetadata metadata = getClient().headObject(uri.bucket(), uri.key());
            return new RemoteObject(uri.key(), uri.key(), metadata.getETag(),
                    metadata.getContentLength(),
                    metadata.getLastModified() == null ? 0L : metadata.getLastModified().getTime());
        } catch (ClientException | ServiceException e) {
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
            OSSObject object = getClient().getObject(request);
            return object.getObjectContent();
        } catch (ClientException | ServiceException e) {
            throw toIOException("Failed to open OSS object: " + remotePath, e);
        }
    }

    @Override
    public void putObject(String remotePath, RequestBody requestBody) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(requestBody.contentLength());
        try (InputStream content = requestBody.content()) {
            getClient().putObject(uri.bucket(), uri.key(), content, metadata);
        } catch (ClientException | ServiceException e) {
            throw toIOException("putObject failed for " + remotePath, e);
        }
    }

    @Override
    public void deleteObject(String remotePath) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            getClient().deleteObject(uri.bucket(), uri.key());
        } catch (ClientException | ServiceException e) {
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
        } catch (ClientException | ServiceException e) {
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
        } catch (ClientException | ServiceException e) {
            throw toIOException("initiateMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try (InputStream content = body.content()) {
            UploadPartRequest request = new UploadPartRequest(
                    uri.bucket(), uri.key(), uploadId, partNum, content, body.contentLength());
            com.aliyun.oss.model.UploadPartResult result = getClient().uploadPart(request);
            return new UploadPartResult(partNum, result.getETag());
        } catch (ClientException | ServiceException e) {
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
        } catch (ClientException | ServiceException e) {
            throw toIOException("completeMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public void abortMultipartUpload(String remotePath, String uploadId) throws IOException {
        S3Uri uri = S3Uri.parse(remotePath, false);
        try {
            getClient().abortMultipartUpload(new AbortMultipartUploadRequest(uri.bucket(), uri.key(), uploadId));
        } catch (ClientException | ServiceException e) {
            throw toIOException("abortMultipartUpload failed for " + remotePath, e);
        }
    }

    @Override
    public void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);
        request.withKeys(keys);
        request.withQuiet(true);
        try {
            getClient().deleteObjects(request);
        } catch (ClientException | ServiceException e) {
            throw toIOException("deleteObjects failed for bucket=" + bucket, e);
        }
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = resolveRequired("OSS_BUCKET", "OSS bucket for presigned URL");
        try {
            OSS oss = getClient();
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, objectKey, HttpMethod.PUT);
            request.setExpiration(expiration);
            URL signedUrl = oss.generatePresignedUrl(request);
            LOG.info("Generated OSS presigned URL for key={}", objectKey);
            return signedUrl.toString();
        } catch (ClientException | ServiceException e) {
            throw toIOException("Failed to generate OSS presigned URL", e);
        }
    }

    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = resolveRequired("OSS_REGION", "OSS region for STS");
        String accessKey = resolveRequired("OSS_ACCESS_KEY", "OSS access key");
        String secretKey = resolveRequired("OSS_SECRET_KEY", "OSS secret key");
        String roleArn = resolveRequired("OSS_ROLE_ARN", "OSS role ARN");
        try {
            DefaultProfile profile = DefaultProfile.getProfile(region);
            BasicCredentials basicCredentials = new BasicCredentials(accessKey, secretKey);
            DefaultAcsClient ramClient =
                    new DefaultAcsClient(profile, new StaticCredentialsProvider(basicCredentials));
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
            request.setDurationSeconds((long) SESSION_EXPIRE_SECONDS);
            AssumeRoleResponse response = ramClient.getAcsResponse(request);
            AssumeRoleResponse.Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getAccessKeyId(),
                    credentials.getAccessKeySecret(),
                    credentials.getSecurityToken());
        } catch (Exception e) {
            LOG.warn("Failed to get OSS STS token, roleArn={}", roleArn, e);
            throw new IOException("Failed to get OSS STS token: " + e.getMessage(), e);
        }
    }

    protected OSS buildOssClient() throws IOException {
        String endpoint = resolveRequired("OSS_ENDPOINT", "OSS endpoint");
        String accessKey = resolveRequired("OSS_ACCESS_KEY", "OSS access key");
        String secretKey = resolveRequired("OSS_SECRET_KEY", "OSS secret key");
        String token = resolveOptional("OSS_TOKEN", "OSS_SESSION_TOKEN");
        if (!token.isEmpty()) {
            return new OSSClientBuilder().build(endpoint, accessKey, secretKey, token);
        }
        return new OSSClientBuilder().build(endpoint, accessKey, secretKey);
    }

    @Override
    public Map<String, String> getProperties() {
        return ossProperties;
    }

    @Override
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    private String resolveRequired(String primaryKey, String description) throws IOException {
        String value = ossProperties.get(primaryKey);
        if (value == null || value.isEmpty()) {
            throw new IOException(description + " is required; set " + primaryKey + " in properties");
        }
        return value;
    }

    private String resolveOptional(String... keys) {
        for (String key : keys) {
            String value = ossProperties.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    private IOException toIOException(String message, RuntimeException e) {
        if (isNotFound(e)) {
            return new FileNotFoundException(message);
        }
        return new IOException(message + ": " + e.getMessage(), e);
    }

    private boolean isNotFound(RuntimeException e) {
        if (e instanceof OSSException) {
            return "NoSuchKey".equals(((OSSException) e).getErrorCode());
        }
        if (e instanceof ServiceException) {
            return "NoSuchKey".equals(((ServiceException) e).getErrorCode());
        }
        if (e instanceof ClientException) {
            return "NoSuchKey".equals(((ClientException) e).getErrorCode());
        }
        return false;
    }

    private static String relativePath(String prefix, String key) {
        String normalized = prefix == null || prefix.isEmpty() ? "" : prefix.endsWith("/") ? prefix : prefix + "/";
        if (!key.startsWith(normalized)) {
            return key;
        }
        return key.substring(normalized.length());
    }
}
