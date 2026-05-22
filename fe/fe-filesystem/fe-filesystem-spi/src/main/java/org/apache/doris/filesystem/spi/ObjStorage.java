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

package org.apache.doris.filesystem.spi;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Low-level object storage abstraction (S3-style API).
 * Zero external dependencies - only JDK types.
 *
 * @param <C> the native client type (e.g., S3Client, BlobServiceClient)
 */
public interface ObjStorage<C> extends AutoCloseable {

    // -----------------------------------------------------------------------
    // Cloud-specific extensions (default UnsupportedOperationException).
    // Implementations override these for backends that support the operation.
    // -----------------------------------------------------------------------

    /**
     * Obtains temporary STS credentials by assuming a configured IAM role.
     * Only supported by cloud providers that have an STS service (AWS, Alibaba OSS, etc.).
     *
     * @return STS credentials (access key, secret key, security token)
     * @throws IOException                   if the STS call fails
     * @throws UnsupportedOperationException if this storage backend has no STS support
     */
    default StsCredentials getStsToken() throws IOException {
        throw new UnsupportedOperationException("getStsToken not supported by " + getClass().getSimpleName());
    }

    /**
     * Lists objects whose keys share the given prefix, with pagination support.
     *
     * @param prefix            the stage/root prefix (e.g. {@code "stage/user1/"})
     * @param subPrefix         sub-prefix relative to {@code prefix}; empty string lists all
     * @param continuationToken opaque pagination token from a previous call; {@code null} for first page
     * @return listing result; use {@link RemoteObjects#getContinuationToken()} and
     *         {@link RemoteObjects#isTruncated()} to fetch subsequent pages
     * @throws IOException                   if the listing fails
     * @throws UnsupportedOperationException if this storage backend does not support this operation
     */
    default RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix,
            String continuationToken) throws IOException {
        throw new UnsupportedOperationException(
                "listObjectsWithPrefix not supported by " + getClass().getSimpleName());
    }

    /**
     * Returns metadata for a single object identified by {@code prefix + subKey}.
     * Returns a single-element result if the object exists, or an empty result if not found.
     *
     * @param prefix the stage/root prefix
     * @param subKey the object key relative to {@code prefix}
     * @return object metadata as a {@link RemoteObjects} result (0 or 1 elements)
     * @throws IOException                   if the HEAD request fails for reasons other than not-found
     * @throws UnsupportedOperationException if this storage backend does not support this operation
     */
    default RemoteObjects headObjectWithMeta(String prefix, String subKey) throws IOException {
        throw new UnsupportedOperationException(
                "headObjectWithMeta not supported by " + getClass().getSimpleName());
    }

    /**
     * Generates a pre-signed URL that allows direct HTTP PUT upload of the given object key,
     * without requiring the uploader to hold storage credentials.
     *
     * @param objectKey the full object key (relative to the bucket root)
     * @return a pre-signed URL string valid for a limited time
     * @throws IOException                   if URL generation fails
     * @throws UnsupportedOperationException if this storage backend does not support pre-signed URLs
     */
    default String getPresignedUrl(String objectKey) throws IOException {
        throw new UnsupportedOperationException(
                "getPresignedUrl not supported by " + getClass().getSimpleName());
    }

    /**
     * Deletes multiple objects by their full object keys in a single logical operation.
     * Implementations may issue individual delete requests or a batch API call.
     *
     * @param bucket the bucket name
     * @param keys   list of full object keys to delete (bare keys, no scheme or bucket prefix)
     * @throws IOException                   if any deletion fails
     * @throws UnsupportedOperationException if this storage backend does not support this operation
     */
    default void deleteObjectsByKeys(String bucket, List<String> keys) throws IOException {
        throw new UnsupportedOperationException(
                "deleteObjectsByKeys not supported by " + getClass().getSimpleName());
    }

    /**
     * Returns the lazily initialized native object-storage client.
     *
     * @return native SDK client used by this storage implementation
     * @throws IOException if client construction fails
     */
    C getClient() throws IOException;

    /**
     * Lists objects under the prefix represented by a full object-storage URI.
     *
     * @param remotePath         full object-storage URI, including scheme, bucket, and key prefix
     * @param continuationToken  opaque pagination token, or {@code null} for the first page
     * @return a page of object metadata and pagination state
     * @throws IOException if the provider list request fails
     */
    RemoteObjects listObjects(String remotePath, String continuationToken) throws IOException;

    /**
     * Lists objects with an implementation-specific page size cap.
     *
     * <p>The default implementation delegates to {@link #listObjects(String, String)}.
     * Providers that support server-side page limits should override this method.
     *
     * @param remotePath         the full object-storage URI to list
     * @param continuationToken  opaque pagination token, or {@code null} for the first page
     * @param maxKeys            maximum number of keys to request; {@code <= 0} means provider default
     * @return a page of object metadata
     */
    default RemoteObjects listObjects(String remotePath, String continuationToken, int maxKeys)
            throws IOException {
        return listObjects(remotePath, continuationToken);
    }

    /**
     * Lists direct object children under a prefix using provider delimiter support.
     *
     * <p>The default implementation delegates to {@link #listObjects(String, String)}.
     * Providers with object-store delimiter support should override this method so callers
     * can implement non-recursive directory listing without scanning nested keys.
     */
    default RemoteObjects listObjectsNonRecursive(String remotePath, String continuationToken)
            throws IOException {
        return listObjects(remotePath, continuationToken);
    }

    /**
     * Returns metadata for a single object.
     *
     * @param remotePath full object-storage URI, including scheme, bucket, and key
     * @return object metadata
     * @throws IOException if the object does not exist or the HEAD request fails
     */
    RemoteObject headObject(String remotePath) throws IOException;

    /**
     * Opens an object stream starting at the given byte offset.
     *
     * @param remotePath the full object-storage URI
     * @param fromByte   zero-based byte offset
     * @return an input stream positioned at {@code fromByte}
     * @throws UnsupportedOperationException if the provider does not support range reads
     */
    default InputStream openInputStreamAt(String remotePath, long fromByte) throws IOException {
        throw new UnsupportedOperationException(
                "openInputStreamAt not supported by " + getClass().getSimpleName());
    }

    /**
     * Returns the last-modified timestamp for an object in milliseconds since epoch.
     *
     * <p>The default implementation delegates to {@link #headObject(String)}.
     */
    default long headObjectLastModified(String remotePath) throws IOException {
        return headObject(remotePath).getModificationTime();
    }

    /**
     * Uploads one object using a single provider PUT operation.
     *
     * @param remotePath   full destination object-storage URI
     * @param requestBody  request body supplier and content length
     * @throws IOException if upload fails
     */
    void putObject(String remotePath, RequestBody requestBody) throws IOException;

    /**
     * Deletes one object. Implementations should treat missing objects as success where the
     * provider API makes that behavior practical.
     *
     * @param remotePath full object-storage URI to delete
     * @throws IOException if deletion fails
     */
    void deleteObject(String remotePath) throws IOException;

    /**
     * Copies one object to another object URI.
     *
     * @param srcPath full source object-storage URI
     * @param dstPath full destination object-storage URI
     * @throws IOException if copy fails
     */
    void copyObject(String srcPath, String dstPath) throws IOException;

    /**
     * Starts a multipart upload session.
     *
     * @param remotePath full destination object-storage URI
     * @return provider upload identifier used for subsequent part operations
     * @throws IOException if multipart initialization fails
     */
    String initiateMultipartUpload(String remotePath) throws IOException;

    /**
     * Uploads one multipart part.
     *
     * @param remotePath full destination object-storage URI
     * @param uploadId   upload identifier returned by {@link #initiateMultipartUpload(String)}
     * @param partNum    one-based part number
     * @param body       part body supplier and content length
     * @return provider ETag and part number for completion
     * @throws IOException if part upload fails
     */
    UploadPartResult uploadPart(String remotePath, String uploadId, int partNum,
            RequestBody body) throws IOException;

    /**
     * Completes a multipart upload session.
     *
     * @param remotePath full destination object-storage URI
     * @param uploadId   upload identifier returned by {@link #initiateMultipartUpload(String)}
     * @param parts      successfully uploaded parts in provider-required order
     * @throws IOException if completion fails
     */
    void completeMultipartUpload(String remotePath, String uploadId,
            List<UploadPartResult> parts) throws IOException;

    /**
     * Aborts a multipart upload session and releases provider-side temporary state.
     *
     * @param remotePath full destination object-storage URI
     * @param uploadId   upload identifier returned by {@link #initiateMultipartUpload(String)}
     * @throws IOException if abort fails
     */
    void abortMultipartUpload(String remotePath, String uploadId) throws IOException;

    /**
     * Returns the normalized properties captured by this storage implementation.
     *
     * @return immutable or implementation-owned property map
     */
    Map<String, String> getProperties();

    /**
     * Closes the native client and releases associated resources.
     *
     * @throws IOException if provider cleanup fails
     */
    @Override
    void close() throws IOException;
}
