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

import org.apache.doris.filesystem.s3.AbstractS3CompatibleFileSystemProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Map;

public final class OssFileSystemProperties extends AbstractS3CompatibleFileSystemProperties {

    private static final String STORAGE_TYPE = "OSS";

    @ConnectorProperty(names = {"AWS_ENDPOINT", "OSS_ENDPOINT", "oss.endpoint", "fs.oss.endpoint",
            "dlf.endpoint", "dlf.catalog.endpoint", "s3.endpoint", "endpoint", "ENDPOINT"},
            required = false)
    private String endpoint;

    @ConnectorProperty(names = {"AWS_REGION", "OSS_REGION", "oss.region", "fs.oss.region", "dlf.region",
            "s3.region", "region", "REGION", "iceberg.rest.signing-region"},
            required = false, isRegionField = true)
    private String region;

    @ConnectorProperty(names = {"AWS_ACCESS_KEY", "OSS_ACCESS_KEY", "oss.access_key", "oss.access-key-id",
            "fs.oss.accessKeyId", "dlf.access_key", "dlf.catalog.accessKeyId", "s3.access_key",
            "s3.access-key-id", "access_key", "ACCESS_KEY"}, required = false, sensitive = true)
    private String accessKey;

    @ConnectorProperty(names = {"AWS_SECRET_KEY", "OSS_SECRET_KEY", "oss.secret_key", "oss.secret-access-key",
            "fs.oss.accessKeySecret", "dlf.secret_key", "dlf.catalog.secret_key", "s3.secret_key",
            "s3.secret-access-key", "secret_key", "SECRET_KEY"}, required = false, sensitive = true)
    private String secretKey;

    @ConnectorProperty(names = {"AWS_TOKEN", "OSS_TOKEN", "oss.session_token", "oss.session-token",
            "fs.oss.securityToken", "s3.session_token", "s3.session-token", "session_token"},
            required = false, sensitive = true)
    private String token;

    @ConnectorProperty(names = {"AWS_BUCKET", "OSS_BUCKET", "oss.bucket", "s3.bucket", "bucket"},
            required = false)
    private String bucket;

    @ConnectorProperty(names = {"AWS_ROLE_ARN", "OSS_ROLE_ARN", "oss.role_arn", "s3.role_arn",
            "sts.role_arn", "glue.role_arn"}, required = false)
    private String roleArn;

    @ConnectorProperty(names = {"AWS_EXTERNAL_ID", "OSS_EXTERNAL_ID", "oss.external_id", "s3.external_id",
            "sts.external_id", "glue.external_id"}, required = false)
    private String externalId;

    @ConnectorProperty(names = {"use_path_style", "oss.use_path_style", "oss.path-style-access",
            "s3.use_path_style", "s3.path-style-access"}, required = false)
    private String pathStyle;

    private OssFileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties, STORAGE_TYPE);
    }

    public static OssFileSystemProperties bind(Map<String, String> rawProperties) {
        return new OssFileSystemProperties(rawProperties);
    }

    @Override
    protected String endpoint() {
        return endpoint;
    }

    @Override
    protected String region() {
        return region;
    }

    @Override
    protected String accessKey() {
        return accessKey;
    }

    @Override
    protected String secretKey() {
        return secretKey;
    }

    @Override
    protected String token() {
        return token;
    }

    @Override
    protected String bucket() {
        return bucket;
    }

    @Override
    protected String roleArn() {
        return roleArn;
    }

    @Override
    protected String externalId() {
        return externalId;
    }

    @Override
    protected String pathStyle() {
        return pathStyle;
    }
}
