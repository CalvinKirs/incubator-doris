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

import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Map;

public final class S3FileSystemProperties extends AbstractS3CompatibleFileSystemProperties {

    @ConnectorProperty(names = {"AWS_ENDPOINT", "s3.endpoint", "endpoint", "ENDPOINT", "aws.endpoint",
            "glue.endpoint", "aws.glue.endpoint"}, required = false)
    private String endpoint;

    @ConnectorProperty(names = {"AWS_REGION", "s3.region", "region", "REGION", "aws.region", "glue.region",
            "aws.glue.region", "iceberg.rest.signing-region", "rest.signing-region", "client.region"},
            required = false, isRegionField = true)
    private String region;

    @ConnectorProperty(names = {"AWS_ACCESS_KEY", "s3.access_key", "s3.access-key-id", "access_key", "ACCESS_KEY",
            "glue.access_key", "aws.glue.access-key", "client.credentials-provider.glue.access_key",
            "iceberg.rest.access-key-id"}, required = false, sensitive = true)
    private String accessKey;

    @ConnectorProperty(names = {"AWS_SECRET_KEY", "s3.secret_key", "s3.secret-access-key", "secret_key",
            "SECRET_KEY", "glue.secret_key", "aws.glue.secret-key", "client.credentials-provider.glue.secret_key",
            "iceberg.rest.secret-access-key"}, required = false, sensitive = true)
    private String secretKey;

    @ConnectorProperty(names = {"AWS_TOKEN", "s3.session_token", "session_token", "s3.session-token",
            "iceberg.rest.session-token"}, required = false, sensitive = true)
    private String token;

    @ConnectorProperty(names = {"AWS_BUCKET", "s3.bucket", "bucket"}, required = false)
    private String bucket;

    @ConnectorProperty(names = {"AWS_ROLE_ARN", "s3.role_arn", "sts.role_arn", "glue.role_arn"},
            required = false)
    private String roleArn;

    @ConnectorProperty(names = {"AWS_EXTERNAL_ID", "s3.external_id", "sts.external_id", "glue.external_id"},
            required = false)
    private String externalId;

    @ConnectorProperty(names = {"use_path_style", "s3.use_path_style", "s3.path-style-access"},
            required = false)
    private String pathStyle;

    private S3FileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties, null);
    }

    public static S3FileSystemProperties bind(Map<String, String> rawProperties) {
        return new S3FileSystemProperties(rawProperties);
    }

    static Map<String, String> normalize(Map<String, String> rawProperties) {
        return bind(rawProperties).toFileSystemKv();
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
