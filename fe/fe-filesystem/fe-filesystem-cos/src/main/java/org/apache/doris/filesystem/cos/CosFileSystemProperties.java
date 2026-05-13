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

import org.apache.doris.filesystem.s3.AbstractS3CompatibleFileSystemProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Map;

public final class CosFileSystemProperties extends AbstractS3CompatibleFileSystemProperties {

    private static final String STORAGE_TYPE = "COS";

    @ConnectorProperty(names = {"AWS_ENDPOINT", "COS_ENDPOINT", "cos.endpoint", "fs.cos.endpoint",
            "s3.endpoint", "endpoint", "ENDPOINT"}, required = false)
    private String endpoint;

    @ConnectorProperty(names = {"AWS_REGION", "COS_REGION", "cos.region", "fs.cos.region",
            "s3.region", "region", "REGION"}, required = false, isRegionField = true)
    private String region;

    @ConnectorProperty(names = {"AWS_ACCESS_KEY", "COS_ACCESS_KEY", "cos.access_key", "cos.access-key-id",
            "fs.cos.accessKeyId", "s3.access_key", "s3.access-key-id", "access_key", "ACCESS_KEY"},
            required = false, sensitive = true)
    private String accessKey;

    @ConnectorProperty(names = {"AWS_SECRET_KEY", "COS_SECRET_KEY", "cos.secret_key", "cos.secret-access-key",
            "fs.cos.accessKeySecret", "s3.secret_key", "s3.secret-access-key", "secret_key", "SECRET_KEY"},
            required = false, sensitive = true)
    private String secretKey;

    @ConnectorProperty(names = {"AWS_TOKEN", "COS_TOKEN", "cos.session_token", "cos.session-token",
            "fs.cos.securityToken", "s3.session_token", "s3.session-token", "session_token"},
            required = false, sensitive = true)
    private String token;

    @ConnectorProperty(names = {"AWS_BUCKET", "COS_BUCKET", "cos.bucket", "s3.bucket", "bucket"},
            required = false)
    private String bucket;

    @ConnectorProperty(names = {"AWS_ROLE_ARN", "COS_ROLE_ARN", "cos.role_arn", "s3.role_arn",
            "sts.role_arn", "glue.role_arn"}, required = false)
    private String roleArn;

    @ConnectorProperty(names = {"AWS_EXTERNAL_ID", "COS_EXTERNAL_ID", "cos.external_id", "s3.external_id",
            "sts.external_id", "glue.external_id"}, required = false)
    private String externalId;

    @ConnectorProperty(names = {"use_path_style", "cos.use_path_style", "cos.path-style-access",
            "s3.use_path_style", "s3.path-style-access"}, required = false)
    private String pathStyle;

    private CosFileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties, STORAGE_TYPE);
    }

    public static CosFileSystemProperties bind(Map<String, String> rawProperties) {
        return new CosFileSystemProperties(rawProperties);
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
