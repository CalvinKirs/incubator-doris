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

import org.apache.doris.filesystem.s3.AbstractS3CompatibleFileSystemProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.Map;

public final class ObsFileSystemProperties extends AbstractS3CompatibleFileSystemProperties {

    private static final String STORAGE_TYPE = "OBS";

    @ConnectorProperty(names = {"obs.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "OBS_ENDPOINT", "fs.obs.endpoint"}, required = false)
    private String endpoint;

    @ConnectorProperty(names = {"obs.region", "s3.region", "AWS_REGION", "region", "REGION",
            "OBS_REGION", "fs.obs.region"}, required = false, isRegionField = true)
    private String region;

    @ConnectorProperty(names = {"obs.access_key", "s3.access_key", "s3.access-key-id", "AWS_ACCESS_KEY",
            "access_key", "ACCESS_KEY", "OBS_ACCESS_KEY", "obs.access-key-id", "fs.obs.accessKeyId"},
            required = false, sensitive = true)
    private String accessKey;

    @ConnectorProperty(names = {"obs.secret_key", "s3.secret_key", "s3.secret-access-key", "AWS_SECRET_KEY",
            "secret_key", "SECRET_KEY", "OBS_SECRET_KEY", "obs.secret-access-key", "fs.obs.accessKeySecret"},
            required = false, sensitive = true)
    private String secretKey;

    @ConnectorProperty(names = {"obs.session_token", "s3.session_token", "s3.session-token", "session_token",
            "AWS_TOKEN", "OBS_TOKEN", "obs.session-token", "fs.obs.securityToken"},
            required = false, sensitive = true)
    private String token;

    @ConnectorProperty(names = {"s3.bucket", "AWS_BUCKET", "OBS_BUCKET", "obs.bucket", "bucket"},
            required = false)
    private String bucket;

    @ConnectorProperty(names = {"s3.role_arn", "AWS_ROLE_ARN", "glue.role_arn", "OBS_ROLE_ARN",
            "obs.role_arn", "sts.role_arn"}, required = false)
    private String roleArn;

    @ConnectorProperty(names = {"s3.external_id", "AWS_EXTERNAL_ID", "glue.external_id", "OBS_EXTERNAL_ID",
            "obs.external_id", "sts.external_id"}, required = false)
    private String externalId;

    @ConnectorProperty(names = {"obs.use_path_style", "use_path_style", "s3.path-style-access",
            "obs.path-style-access", "s3.use_path_style"}, required = false)
    private String pathStyle;

    private ObsFileSystemProperties(Map<String, String> rawProperties) {
        super(rawProperties, STORAGE_TYPE);
    }

    public static ObsFileSystemProperties bind(Map<String, String> rawProperties) {
        return new ObsFileSystemProperties(rawProperties);
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
