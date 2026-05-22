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

package org.apache.doris.fs;

import org.apache.doris.datasource.property.storage.COSProperties;
import org.apache.doris.datasource.property.storage.OBSProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class StoragePropertiesConverterTest {

    @Test
    public void testS3PropertiesUseAwsFileSystemKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        props.put("s3.region", "us-west-2");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.bucket", "s3-bucket");
        props.put("s3.session_token", "token");
        props.put("sts.role_arn", "arn:aws:iam::123456789012:role/doris");
        props.put("sts.external_id", "external-id");

        Map<String, String> converted = StoragePropertiesConverter.toMap(S3Properties.of(props));

        Assert.assertEquals("S3", converted.get("_STORAGE_TYPE_"));
        Assert.assertEquals("s3.us-west-2.amazonaws.com", converted.get("AWS_ENDPOINT"));
        Assert.assertEquals("us-west-2", converted.get("AWS_REGION"));
        Assert.assertEquals("ak", converted.get("AWS_ACCESS_KEY"));
        Assert.assertEquals("sk", converted.get("AWS_SECRET_KEY"));
        Assert.assertEquals("s3-bucket", converted.get("AWS_BUCKET"));
        Assert.assertEquals("token", converted.get("AWS_TOKEN"));
        Assert.assertEquals("arn:aws:iam::123456789012:role/doris", converted.get("AWS_ROLE_ARN"));
        Assert.assertEquals("external-id", converted.get("AWS_EXTERNAL_ID"));
    }

    @Test
    public void testCosPropertiesUseCosFileSystemKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("cos.endpoint", "cos.ap-guangzhou.myqcloud.com");
        props.put("cos.region", "ap-guangzhou");
        props.put("cos.access_key", "ak");
        props.put("cos.secret_key", "sk");
        props.put("cos.bucket", "cos-bucket");
        props.put("cos.session_token", "token");
        props.put("cos.role_arn", "qcs::cam::uin/100000:roleName/DorisRole");

        Map<String, String> converted = StoragePropertiesConverter.toMap(COSProperties.of(props));

        Assert.assertEquals("COS", converted.get("_STORAGE_TYPE_"));
        Assert.assertEquals("cos.ap-guangzhou.myqcloud.com", converted.get("COS_ENDPOINT"));
        Assert.assertEquals("ap-guangzhou", converted.get("COS_REGION"));
        Assert.assertEquals("ak", converted.get("COS_ACCESS_KEY"));
        Assert.assertEquals("sk", converted.get("COS_SECRET_KEY"));
        Assert.assertEquals("cos-bucket", converted.get("COS_BUCKET"));
        Assert.assertEquals("token", converted.get("COS_TOKEN"));
        Assert.assertEquals("token", converted.get("COS_SESSION_TOKEN"));
        Assert.assertEquals("qcs::cam::uin/100000:roleName/DorisRole", converted.get("COS_ROLE_ARN"));
        Assert.assertFalse(converted.containsKey("AWS_ENDPOINT"));
        Assert.assertFalse(converted.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    public void testObsPropertiesUseObsFileSystemKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        props.put("obs.region", "cn-north-4");
        props.put("obs.access_key", "ak");
        props.put("obs.secret_key", "sk");
        props.put("obs.bucket", "obs-bucket");
        props.put("obs.session_token", "token");
        props.put("obs.agency_name", "agency");
        props.put("obs.domain_name", "domain");

        Map<String, String> converted = StoragePropertiesConverter.toMap(OBSProperties.of(props));

        Assert.assertEquals("OBS", converted.get("_STORAGE_TYPE_"));
        Assert.assertEquals("obs.cn-north-4.myhuaweicloud.com", converted.get("OBS_ENDPOINT"));
        Assert.assertEquals("cn-north-4", converted.get("OBS_REGION"));
        Assert.assertEquals("ak", converted.get("OBS_ACCESS_KEY"));
        Assert.assertEquals("sk", converted.get("OBS_SECRET_KEY"));
        Assert.assertEquals("obs-bucket", converted.get("OBS_BUCKET"));
        Assert.assertEquals("token", converted.get("OBS_TOKEN"));
        Assert.assertEquals("token", converted.get("OBS_SESSION_TOKEN"));
        Assert.assertEquals("agency", converted.get("OBS_AGENCY_NAME"));
        Assert.assertEquals("domain", converted.get("OBS_DOMAIN_NAME"));
        Assert.assertFalse(converted.containsKey("AWS_ENDPOINT"));
        Assert.assertFalse(converted.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    public void testOssPropertiesUseOssFileSystemKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        props.put("oss.region", "cn-hangzhou");
        props.put("oss.access_key", "ak");
        props.put("oss.secret_key", "sk");
        props.put("oss.bucket", "oss-bucket");
        props.put("oss.session_token", "token");
        props.put("oss.role_arn", "acs:ram::123456789012:role/doris");

        Map<String, String> converted = StoragePropertiesConverter.toMap(OSSProperties.of(props));

        Assert.assertEquals("OSS", converted.get("_STORAGE_TYPE_"));
        Assert.assertEquals("oss-cn-hangzhou.aliyuncs.com", converted.get("OSS_ENDPOINT"));
        Assert.assertEquals("cn-hangzhou", converted.get("OSS_REGION"));
        Assert.assertEquals("ak", converted.get("OSS_ACCESS_KEY"));
        Assert.assertEquals("sk", converted.get("OSS_SECRET_KEY"));
        Assert.assertEquals("oss-bucket", converted.get("OSS_BUCKET"));
        Assert.assertEquals("token", converted.get("OSS_TOKEN"));
        Assert.assertEquals("token", converted.get("OSS_SESSION_TOKEN"));
        Assert.assertEquals("acs:ram::123456789012:role/doris", converted.get("OSS_ROLE_ARN"));
        Assert.assertFalse(converted.containsKey("AWS_ENDPOINT"));
        Assert.assertFalse(converted.containsKey("AWS_ACCESS_KEY"));
    }
}
