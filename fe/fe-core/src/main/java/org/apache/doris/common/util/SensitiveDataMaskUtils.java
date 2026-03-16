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

package org.apache.doris.common.util;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;

public final class SensitiveDataMaskUtils {
    public static final String MASK = "******";

    private SensitiveDataMaskUtils() {
    }

    public static String maskSecret(String value) {
        if (value == null) {
            return null;
        }
        return value.isEmpty() ? value : MASK;
    }

    public static String maskToken(String token) {
        return maskSecret(token);
    }

    public static String sanitizeObjectStoreInfoPB(ObjectStoreInfoPB objectStoreInfoPB) {
        if (objectStoreInfoPB == null) {
            return "null";
        }
        return "ObjectStoreInfoPB{"
                + "provider=" + objectStoreInfoPB.getProvider()
                + ", bucket=" + objectStoreInfoPB.getBucket()
                + ", endpoint=" + objectStoreInfoPB.getEndpoint()
                + ", region=" + objectStoreInfoPB.getRegion()
                + ", prefix=" + objectStoreInfoPB.getPrefix()
                + ", externalEndpoint=" + objectStoreInfoPB.getExternalEndpoint()
                + ", roleArn=" + objectStoreInfoPB.getRoleArn()
                + ", externalId=" + maskSecret(objectStoreInfoPB.getExternalId())
                + ", ak=" + maskSecret(objectStoreInfoPB.getAk())
                + ", sk=" + maskSecret(objectStoreInfoPB.getSk())
                + '}';
    }
}
