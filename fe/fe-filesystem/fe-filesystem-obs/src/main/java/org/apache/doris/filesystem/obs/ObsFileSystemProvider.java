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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for Huawei Cloud OBS.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by {@code _STORAGE_TYPE_=OBS} or an OBS endpoint containing
 * {@code myhuaweicloud.com}. The provider creates {@link ObsObjStorage} directly with
 * OBS-scoped properties, for example {@code OBS_ENDPOINT}, {@code OBS_REGION},
 * {@code OBS_ACCESS_KEY}, and {@code OBS_SECRET_KEY}.
 */
public class ObsFileSystemProvider implements FileSystemProvider {

    @Override
    public boolean supports(Map<String, String> properties) {
        if ("OBS".equals(properties.get("_STORAGE_TYPE_"))) {
            return true;
        }
        String endpoint = properties.get("OBS_ENDPOINT");
        return endpoint != null && endpoint.contains("myhuaweicloud.com");
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return new S3FileSystem(name(), new ObsObjStorage(properties));
    }

    @Override
    public String name() {
        return "OBS";
    }
}
