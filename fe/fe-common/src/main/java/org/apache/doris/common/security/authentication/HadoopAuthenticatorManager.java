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

package org.apache.doris.common.security.authentication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class HadoopAuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(HadoopAuthenticatorManager.class);

    private static final ConcurrentHashMap<AuthenticationConfig, HadoopAuthenticator> authenticatorMap =
            new ConcurrentHashMap<>();

    public static HadoopAuthenticator getAuthenticator(AuthenticationConfig config) {
        return authenticatorMap.computeIfAbsent(config, HadoopAuthenticatorManager::createAuthenticator);
    }

    private static HadoopAuthenticator createAuthenticator(AuthenticationConfig config) {
        LOG.info("Creating a new authenticator.");
        if (config instanceof KerberosAuthenticationConfig) {
            return new HadoopKerberosAuthenticator((KerberosAuthenticationConfig) config);
        } else if (config instanceof SimpleAuthenticationConfig) {
            return new HadoopSimpleAuthenticator((SimpleAuthenticationConfig) config);
        } else {
            throw new IllegalArgumentException("Unsupported AuthenticationConfig type: " + config.getClass().getName());
        }
    }
}
