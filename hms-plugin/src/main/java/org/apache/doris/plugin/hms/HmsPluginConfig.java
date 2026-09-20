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

package org.apache.doris.plugin.hms;

import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.property.metastore.HMSBaseProperties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Configuration belongs to this controller instance, independently of the catalog metadata client. */
public final class HmsPluginConfig {
    final long ttlSeconds;
    final long maximumSize;
    final int poolSize;
    final int socketTimeoutSeconds;
    final boolean adminBypass;
    final String uris;
    final String authentication;
    private final Map<String, String> properties;
    private final HMSBaseProperties hmsProperties;

    public HmsPluginConfig(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        uris = required("hms.uri");
        for (String address : uris.split(",", -1)) {
            URI uri = URI.create(address.trim());
            Preconditions.checkArgument("thrift".equals(uri.getScheme()) && uri.getHost() != null
                    && uri.getPort() > 0 && uri.getPort() <= 65535, "Invalid hms.uri endpoint");
        }
        // Require an explicit revocation window instead of silently choosing a deployment policy.
        ttlSeconds = Long.parseLong(required("cache.ttl.seconds"));
        maximumSize = Long.parseLong(properties.getOrDefault("cache.maximum.size", "10000"));
        poolSize = Integer.parseInt(properties.getOrDefault("hms.pool.size", "8"));
        Preconditions.checkArgument(ttlSeconds >= 0, "cache.ttl.seconds must be nonnegative");
        Preconditions.checkArgument(maximumSize > 0, "cache.maximum.size must be positive");
        Preconditions.checkArgument(poolSize > 0, "hms.pool.size must be positive");
        socketTimeoutSeconds = Integer.parseInt(properties.getOrDefault("hms.socket.timeout.seconds", "30"));
        Preconditions.checkArgument(socketTimeoutSeconds > 0, "hms.socket.timeout.seconds must be positive");
        String bypass = properties.getOrDefault("doris.admin.bypass.enabled", "false");
        Preconditions.checkArgument("true".equalsIgnoreCase(bypass) || "false".equalsIgnoreCase(bypass),
                "doris.admin.bypass.enabled must be true or false");
        adminBypass = Boolean.parseBoolean(bypass);
        authentication = properties.getOrDefault("hive.metastore.authentication.type", "simple")
                .toLowerCase(Locale.ROOT);
        Preconditions.checkArgument("simple".equals(authentication) || "kerberos".equals(authentication),
                "hive.metastore.authentication.type must be simple or kerberos");
        Map<String, String> hiveProperties = new HashMap<>(this.properties);
        hiveProperties.put(HMSBaseProperties.HIVE_METASTORE_URIS, uris);
        // Select HMS authentication explicitly; never select a client's credentials from HDFS properties.
        hiveProperties.put("hive.metastore.authentication.type", authentication);
        hmsProperties = HMSBaseProperties.of(hiveProperties);
        HiveConf conf = hmsProperties.getHiveConf();
        conf.set("hadoop.security.authentication", authentication);
        conf.setBoolean("hive.metastore.sasl.enabled", "kerberos".equals(authentication));
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_SASL, "kerberos".equals(authentication));
        MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, uris);
        MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT,
                socketTimeoutSeconds, TimeUnit.SECONDS);
    }

    private String required(String key) {
        String value = properties.get(key);
        Preconditions.checkArgument(value != null && !value.trim().isEmpty(), "Missing property: %s", key);
        return value;
    }

    /** Operational settings only; credential locations and Hive properties stay out of the log. */
    String describe() {
        return "uri=" + uris + ", authentication=" + authentication + ", ttl=" + ttlSeconds + "s, maximumSize="
                + maximumSize + ", poolSize=" + poolSize + ", socketTimeout=" + socketTimeoutSeconds
                + "s, adminBypass=" + adminBypass;
    }

    HiveConf hiveConf() {
        return hmsProperties.getHiveConf();
    }

    HadoopAuthenticator hmsAuthenticator() {
        return hmsProperties.getHmsAuthenticator();
    }
}
