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

import org.apache.doris.common.security.authentication.HadoopKerberosAuthenticator;
import org.apache.doris.common.security.authentication.HadoopSimpleAuthenticator;
import org.apache.doris.mysql.privilege.AccessControllerFactory;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

class HmsPluginConfigTest {
    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", "thrift://localhost:9083");
        properties.put("cache.ttl.seconds", "600");
        return properties;
    }

    @Test
    void requiresExplicitTtlAndValidLimits() {
        Map<String, String> properties = properties();
        properties.remove("cache.ttl.seconds");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
        properties.put("cache.ttl.seconds", "-1");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
        properties.put("cache.ttl.seconds", "0");
        properties.put("cache.maximum.size", "0");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
        properties.put("cache.maximum.size", "1");
        properties.put("doris.admin.bypass.enabled", "ture");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
    }

    @Test
    void describeListsOperationalSettingsWithoutCredentialPaths() {
        Map<String, String> properties = properties();
        properties.put("hive.metastore.authentication.type", "kerberos");
        properties.put("hive.metastore.client.principal", "doris-hms@EXAMPLE.COM");
        properties.put("hive.metastore.client.keytab", "/secret/doris-hms.keytab");
        properties.put("hive.metastore.service.principal", "hive/_HOST@EXAMPLE.COM");
        properties.put("doris.admin.bypass.enabled", "true");
        String description = new HmsPluginConfig(properties).describe();
        for (String expected : new String[] {"thrift://localhost:9083", "kerberos", "ttl=600", "maximumSize=10000",
                "poolSize=8", "socketTimeout=30", "adminBypass=true"}) {
            Assertions.assertTrue(description.contains(expected), description);
        }
        Assertions.assertFalse(description.contains("/secret"), description);
        Assertions.assertFalse(description.contains("keytab"), description);
    }

    @Test
    void hiveKerberosRequiresItsOwnClientCredentials() {
        Map<String, String> properties = properties();
        Assertions.assertFalse(new HmsPluginConfig(properties).hiveConf()
                .getBoolean("hive.metastore.sasl.enabled", true));
        properties.put("hive.metastore.authentication.type", "kerberos");
        properties.put("hadoop.security.authentication", "kerberos");
        properties.put("hadoop.kerberos.principal", "hdfs-client@EXAMPLE.COM");
        properties.put("hadoop.kerberos.keytab", "/tmp/hdfs-client.keytab");
        properties.put("hive.metastore.service.principal", "hive/_HOST@EXAMPLE.COM");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
        properties.put("hive.metastore.client.principal", "hms-client@EXAMPLE.COM");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
        properties.put("hive.metastore.client.keytab", "/tmp/hms-client.keytab");
        HmsPluginConfig config = new HmsPluginConfig(properties);
        Assertions.assertInstanceOf(HadoopKerberosAuthenticator.class, config.hmsAuthenticator());
        Assertions.assertTrue(MetastoreConf.getBoolVar(config.hiveConf(), MetastoreConf.ConfVars.USE_THRIFT_SASL));
        Assertions.assertEquals("hive/_HOST@EXAMPLE.COM",
                config.hiveConf().get("hive.metastore.kerberos.principal"));
        // The metastore authenticator must not be reconstructed from generic HDFS keys in this conf.
        Assertions.assertNull(config.hiveConf().get("hadoop.kerberos.principal"));
        Assertions.assertNull(config.hiveConf().get("hadoop.kerberos.keytab"));
    }

    @Test
    void hiveKerberosWorksWithoutHdfsKerberosAndAcceptsServicePrincipalAlias() {
        Map<String, String> properties = properties();
        properties.put("hive.metastore.authentication.type", "KERBEROS");
        properties.put("hive.metastore.client.principal", "hms-client@EXAMPLE.COM");
        properties.put("hive.metastore.client.keytab", "/tmp/hms-client.keytab");
        properties.put("hive.metastore.kerberos.principal", "hive/_HOST@EXAMPLE.COM");
        properties.put("hadoop.security.authentication", "simple");
        HmsPluginConfig config = new HmsPluginConfig(properties);
        Assertions.assertInstanceOf(HadoopKerberosAuthenticator.class, config.hmsAuthenticator());
        Assertions.assertTrue(config.hiveConf().getBoolean("hive.metastore.sasl.enabled", false));
        Assertions.assertEquals("hive/_HOST@EXAMPLE.COM",
                config.hiveConf().get("hive.metastore.kerberos.principal"));
    }

    @Test
    void hiveSimpleDoesNotBorrowHdfsCredentialsOrEnableSasl() {
        Map<String, String> properties = properties();
        properties.put("hadoop.security.authentication", "kerberos");
        properties.put("hadoop.kerberos.principal", "hdfs-client@EXAMPLE.COM");
        properties.put("hadoop.kerberos.keytab", "/tmp/hdfs-client.keytab");
        properties.put("hive.metastore.sasl.enabled", "true");
        properties.put("hive.metastore.username", "hms_service");
        for (String authentication : new String[] {null, "simple"}) {
            if (authentication != null) {
                properties.put("hive.metastore.authentication.type", authentication);
            }
            HmsPluginConfig config = new HmsPluginConfig(properties);
            Assertions.assertInstanceOf(HadoopSimpleAuthenticator.class, config.hmsAuthenticator());
            Assertions.assertFalse(MetastoreConf.getBoolVar(config.hiveConf(), MetastoreConf.ConfVars.USE_THRIFT_SASL));
            Assertions.assertEquals("hms_service", config.hiveConf().get("hadoop.username"));
        }
        properties.put("hive.metastore.client.principal", "hms-client@EXAMPLE.COM");
        properties.put("hive.metastore.client.keytab", "/tmp/hms-client.keytab");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
        properties.put("hive.metastore.authentication.type", "kerbero");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsPluginConfig(properties));
    }

    @Test
    void factoryIsDiscoverableByTheExistingDorisServiceLoader() {
        boolean found = false;
        for (AccessControllerFactory factory : ServiceLoader.load(AccessControllerFactory.class)) {
            if (factory instanceof HmsAccessControllerFactory) {
                Assertions.assertEquals("hms-native", factory.factoryIdentifier());
                found = true;
            }
        }
        Assertions.assertTrue(found);
    }

}
