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
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.security.AccessController;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

/** Real SASL/KDC tests, isolated from the Simple HMS fixture. See docker/kerberos/README.md. */
@EnabledIfEnvironmentVariable(named = "HMS_KRB_TEST_URI", matches = ".+")
class HmsKerberosIntegrationTest {
    private static String database;

    private static Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hms.uri", System.getenv("HMS_KRB_TEST_URI"));
        properties.put("cache.ttl.seconds", "0");
        properties.put("hms.socket.timeout.seconds", "2");
        properties.put("hive.metastore.connect.retries", "1");
        properties.put("hive.metastore.failure.retries", "1");
        properties.put("hive.metastore.authentication.type", "kerberos");
        properties.put("hive.metastore.client.principal", "doris-hms@HMS.AUTH.TEST");
        properties.put("hive.metastore.client.keytab", keytab("client"));
        properties.put("hive.metastore.service.principal", "hive/localhost@HMS.AUTH.TEST");
        // These deliberately unusable HDFS credentials must never select the HMS identity.
        properties.put("hadoop.security.authentication", "simple");
        properties.put("hadoop.kerberos.principal", "not-hms@INVALID.TEST");
        properties.put("hadoop.kerberos.keytab", "/missing/hdfs.keytab");
        return properties;
    }

    private static String keytab(String name) {
        return Path.of(System.getenv("HMS_KRB_TEST_DIR"), name + ".keytab").toString();
    }

    @BeforeAll
    static void createFixture() throws Exception {
        System.setProperty("java.security.krb5.conf",
                Path.of(System.getenv("HMS_KRB_TEST_DIR"), "krb5.conf").toString());
        HmsPluginConfig config = new HmsPluginConfig(properties());
        database = "kerberos_" + UUID.randomUUID().toString().replace("-", "");
        config.hmsAuthenticator().doAs(() -> {
            try (HmsConnection connection = HmsConnection.open(config.hiveConf())) {
                Database db = new Database();
                db.setName(database);
                db.setOwnerName("KrbOwner");
                db.setOwnerType(PrincipalType.USER);
                db.setParameters(new HashMap<>());
                connection.client.create_database(db);
            }
            return null;
        });
        System.out.println("Kerberos HMS fixture: " + database);
    }

    @Test
    void serviceLoginKeepsQueryUserIdentityAndIgnoresHdfsCredentials() {
        HmsPluginConfig config = new HmsPluginConfig(properties());
        HmsAuthorization auth = new HmsAuthorization(config, new PooledHmsPrivilegeSource(config));
        Assertions.assertTrue(auth.canCreate(auth.identities("KrbOwner"), database));
        Assertions.assertFalse(auth.canCreate(auth.identities("krbowner"), database));
        Assertions.assertFalse(auth.canCreate(auth.identities("doris-hms"), database));
    }

    @ParameterizedTest
    @ValueSource(strings = {"wrong-keytab", "missing-keytab", "wrong-service", "simple"})
    void invalidAuthenticationFailsClosed(String scenario) {
        Map<String, String> properties = properties();
        switch (scenario) {
            case "wrong-keytab":
                properties.put("hive.metastore.client.keytab", keytab("other"));
                break;
            case "missing-keytab":
                properties.put("hive.metastore.client.keytab", keytab("missing"));
                break;
            case "wrong-service":
                properties.put("hive.metastore.service.principal", "hive/missing@HMS.AUTH.TEST");
                break;
            case "simple":
                properties.put("hive.metastore.authentication.type", "simple");
                properties.remove("hive.metastore.client.principal");
                properties.remove("hive.metastore.client.keytab");
                break;
            default:
                throw new AssertionError(scenario);
        }
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(new HmsPluginConfig(properties));
        Assertions.assertThrows(HmsAuthorizationException.class, () -> source.database(database));
        Assertions.assertEquals(HmsPrincipal.user("KrbOwner"),
                new PooledHmsPrivilegeSource(new HmsPluginConfig(properties())).database(database).owner);
    }

    @Test
    void shortLivedTicketRefreshesOnSameUgiAndPool() throws Exception {
        Map<String, String> properties = properties();
        properties.put("hive.metastore.client.principal", "doris-renew@HMS.AUTH.TEST");
        properties.put("hive.metastore.client.keytab", keytab("renew"));
        HmsPluginConfig config = new HmsPluginConfig(properties);
        HadoopAuthenticator authenticator = config.hmsAuthenticator();
        // Same construction path as the production pool, exposing only the authenticator for observation.
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(1,
                new HadoopExecutionAuthenticator(authenticator), () -> HmsConnection.open(config.hiveConf()));
        Assertions.assertEquals(HmsPrincipal.user("KrbOwner"), source.database(database).owner);
        UserGroupInformation originalUgi = authenticator.getUGI();
        long[] original = ticketTimes(authenticator);
        Assertions.assertTrue(original[1] - original[0] <= 60_000, "KDC did not issue a short-lived TGT");
        Assertions.assertTrue(original[1] > System.currentTimeMillis());
        // Keep issuing RPCs past the original TGT's end; success alone is not proof of renewal.
        while (System.currentTimeMillis() <= original[1] + 1000) {
            Assertions.assertEquals(HmsPrincipal.user("KrbOwner"), source.database(database).owner);
            Thread.sleep(1000);
        }
        long[] refreshed = ticketTimes(authenticator);
        Assertions.assertSame(originalUgi, authenticator.getUGI());
        Assertions.assertTrue(refreshed[0] > original[0], "TGT start time did not advance");
        Assertions.assertTrue(refreshed[1] > original[1], "TGT end time did not advance");
        System.out.println("Kerberos TGT refresh: " + Arrays.toString(original) + " -> "
                + Arrays.toString(refreshed));
    }

    private long[] ticketTimes(HadoopAuthenticator authenticator) throws Exception {
        return authenticator.doAs(() -> {
            Subject subject = Subject.getSubject(AccessController.getContext());
            KerberosTicket ticket = subject.getPrivateCredentials(KerberosTicket.class).stream()
                    .filter(value -> value.getServer().getName().startsWith("krbtgt/"))
                    .findFirst().orElseThrow(() -> new AssertionError("No TGT in doAs Subject"));
            return new long[] {ticket.getStartTime().getTime(), ticket.getEndTime().getTime()};
        });
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "HMS_KRB_TEST_CONTAINER", matches = ".+")
    void saslPoolReconnectsAfterHmsRestart() throws Exception {
        PooledHmsPrivilegeSource source = new PooledHmsPrivilegeSource(new HmsPluginConfig(properties()));
        Assertions.assertEquals(HmsPrincipal.user("KrbOwner"), source.database(database).owner);
        String container = System.getenv("HMS_KRB_TEST_CONTAINER");
        try {
            docker("stop", "--time", "2", container);
            Assertions.assertThrows(HmsAuthorizationException.class, () -> source.database(database));
        } finally {
            docker("start", container);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (true) {
            try {
                Assertions.assertEquals(HmsPrincipal.user("KrbOwner"), source.database(database).owner);
                break;
            } catch (HmsAuthorizationException e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
    }

    private void docker(String... arguments) throws Exception {
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add("docker");
        java.util.Collections.addAll(command, arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(new java.io.File("target/kerberos-docker.log")))
                .start();
        Assertions.assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Docker command timed out");
        Assertions.assertEquals(0, process.exitValue());
    }
}
