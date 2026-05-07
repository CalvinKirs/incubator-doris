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

suite("test_oidc_login_diagnostics", "docker,p0") {
    if (context.config.otherConfigs.get("enableEnterprisePluginOidcTest") != "true") {
        logger.info("enableEnterprisePluginOidcTest is not true, skip synced OIDC suite")
        return
    }

    String suiteName = "test_oidc_login_diagnostics"
    String dbName = context.config.getDbNameByFile(context.file)
    Class helperClass = new GroovyShell(getClass().classLoader)
            .evaluate(new File(context.file.parentFile, "test_oidc_common.groovy")) as Class
    def oidc = helperClass.newInstance(delegate)

    Map fixture = oidc.prepareBaseOidcFixture([
            "oidc_alice_grafana_query",
            "oidc_alice_grafana_missing_scope",
            "oidc_alice_agent_query",
            "oidc_bob_grafana_query"
    ])
    Map manifest = fixture.manifest
    Map<String, Map> tokensByName = fixture.tokensByName
    Map aliceToken = tokensByName["oidc_alice_grafana_query"]
    Map missingScopeToken = tokensByName["oidc_alice_grafana_missing_scope"]
    Map deniedClientToken = tokensByName["oidc_alice_agent_query"]
    Map bobToken = tokensByName["oidc_bob_grafana_query"]

    assertEquals("oidc_alice", aliceToken.username)
    assertEquals("oidc_alice", missingScopeToken.username)
    assertEquals("oidc_alice", deniedClientToken.username)
    assertEquals("oidc_bob", bobToken.username)
    assertEquals("grafana-doris-plugin", aliceToken.clientId)
    assertEquals("grafana-doris-plugin", missingScopeToken.clientId)
    assertEquals("agent-gateway", deniedClientToken.clientId)
    assertEquals("grafana-doris-plugin", bobToken.clientId)
    assertTrue(aliceToken.claims.scope.toString().contains("doris.query"))
    assertTrue(!missingScopeToken.claims.scope.toString().contains("doris.query"))
    assertTrue(deniedClientToken.claims.scope.toString().contains("doris.query"))
    assertTrue(bobToken.claims.scope.toString().contains("doris.query"))

    Map liveLogin = oidc.prepareLiveLogin(suiteName, suiteName + "_oidc")
    String integrationName = liveLogin.integrationName.toString()
    String roleName = suiteName + "_reader"
    String tableName = suiteName + "_table"
    String roleMappingName = suiteName + "_mapping"

    try_sql("DROP USER IF EXISTS 'oidc_bob'")
    oidc.dropRoleMappingsOnIntegration(integrationName)
    try_sql("DROP ROLE MAPPING IF EXISTS ${roleMappingName}")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    try_sql("DROP ROLE IF EXISTS ${roleName}")
    try_sql("DROP TABLE IF EXISTS ${tableName}")

    oidc.createRole(roleName)
    oidc.createSingleIntTable(tableName, 31)
    oidc.grantSelect(dbName, tableName, roleName)
    oidc.createOidcIntegration(integrationName, manifest, [
            requiredScopes: ["doris.query"],
            allowedClientIds: ["grafana-doris-plugin"],
            comment: "OIDC login diagnostics regression integration"
    ])

    List<Map> roleMappingRules = [
            [cel: 'has_scope("doris.query")', role: roleName]
    ]
    oidc.createRoleMapping(
            roleMappingName,
            integrationName,
            roleMappingRules,
            "OIDC login diagnostics role mapping")
    sql "sync"

    if (!(liveLogin.mysqlshAvailable as boolean)) {
        return
    }

    oidc.withOidcAuthenticationChain(liveLogin) {
        Map allowedResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                aliceToken.path.toString(),
                "SELECT k FROM ${dbName}.${tableName} ORDER BY k")
        assertEquals(0, allowedResult.status)
        assertTrue(allowedResult.output.contains("31"))

        Map insecureResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                aliceToken.path.toString(),
                "SELECT 1",
                "DISABLED")
        assertTrue(insecureResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                insecureResult.output,
                "The client-server connection is insecure"))

        Map usernameMismatchResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_bob",
                aliceToken.path.toString(),
                "SELECT 1")
        assertTrue(usernameMismatchResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                usernameMismatchResult.output,
                "Authentication request username does not match OIDC access token username"))

        Map missingScopeResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                missingScopeToken.path.toString(),
                "SELECT 1")
        assertTrue(missingScopeResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                missingScopeResult.output,
                "OIDC access token scope is missing a required value"))

        Map deniedClientResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                deniedClientToken.path.toString(),
                "SELECT 1")
        assertTrue(deniedClientResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                deniedClientResult.output,
                "OIDC access token client id is not allowed"))

        String badSignatureTokenPath = "/tmp/${suiteName}_bad_signature.access_token"
        String badSignatureToken = ("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9."
                + "eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjgwODAvcmVhbG1zL2RvcmlzIiwiYXVkIjpbImRvcmlzIl0s"
                + "ImV4cCI6NDcxMDAwMDAwMCwicHJlZmVycmVkX3VzZXJuYW1lIjoib2lkY19hbGljZSIsInN1YiI6"
                + "InN1YmplY3QtYWxpY2UiLCJzY29wZSI6ImRvcmlzLnF1ZXJ5IiwiYXpwIjoiZ3JhZmFuYS1kb3Jp"
                + "cy1wbHVnaW4ifQ.invalidsignature")
        new File(badSignatureTokenPath).text = badSignatureToken
        Map badSignatureResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                badSignatureTokenPath,
                "SELECT 1")
        assertTrue(badSignatureResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                badSignatureResult.output,
                "OIDC access token signature validation failed"))

        try {
            sql """
                ALTER AUTHENTICATION INTEGRATION ${integrationName}
                SET PROPERTIES ('oidc.allowed_audiences' = 'other-service')
            """
            sql "sync"

            Map invalidAudienceResult = oidc.runOidcMysqlsh(
                    liveLogin,
                    "oidc_alice",
                    aliceToken.path.toString(),
                    "SELECT 1")
            assertTrue(invalidAudienceResult.status != 0)
            assertTrue(oidc.containsAuthenticationFailure(
                    invalidAudienceResult.output,
                    "OIDC access token audience is not allowed"))
        } finally {
            sql """
                ALTER AUTHENTICATION INTEGRATION ${integrationName}
                SET PROPERTIES ('oidc.allowed_audiences' = '${manifest.audience}')
            """
            sql "sync"
        }

        try {
            sql """
                ALTER AUTHENTICATION INTEGRATION ${integrationName}
                SET PROPERTIES ('enable_jit_user' = 'false')
            """
            sql "sync"

            Map jitDisabledResult = oidc.runOidcMysqlsh(
                    liveLogin,
                    "oidc_bob",
                    bobToken.path.toString(),
                    "SELECT 1")
            assertTrue(jitDisabledResult.status != 0)
            assertTrue(oidc.containsAuthenticationFailure(
                    jitDisabledResult.output,
                    "OIDC authentication succeeded but no matching Doris user exists and JIT user is disabled"))
        } finally {
            sql """
                ALTER AUTHENTICATION INTEGRATION ${integrationName}
                SET PROPERTIES ('enable_jit_user' = 'true')
            """
            sql "sync"
        }
    }
}
