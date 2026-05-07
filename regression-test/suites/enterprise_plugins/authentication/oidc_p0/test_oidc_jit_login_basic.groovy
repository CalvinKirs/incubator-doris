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

suite("test_oidc_jit_login_basic", "docker,p0") {
    if (context.config.otherConfigs.get("enableEnterprisePluginOidcTest") != "true") {
        logger.info("enableEnterprisePluginOidcTest is not true, skip synced OIDC suite")
        return
    }

    String suiteName = "test_oidc_jit_login_basic"
    String dbName = context.config.getDbNameByFile(context.file)
    Class helperClass = new GroovyShell(getClass().classLoader)
            .evaluate(new File(context.file.parentFile, "test_oidc_common.groovy")) as Class
    def oidc = helperClass.newInstance(delegate)

    Map fixture = oidc.prepareBaseOidcFixture([
            "oidc_alice_grafana_query",
            "oidc_erin_grafana_query"
    ])
    Map manifest = fixture.manifest
    Map<String, Map> tokensByName = fixture.tokensByName
    Map aliceToken = tokensByName["oidc_alice_grafana_query"]
    Map erinToken = tokensByName["oidc_erin_grafana_query"]

    assertEquals("grafana-doris-plugin", aliceToken.clientId)
    assertEquals("grafana-doris-plugin", erinToken.clientId)
    assertTrue(aliceToken.claims.scope.toString().contains("doris.query"))
    assertTrue(erinToken.claims.scope.toString().contains("doris.query"))
    assertEquals("oidc_alice", aliceToken.username)
    assertEquals("oidc_erin", erinToken.username)

    Map liveLogin = oidc.prepareLiveLogin(suiteName, suiteName + "_oidc")
    String integrationName = liveLogin.integrationName.toString()
    String roleName = suiteName + "_scope_reader"
    String tableName = suiteName + "_scope_table"
    String roleMappingName = suiteName + "_mapping"

    try {
        try_sql("DROP USER IF EXISTS 'oidc_erin'")
        oidc.dropRoleMappingsOnIntegration(integrationName)
        try_sql("DROP ROLE MAPPING IF EXISTS ${roleMappingName}")
        try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
        try_sql("DROP ROLE IF EXISTS ${roleName}")
        try_sql("DROP TABLE IF EXISTS ${tableName}")

        oidc.createRole(roleName)
        oidc.createSingleIntTable(tableName, 11)
        oidc.grantSelect(dbName, tableName, roleName)
        oidc.createOidcIntegration(integrationName, manifest, [
                requiredScopes: ["doris.query"],
                allowedClientIds: ["grafana-doris-plugin"],
                comment: "OIDC JIT login regression integration"
        ])

        List<Map> roleMappingRules = [
                [cel: 'has_scope("doris.query")', role: roleName]
        ]
        oidc.createRoleMapping(
                roleMappingName,
                integrationName,
                roleMappingRules,
                "OIDC JIT login role mapping")
        sql "sync"

        def integrationRows = sql """
            SELECT
                NAME,
                TYPE,
                PROPERTIES,
                COMMENT
            FROM information_schema.authentication_integrations
            WHERE NAME = '${integrationName}'
            ORDER BY NAME
        """
        assertEquals(1, integrationRows.size())
        assertEquals("oidc", integrationRows[0][1])
        assertTrue(integrationRows[0][2].contains("\"oidc.required_scopes\" = \"doris.query\""))
        assertTrue(integrationRows[0][2].contains("\"oidc.allowed_client_ids\" = \"grafana-doris-plugin\""))

        def roleMappingRows = sql """
            SELECT
                NAME,
                INTEGRATION_NAME,
                RULES,
                COMMENT
            FROM information_schema.role_mappings
            WHERE NAME = '${roleMappingName}'
            ORDER BY NAME
        """
        assertEquals(1, roleMappingRows.size())
        assertEquals(roleMappingName, roleMappingRows[0][0])
        assertEquals(integrationName, roleMappingRows[0][1])
        assertEquals(oidc.expectedRoleMappingRules(roleMappingRules), roleMappingRows[0][2])
        assertEquals("OIDC JIT login role mapping", roleMappingRows[0][3])

        if (!(liveLogin.mysqlshAvailable as boolean)) {
            return
        }

        oidc.withOidcAuthenticationChain(liveLogin) {
            Map aliceQueryResult = oidc.runOidcMysqlsh(
                    liveLogin,
                    "oidc_alice",
                    aliceToken.path.toString(),
                    "SELECT k FROM ${dbName}.${tableName} ORDER BY k")
            assertEquals(0, aliceQueryResult.status)
            assertTrue(aliceQueryResult.output.contains("11"))

            Map erinQueryResult = oidc.runOidcMysqlsh(
                    liveLogin,
                    "oidc_erin",
                    erinToken.path.toString(),
                    "SELECT k FROM ${dbName}.${tableName} ORDER BY k")
            assertEquals(0, erinQueryResult.status)
            assertTrue(erinQueryResult.output.contains("11"))

            Map erinCurrentUserResult = oidc.runOidcMysqlsh(
                    liveLogin,
                    "oidc_erin",
                    erinToken.path.toString(),
                    "SELECT CURRENT_USER()")
            assertEquals(0, erinCurrentUserResult.status)
            assertTrue(erinCurrentUserResult.output.contains("oidc_erin"))
        }
    } finally {
        try_sql("DROP USER IF EXISTS 'oidc_erin'")
    }
}
