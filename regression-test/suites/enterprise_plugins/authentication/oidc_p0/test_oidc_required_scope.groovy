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

suite("test_oidc_required_scope", "docker,p0") {
    if (context.config.otherConfigs.get("enableEnterprisePluginOidcTest") != "true") {
        logger.info("enableEnterprisePluginOidcTest is not true, skip synced OIDC suite")
        return
    }

    String suiteName = "test_oidc_required_scope"
    String dbName = context.config.getDbNameByFile(context.file)
    Class helperClass = new GroovyShell(getClass().classLoader)
            .evaluate(new File(context.file.parentFile, "test_oidc_common.groovy")) as Class
    def oidc = helperClass.newInstance(delegate)

    Map fixture = oidc.prepareBaseOidcFixture([
            "oidc_alice_grafana_query",
            "oidc_alice_grafana_missing_scope"
    ])
    Map manifest = fixture.manifest
    Map<String, Map> tokensByName = fixture.tokensByName
    Map aliceToken = tokensByName["oidc_alice_grafana_query"]
    Map missingScopeToken = tokensByName["oidc_alice_grafana_missing_scope"]

    assertEquals("grafana-doris-plugin", aliceToken.clientId)
    assertEquals("grafana-doris-plugin", missingScopeToken.clientId)
    assertTrue(aliceToken.claims.scope.toString().contains("doris.query"))
    assertTrue(!missingScopeToken.claims.scope.toString().contains("doris.query"))

    Map liveLogin = oidc.prepareLiveLogin(suiteName, suiteName + "_oidc")
    String integrationName = liveLogin.integrationName.toString()
    String roleName = suiteName + "_reader"
    String tableName = suiteName + "_table"
    String roleMappingName = suiteName + "_mapping"
    List<Map> roleMappingRules = [
            [cel: 'has_scope("doris.query")', role: roleName]
    ]

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
            comment: "OIDC required scope regression integration"
    ])
    oidc.createRoleMapping(
            roleMappingName,
            integrationName,
            roleMappingRules,
            "OIDC required scope regression mapping")
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
    assertEquals("OIDC required scope regression integration", integrationRows[0][3])

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
    assertEquals("OIDC required scope regression mapping", roleMappingRows[0][3])

    if (!(liveLogin.mysqlshAvailable as boolean)) {
        return
    }

    oidc.withOidcAuthenticationChain(liveLogin) {
        Map allowedQueryResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                aliceToken.path.toString(),
                "SELECT k FROM ${dbName}.${tableName} ORDER BY k")
        assertEquals(0, allowedQueryResult.status)
        assertTrue(allowedQueryResult.output.contains("11"))

        Map missingScopeResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                missingScopeToken.path.toString(),
                "SELECT 1")
        assertTrue(missingScopeResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                missingScopeResult.output,
                "OIDC access token scope is missing a required value"))
    }
}
