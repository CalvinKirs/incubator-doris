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

suite("test_oidc_access_token_auth", "docker,p0") {
    if (context.config.otherConfigs.get("enableEnterprisePluginOidcTest") != "true") {
        logger.info("enableEnterprisePluginOidcTest is not true, skip synced OIDC suite")
        return
    }

    String suiteName = "test_oidc_access_token_auth"
    String dbName = context.config.getDbNameByFile(context.file)
    Class helperClass = new GroovyShell(getClass().classLoader)
            .evaluate(new File(context.file.parentFile, "test_oidc_common.groovy")) as Class
    def oidc = helperClass.newInstance(delegate)

    Map fixture = oidc.prepareBaseOidcFixture([
            "oidc_alice_grafana_query",
            "oidc_alice_grafana_missing_scope",
            "oidc_alice_agent_query",
            "oidc_frank_legacy_query"
    ])
    Map manifest = fixture.manifest
    Map<String, Map> tokensByName = fixture.tokensByName

    assertEquals("grafana-doris-plugin", tokensByName["oidc_alice_grafana_query"].clientId)
    assertTrue(((List) tokensByName["oidc_alice_grafana_query"].claims.aud).contains("doris"))
    assertTrue(tokensByName["oidc_alice_grafana_query"].claims.scope.toString().contains("doris.query"))
    assertEquals(
            ["doris_analyst", "doris_dashboard_readonly"],
            (List) tokensByName["oidc_alice_grafana_query"].claims.doris_groups)
    assertEquals(
            "doris_analyst,doris_dashboard_readonly",
            tokensByName["oidc_frank_legacy_query"].claims.doris_groups)

    Map liveLogin = oidc.prepareLiveLogin(suiteName, suiteName + "_oidc")
    String integrationName = liveLogin.integrationName.toString()
    String roleMappingName = suiteName + "_mapping"
    String scopeReaderRoleName = suiteName + "_scope_reader"
    String groupReaderRoleName = suiteName + "_group_reader"
    List<Map> roleMappingRules = [
            [cel: 'has_scope("doris.query")', role: scopeReaderRoleName],
            [cel: 'has_group("doris_analyst")', role: groupReaderRoleName]
    ]

    oidc.dropRoleMappingsOnIntegration(integrationName)
    try_sql("DROP ROLE MAPPING IF EXISTS ${roleMappingName}")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_scope_reader")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_group_reader")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_scope_table")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_group_table")

    sql """CREATE ROLE ${suiteName}_scope_reader"""
    sql """CREATE ROLE ${suiteName}_group_reader"""

    sql """
        CREATE TABLE ${suiteName}_scope_table (
            k INT
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${suiteName}_scope_table VALUES (11)"""

    sql """
        CREATE TABLE ${suiteName}_group_table (
            k INT
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${suiteName}_group_table VALUES (22)"""

    sql """GRANT SELECT_PRIV ON ${dbName}.${suiteName}_scope_table TO ROLE ${suiteName}_scope_reader"""
    sql """GRANT SELECT_PRIV ON ${dbName}.${suiteName}_group_table TO ROLE ${suiteName}_group_reader"""

    oidc.createOidcIntegration(integrationName, manifest, [
            requiredScopes: ["doris.query"],
            allowedClientIds: ["grafana-doris-plugin", "legacy-grafana-plugin"],
            extraClaims: ["tenant", "department"],
            comment: "OIDC access token regression integration"
    ])
    oidc.createRoleMapping(
            roleMappingName,
            integrationName,
            roleMappingRules,
            "OIDC access token regression mapping")
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
    assertTrue(integrationRows[0][2].contains(
            "\"oidc.allowed_client_ids\" = \"grafana-doris-plugin,legacy-grafana-plugin\""))
    assertTrue(integrationRows[0][2].contains("\"oidc.groups_claim\" = \"doris_groups\""))

    def roleMappingRows = sql """
        SELECT
            NAME,
            INTEGRATION_NAME,
            RULES,
            COMMENT,
            CREATE_USER,
            CREATE_TIME,
            ALTER_USER,
            MODIFY_TIME
        FROM information_schema.role_mappings
        WHERE NAME = '${roleMappingName}'
        ORDER BY NAME
    """
    assertEquals(1, roleMappingRows.size())
    assertEquals(8, roleMappingRows[0].size())
    assertEquals(roleMappingName, roleMappingRows[0][0])
    assertEquals(integrationName, roleMappingRows[0][1])
    assertEquals(oidc.expectedRoleMappingRules(roleMappingRules), roleMappingRows[0][2])
    assertEquals("OIDC access token regression mapping", roleMappingRows[0][3])
    assertTrue(roleMappingRows[0][4] != null && roleMappingRows[0][4].length() > 0)
    assertTrue(roleMappingRows[0][5] != null && roleMappingRows[0][5].length() > 0)
    assertTrue(roleMappingRows[0][6] != null && roleMappingRows[0][6].length() > 0)
    assertTrue(roleMappingRows[0][7] != null && roleMappingRows[0][7].length() > 0)

    if (!(liveLogin.mysqlshAvailable as boolean)) {
        return
    }

    oidc.withOidcAuthenticationChain(liveLogin) {
        Map aliceScopeTableResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_scope_table ORDER BY k")
        assertEquals(0, aliceScopeTableResult.status)
        assertTrue(aliceScopeTableResult.output.contains("11"))

        Map aliceGroupTableResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_group_table ORDER BY k")
        assertEquals(0, aliceGroupTableResult.status)
        assertTrue(aliceGroupTableResult.output.contains("22"))

        Map missingScopeResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                tokensByName["oidc_alice_grafana_missing_scope"].path.toString(),
                "SELECT 1")
        assertTrue(missingScopeResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                missingScopeResult.output,
                "OIDC access token scope is missing a required value"))

        Map deniedClientResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                tokensByName["oidc_alice_agent_query"].path.toString(),
                "SELECT 1")
        assertTrue(deniedClientResult.status != 0)
        assertTrue(oidc.containsAuthenticationFailure(
                deniedClientResult.output,
                "OIDC access token client id is not allowed"))

        Map legacyScopeTableResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_scope_table ORDER BY k")
        assertEquals(0, legacyScopeTableResult.status)
        assertTrue(legacyScopeTableResult.output.contains("11"))

        Map legacyGroupTableResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_group_table ORDER BY k")
        assertTrue(legacyGroupTableResult.status != 0)
    }
}
