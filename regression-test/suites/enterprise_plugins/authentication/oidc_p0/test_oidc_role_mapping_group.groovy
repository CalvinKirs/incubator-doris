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

suite("test_oidc_role_mapping_group", "docker,p0") {
    if (context.config.otherConfigs.get("enableEnterprisePluginOidcTest") != "true") {
        logger.info("enableEnterprisePluginOidcTest is not true, skip synced OIDC suite")
        return
    }

    String suiteName = "test_oidc_role_mapping_group"
    String dbName = context.config.getDbNameByFile(context.file)
    Class helperClass = new GroovyShell(getClass().classLoader)
            .evaluate(new File(context.file.parentFile, "test_oidc_common.groovy")) as Class
    def oidc = helperClass.newInstance(delegate)

    Map fixture = oidc.prepareBaseOidcFixture([
            "oidc_alice_grafana_query",
            "oidc_bob_grafana_query",
            "oidc_carol_grafana_query",
            "oidc_dave_grafana_query"
    ])
    Map manifest = fixture.manifest
    Map<String, Map> tokensByName = fixture.tokensByName

    assertEquals(
            ["doris_analyst", "doris_dashboard_readonly"],
            (List) tokensByName["oidc_alice_grafana_query"].claims.doris_groups)
    assertEquals(
            ["doris_analyst"],
            (List) tokensByName["oidc_bob_grafana_query"].claims.doris_groups)
    assertEquals(
            ["doris_dashboard_readonly"],
            (List) tokensByName["oidc_carol_grafana_query"].claims.doris_groups)
    assertEquals(null, tokensByName["oidc_dave_grafana_query"].claims.doris_groups)

    Map liveLogin = oidc.prepareLiveLogin(suiteName, suiteName + "_oidc")
    String integrationName = liveLogin.integrationName.toString()

    oidc.dropRoleMappingsOnIntegration(integrationName)
    try_sql("DROP ROLE MAPPING IF EXISTS ${suiteName}_mapping")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_analyst_reader")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_dashboard_reader")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_analyst_table")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_dashboard_table")

    oidc.createRole("${suiteName}_analyst_reader")
    oidc.createRole("${suiteName}_dashboard_reader")
    oidc.createSingleIntTable("${suiteName}_analyst_table", 11)
    oidc.createSingleIntTable("${suiteName}_dashboard_table", 22)
    oidc.grantSelect(dbName, "${suiteName}_analyst_table", "${suiteName}_analyst_reader")
    oidc.grantSelect(dbName, "${suiteName}_dashboard_table", "${suiteName}_dashboard_reader")
    oidc.createOidcIntegration(integrationName, manifest, [
            requiredScopes: ["doris.query"],
            allowedClientIds: ["grafana-doris-plugin"],
            comment: "OIDC group role mapping integration"
    ])

    List<Map> roleMappingRules = [
            [cel: 'has_group("doris_analyst")', role: "${suiteName}_analyst_reader"],
            [cel: 'has_group("doris_dashboard_readonly")', role: "${suiteName}_dashboard_reader"]
    ]
    oidc.createRoleMapping(
            "${suiteName}_mapping",
            integrationName,
            roleMappingRules,
            "OIDC group role mapping")
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
    assertTrue(integrationRows[0][2].contains("\"oidc.allowed_client_ids\" = \"grafana-doris-plugin\""))
    assertTrue(integrationRows[0][2].contains("\"oidc.groups_claim\" = \"doris_groups\""))

    String roleMappingName = suiteName + "_mapping"
    String expectedRoleMappingRules = oidc.expectedRoleMappingRules(roleMappingRules)
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
    assertEquals(roleMappingName, roleMappingRows[0][0])
    assertEquals(integrationName, roleMappingRows[0][1])
    assertEquals(expectedRoleMappingRules, roleMappingRows[0][2])
    assertEquals("OIDC group role mapping", roleMappingRows[0][3])

    if (!(liveLogin.mysqlshAvailable as boolean)) {
        return
    }

    oidc.withOidcAuthenticationChain(liveLogin) {
        Map aliceAnalystResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_analyst_table ORDER BY k")
        assertEquals(0, aliceAnalystResult.status)
        assertTrue(aliceAnalystResult.output.contains("11"))

        Map aliceDashboardResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_dashboard_table ORDER BY k")
        assertEquals(0, aliceDashboardResult.status)
        assertTrue(aliceDashboardResult.output.contains("22"))

        Map bobAnalystResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_bob",
                tokensByName["oidc_bob_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_analyst_table ORDER BY k")
        assertEquals(0, bobAnalystResult.status)
        assertTrue(bobAnalystResult.output.contains("11"))

        Map bobDashboardResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_bob",
                tokensByName["oidc_bob_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_dashboard_table ORDER BY k")
        assertTrue(bobDashboardResult.status != 0)
        assertTrue(oidc.isPermissionDenied(bobDashboardResult.output))

        Map carolDashboardResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_carol",
                tokensByName["oidc_carol_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_dashboard_table ORDER BY k")
        assertEquals(0, carolDashboardResult.status)
        assertTrue(carolDashboardResult.output.contains("22"))

        Map carolAnalystResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_carol",
                tokensByName["oidc_carol_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_analyst_table ORDER BY k")
        assertTrue(carolAnalystResult.status != 0)
        assertTrue(oidc.isPermissionDenied(carolAnalystResult.output))

        Map daveLoginResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_dave",
                tokensByName["oidc_dave_grafana_query"].path.toString(),
                "SELECT 1")
        assertEquals(0, daveLoginResult.status)

        Map daveAnalystResult = oidc.runOidcMysqlsh(
                liveLogin,
                "oidc_dave",
                tokensByName["oidc_dave_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_analyst_table ORDER BY k")
        assertTrue(daveAnalystResult.status != 0)
        assertTrue(oidc.isPermissionDenied(daveAnalystResult.output))
    }
}
