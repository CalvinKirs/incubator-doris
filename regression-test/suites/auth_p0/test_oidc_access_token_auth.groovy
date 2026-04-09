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

import groovy.json.JsonSlurper

suite("test_oidc_access_token_auth", "p0,auth") {
    String suiteName = "test_oidc_access_token_auth"
    String dbName = context.config.getDbNameByFile(context.file)
    String repoRoot = new File(context.config.suitePath).parentFile.parentFile.canonicalPath
    File manifestFile = new File(
            "${repoRoot}/docker/thirdparties/docker-compose/oidc/tokens/manifest.json")

    assertTrue(
            manifestFile.exists(),
            "OIDC token manifest not found at ${manifestFile}. "
                    + "Start the OIDC docker fixture first with "
                    + "'bash docker/thirdparties/run-thirdparties-docker.sh -c oidc'.")

    Map manifest = (Map) new JsonSlurper().parse(manifestFile)
    assertEquals("doris", manifest.audience)
    assertEquals(10800, ((Number) manifest.tokenTtlSeconds).intValue())
    assertEquals("doris_groups", manifest.groupsClaim)

    Map<String, Map> tokensByName = ((List<Map>) manifest.tokens).collectEntries { Map token ->
        [(token.name.toString()): token]
    }
    [
            "oidc_alice_grafana_query",
            "oidc_alice_grafana_missing_scope",
            "oidc_alice_agent_query",
            "oidc_frank_legacy_query"
    ].each { String tokenName ->
        assertTrue(tokensByName.containsKey(tokenName), "Missing token case in manifest: ${tokenName}")
        assertTrue(new File(tokensByName[tokenName].path.toString()).exists(),
                "Generated token file does not exist: ${tokensByName[tokenName].path}")
    }

    assertEquals("grafana-doris-plugin", tokensByName["oidc_alice_grafana_query"].clientId)
    assertTrue(((List) tokensByName["oidc_alice_grafana_query"].claims.aud).contains("doris"))
    assertTrue(tokensByName["oidc_alice_grafana_query"].claims.scope.toString().contains("doris.query"))
    assertEquals(
            ["doris_analyst", "doris_dashboard_readonly"],
            (List) tokensByName["oidc_alice_grafana_query"].claims.doris_groups)
    assertEquals(
            "doris_analyst,doris_dashboard_readonly",
            tokensByName["oidc_frank_legacy_query"].claims.doris_groups)

    try_sql("DROP ROLE MAPPING IF EXISTS ${suiteName}_mapping")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${suiteName}_oidc")
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

    sql """
        CREATE AUTHENTICATION INTEGRATION ${suiteName}_oidc
        PROPERTIES (
            'type' = 'oidc',
            'enable_jit_user' = 'true',
            'oidc.issuer' = '${manifest.issuer}',
            'oidc.jwks_uri' = '${manifest.jwksUri}',
            'oidc.allowed_audiences' = '${manifest.audience}',
            'oidc.required_scopes' = 'doris.query',
            'oidc.allowed_client_ids' = 'grafana-doris-plugin,legacy-grafana-plugin',
            'oidc.username_claim' = 'preferred_username',
            'oidc.subject_claim' = 'sub',
            'oidc.groups_claim' = '${manifest.groupsClaim}',
            'oidc.extra_claims' = 'tenant,department',
            'oidc.allowed_algorithms' = 'RS256'
        )
        COMMENT 'OIDC access token regression integration'
    """

    sql """
        CREATE ROLE MAPPING ${suiteName}_mapping
        ON AUTHENTICATION INTEGRATION ${suiteName}_oidc
        RULE ( USING CEL 'has_scope("doris.query")' GRANT ROLE ${suiteName}_scope_reader ),
        RULE ( USING CEL 'has_group("doris_analyst")' GRANT ROLE ${suiteName}_group_reader )
        COMMENT 'OIDC access token regression mapping'
    """
    sql "sync"

    def integrationRows = sql """
        SELECT
            NAME,
            TYPE,
            PROPERTIES,
            COMMENT
        FROM information_schema.authentication_integrations
        WHERE NAME = '${suiteName}_oidc'
        ORDER BY NAME
    """
    assertEquals(1, integrationRows.size())
    assertEquals("oidc", integrationRows[0][1])
    assertTrue(integrationRows[0][2].contains("\"oidc.required_scopes\" = \"doris.query\""))
    assertTrue(integrationRows[0][2].contains(
            "\"oidc.allowed_client_ids\" = \"grafana-doris-plugin,legacy-grafana-plugin\""))
    assertTrue(integrationRows[0][2].contains("\"oidc.groups_claim\" = \"doris_groups\""))

    String pluginDir = System.getenv("DORIS_OIDC_PLUGIN_DIR")
    String mysqlshBin = System.getenv("MYSQLSH_BIN") ?: "mysqlsh"
    boolean mysqlshAvailable = "yes".equals(cmd(
            "if command -v ${mysqlshBin} >/dev/null 2>&1; then echo yes; else echo no; fi").trim())
    boolean shouldCopyPlugin = pluginDir != null && !pluginDir.trim().isEmpty()
    if (!mysqlshAvailable) {
        logger.info("Skip live OIDC login checks because mysqlsh is unavailable")
        return
    }
    if (shouldCopyPlugin) {
        assertTrue(new File(pluginDir).isDirectory(),
                "DORIS_OIDC_PLUGIN_DIR does not exist or is not a directory: ${pluginDir}")
    }

    String previousAuthenticationChain = sql_return_maparray(
            "SHOW FRONTEND CONFIG LIKE 'authentication_chain'")[0].Value.toString()
    List<String> feHosts = sql_return_maparray("SHOW FRONTENDS").collect { it.Host.toString() }
    String authenticationPluginRoot = null
    String remotePluginDir = null
    if (shouldCopyPlugin) {
        authenticationPluginRoot = sql_return_maparray(
                "SHOW FRONTEND CONFIG LIKE 'authentication_plugins_dir'")[0].Value.toString()
        String pluginBaseName = new File(pluginDir).name
        remotePluginDir = "${authenticationPluginRoot}/${pluginBaseName}"
    } else {
        logger.info("Assume OIDC authentication plugin is already installed on FE hosts")
    }
    String mysqlPort = ((context.config.jdbcUrl =~ /jdbc:mysql:\/\/[^:\/]+:(\d+)/)[0][1]).toString()
    Closure<String> shQuote = { String value ->
        "'" + value.replace("'", "'\"'\"'") + "'"
    }
    Closure<Map<String, Object>> runMysqlsh = { String userName, String tokenFilePath, String query ->
        String mysqlshHome = "/tmp/${suiteName}_mysqlsh_home"
        String mysqlshOutputPath = "/tmp/${suiteName}_mysqlsh.out"
        String mysqlshStatusPath = "/tmp/${suiteName}_mysqlsh.status"
        String wrappedCommand = """
            set +e
            rm -rf ${shQuote(mysqlshHome)}
            rm -f ${shQuote(mysqlshOutputPath)} ${shQuote(mysqlshStatusPath)}
            mkdir -p ${shQuote(mysqlshHome)}
            HOME=${shQuote(mysqlshHome)} ${shQuote(mysqlshBin)} \\
                --sql --sqlc --ssl-mode=REQUIRED \\
                -h127.0.0.1 -P${mysqlPort} -u ${shQuote(userName)} \\
                --log-file=${shQuote("${mysqlshHome}/mysqlsh.log")} \\
                --authentication-openid-connect-client-id-token-file=${shQuote(tokenFilePath)} \\
                -e ${shQuote(query)} >${shQuote(mysqlshOutputPath)} 2>&1
            STATUS=\$?
            printf '%s' "\${STATUS}" >${shQuote(mysqlshStatusPath)}
            cat ${shQuote(mysqlshOutputPath)}
        """.stripIndent()
        cmd(wrappedCommand)
        File statusFile = new File(mysqlshStatusPath)
        File outputFile = new File(mysqlshOutputPath)
        assertTrue(statusFile.exists(), "mysqlsh status file is missing: ${mysqlshStatusPath}")
        assertTrue(outputFile.exists(), "mysqlsh output file is missing: ${mysqlshOutputPath}")
        [
                status: Integer.parseInt(statusFile.text.trim()),
                output: outputFile.text
        ]
    }

    try {
        if (shouldCopyPlugin) {
            feHosts.each { String feHost ->
                sshExec("root", feHost, "rm -rf ${remotePluginDir} && mkdir -p ${authenticationPluginRoot}")
                scpFiles("root", feHost, pluginDir, authenticationPluginRoot, false)
            }
        }

        sql """ADMIN SET ALL FRONTENDS CONFIG ('authentication_chain' = '${suiteName}_oidc')"""

        Map aliceScopeTableResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_scope_table ORDER BY k")
        assertEquals(0, aliceScopeTableResult.status)
        assertTrue(aliceScopeTableResult.output.contains("11"))

        Map aliceGroupTableResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_group_table ORDER BY k")
        assertEquals(0, aliceGroupTableResult.status)
        assertTrue(aliceGroupTableResult.output.contains("22"))

        Map missingScopeResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_grafana_missing_scope"].path.toString(),
                "SELECT 1")
        assertTrue(missingScopeResult.status != 0)
        assertTrue(missingScopeResult.output.contains("OIDC access token scope is missing a required value"))

        Map deniedClientResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_agent_query"].path.toString(),
                "SELECT 1")
        assertTrue(deniedClientResult.status != 0)
        assertTrue(deniedClientResult.output.contains("OIDC access token client id is not allowed"))

        Map legacyScopeTableResult = runMysqlsh(
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_scope_table ORDER BY k")
        assertEquals(0, legacyScopeTableResult.status)
        assertTrue(legacyScopeTableResult.output.contains("11"))

        Map legacyGroupTableResult = runMysqlsh(
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_group_table ORDER BY k")
        assertTrue(legacyGroupTableResult.status != 0)
    } finally {
        try_sql("""ADMIN SET ALL FRONTENDS CONFIG ('authentication_chain' = '${previousAuthenticationChain}')""")
        if (shouldCopyPlugin) {
            feHosts.each { String feHost ->
                try {
                    sshExec("root", feHost, "rm -rf ${remotePluginDir}", false)
                } catch (Exception e) {
                    logger.warn("Failed to clean up OIDC plugin directory on ${feHost}: ${e.message}")
                }
            }
        }
    }
}
