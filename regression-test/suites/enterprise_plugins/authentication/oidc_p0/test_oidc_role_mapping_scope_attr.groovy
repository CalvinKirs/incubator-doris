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

suite("test_oidc_role_mapping_scope_attr", "docker,p0") {
    if (context.config.otherConfigs.get("enableEnterprisePluginOidcTest") != "true") {
        logger.info("enableEnterprisePluginOidcTest is not true, skip synced OIDC suite")
        return
    }

    String suiteName = "test_oidc_role_mapping_scope_attr"
    String dbName = context.config.getDbNameByFile(context.file)
    String manifestPath = System.getenv("DORIS_OIDC_MANIFEST_PATH")
    String externalEnvIp = context.config.otherConfigs?.get("externalEnvIp")?.toString()?.trim()
    String fixtureStartCommand = "bash tools/run-oidc-fixture.sh up" +
            (externalEnvIp ? " --external-host ${externalEnvIp}" : "")

    assertTrue(
            manifestPath != null && !manifestPath.trim().isEmpty(),
            "DORIS_OIDC_MANIFEST_PATH is required. Start the plugin-owned OIDC fixture first with "
                    + "'${fixtureStartCommand}' in enterprise-plugins and export "
                    + "DORIS_OIDC_MANIFEST_PATH to the generated docker/oidc/tokens/manifest.json")

    File manifestFile = new File(manifestPath)
    assertTrue(
            manifestFile.exists(),
            "OIDC token manifest not found at ${manifestFile}. "
                    + "Start the plugin-owned OIDC fixture first with "
                    + "'${fixtureStartCommand}' in enterprise-plugins.")

    Map manifest = (Map) new JsonSlurper().parse(manifestFile)
    assertEquals("doris", manifest.audience)
    assertEquals(10800, ((Number) manifest.tokenTtlSeconds).intValue())
    assertEquals("doris_groups", manifest.groupsClaim)

    Map<String, Map> tokensByName = ((List<Map>) manifest.tokens).collectEntries { Map token ->
        [(token.name.toString()): token]
    }
    Closure<String> resolveTokenPath = { Map token ->
        List<File> candidates = []
        String relativePath = token.relativePath?.toString()?.trim()
        if (relativePath) {
            candidates.add(new File(manifestFile.parentFile, relativePath))
        }

        String path = token.path?.toString()?.trim()
        if (path) {
            File configuredPath = new File(path)
            candidates.add(configuredPath.isAbsolute()
                    ? configuredPath
                    : new File(manifestFile.parentFile, path))
            if (configuredPath.name) {
                candidates.add(new File(manifestFile.parentFile, configuredPath.name))
            }
        }

        File existing = candidates.find { File candidate -> candidate.exists() }
        existing != null ? existing.canonicalPath : (candidates.isEmpty() ? "" : candidates[0].path)
    }
    [
            "oidc_alice_grafana_query",
            "oidc_bob_grafana_query",
            "oidc_carol_grafana_query",
            "oidc_erin_grafana_query",
            "oidc_frank_legacy_query"
    ].each { String tokenName ->
        assertTrue(tokensByName.containsKey(tokenName), "Missing token case in manifest: ${tokenName}")
        String tokenPath = resolveTokenPath(tokensByName[tokenName])
        tokensByName[tokenName].path = tokenPath
        assertTrue(new File(tokenPath).exists(), "Generated token file does not exist: ${tokenPath}")
    }

    assertTrue(tokensByName["oidc_alice_grafana_query"].claims.scope.toString().contains("doris.profile"))
    assertEquals("acme", tokensByName["oidc_alice_grafana_query"].claims.tenant)
    assertEquals("finance", tokensByName["oidc_alice_grafana_query"].claims.department)
    assertEquals("acme", tokensByName["oidc_bob_grafana_query"].claims.tenant)
    assertEquals("dashboard", tokensByName["oidc_carol_grafana_query"].claims.department)
    assertEquals("globex", tokensByName["oidc_erin_grafana_query"].claims.tenant)
    assertEquals("legacy-grafana-plugin", tokensByName["oidc_frank_legacy_query"].clientId)

    Map authenticationChainConfig = sql_return_maparray(
            "SHOW FRONTEND CONFIG LIKE 'authentication_chain'")[0]
    String previousAuthenticationChain = authenticationChainConfig.Value.toString()
    boolean authenticationChainMutable = Boolean.parseBoolean(
            authenticationChainConfig.IsMutable.toString())
    List<String> configuredAuthenticationChain = previousAuthenticationChain.split(",")
            .collect { it.trim() }
            .findAll { !it.isEmpty() }
    String integrationName = suiteName + "_oidc"
    if (!authenticationChainMutable) {
        if (configuredAuthenticationChain.any { it == integrationName }) {
            logger.info("Reuse immutable authentication_chain integration ${integrationName} for suite ${suiteName}")
        } else if (configuredAuthenticationChain.size() == 1) {
            integrationName = configuredAuthenticationChain[0]
            logger.info("Reuse only immutable authentication_chain integration ${integrationName} for suite ${suiteName}")
        } else {
            assertTrue(
                    false,
                    "Running Doris FE reports authentication_chain as immutable and does not include "
                            + "${suiteName}_oidc. Preconfigure authentication_chain=${suiteName}_oidc "
                            + "before starting FE, or expose a single reusable integration name. "
                            + "Current authentication_chain='${previousAuthenticationChain}'")
        }
    }

    def existingRoleMappingsOnIntegration = sql """
        SELECT NAME
        FROM information_schema.role_mappings
        WHERE INTEGRATION_NAME = '${integrationName}'
        ORDER BY NAME
    """
    existingRoleMappingsOnIntegration.each { List row ->
        try_sql("DROP ROLE MAPPING IF EXISTS ${row[0].toString()}")
    }
    try_sql("DROP ROLE MAPPING IF EXISTS ${suiteName}_mapping")
    try_sql("DROP AUTHENTICATION INTEGRATION IF EXISTS ${integrationName}")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_profile_reader")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_finance_reader")
    try_sql("DROP ROLE IF EXISTS ${suiteName}_legacy_reader")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_profile_table")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_finance_table")
    try_sql("DROP TABLE IF EXISTS ${suiteName}_legacy_table")

    sql """CREATE ROLE ${suiteName}_profile_reader"""
    sql """CREATE ROLE ${suiteName}_finance_reader"""
    sql """CREATE ROLE ${suiteName}_legacy_reader"""

    sql """
        CREATE TABLE ${suiteName}_profile_table (
            k INT
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${suiteName}_profile_table VALUES (11)"""

    sql """
        CREATE TABLE ${suiteName}_finance_table (
            k INT
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${suiteName}_finance_table VALUES (22)"""

    sql """
        CREATE TABLE ${suiteName}_legacy_table (
            k INT
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO ${suiteName}_legacy_table VALUES (33)"""

    sql """GRANT SELECT_PRIV ON ${dbName}.${suiteName}_profile_table TO ROLE ${suiteName}_profile_reader"""
    sql """GRANT SELECT_PRIV ON ${dbName}.${suiteName}_finance_table TO ROLE ${suiteName}_finance_reader"""
    sql """GRANT SELECT_PRIV ON ${dbName}.${suiteName}_legacy_table TO ROLE ${suiteName}_legacy_reader"""

    sql """
        CREATE AUTHENTICATION INTEGRATION ${integrationName}
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
        COMMENT 'OIDC scope and attribute role mapping integration'
    """

    sql """
        CREATE ROLE MAPPING ${suiteName}_mapping
        ON AUTHENTICATION INTEGRATION ${integrationName}
        RULE ( USING CEL 'has_scope("doris.profile")' GRANT ROLE ${suiteName}_profile_reader ),
        RULE (
            USING CEL 'attr("tenant") == "acme" && attr("department") == "finance"'
            GRANT ROLE ${suiteName}_finance_reader
        ),
        RULE (
            USING CEL 'attr("oauth.client_id") == "legacy-grafana-plugin"'
            GRANT ROLE ${suiteName}_legacy_reader
        )
        COMMENT 'OIDC scope and attribute role mapping'
    """
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
    assertTrue(integrationRows[0][2].contains("\"oidc.extra_claims\" = \"tenant,department\""))

    String roleMappingName = suiteName + "_mapping"
    String profileRoleName = suiteName + "_profile_reader"
    String financeRoleName = suiteName + "_finance_reader"
    String legacyRoleName = suiteName + "_legacy_reader"
    String expectedRoleMappingRules = (
            """RULE (USING CEL 'has_scope("doris.profile")' GRANT ROLE ${profileRoleName}); """
            + """RULE (USING CEL 'attr("tenant") == "acme" && attr("department") == "finance"' """
            + """GRANT ROLE ${financeRoleName}); """
            + """RULE (USING CEL 'attr("oauth.client_id") == "legacy-grafana-plugin"' """
            + """GRANT ROLE ${legacyRoleName})"""
    ).toString()
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
    assertEquals("OIDC scope and attribute role mapping", roleMappingRows[0][3])

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
    def mysqlEndpointMatcher = context.config.jdbcUrl.toString() =~ /^jdbc:mysql:\/\/(\[[^\]]+\]|[^:\/?,]+)(?::(\d+))?/
    assertTrue(
            mysqlEndpointMatcher.find(),
            "Cannot parse FE MySQL endpoint from jdbcUrl: ${context.config.jdbcUrl}")
    String mysqlHost = mysqlEndpointMatcher.group(1)
    String mysqlPort = mysqlEndpointMatcher.group(2) ?: "9030"
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
                -h ${shQuote(mysqlHost)} -P${mysqlPort} -u ${shQuote(userName)} \\
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
    Closure<Boolean> isPermissionDenied = { String output ->
        output.contains("Permission denied")
    }

    try {
        if (shouldCopyPlugin) {
            feHosts.each { String feHost ->
                sshExec("root", feHost, "rm -rf ${remotePluginDir} && mkdir -p ${authenticationPluginRoot}")
                scpFiles("root", feHost, pluginDir, authenticationPluginRoot, false)
            }
        }

        if (authenticationChainMutable) {
            sql """ADMIN SET ALL FRONTENDS CONFIG ('authentication_chain' = '${integrationName}')"""
        } else {
            assertTrue(configuredAuthenticationChain.any { it == integrationName })
        }

        Map aliceProfileResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_profile_table ORDER BY k")
        assertEquals(0, aliceProfileResult.status)
        assertTrue(aliceProfileResult.output.contains("11"))

        Map aliceFinanceResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_finance_table ORDER BY k")
        assertEquals(0, aliceFinanceResult.status)
        assertTrue(aliceFinanceResult.output.contains("22"))

        Map aliceLegacyResult = runMysqlsh(
                "oidc_alice",
                tokensByName["oidc_alice_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_legacy_table ORDER BY k")
        assertTrue(aliceLegacyResult.status != 0)
        assertTrue(isPermissionDenied(aliceLegacyResult.output))

        Map bobFinanceResult = runMysqlsh(
                "oidc_bob",
                tokensByName["oidc_bob_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_finance_table ORDER BY k")
        assertEquals(0, bobFinanceResult.status)
        assertTrue(bobFinanceResult.output.contains("22"))

        Map bobProfileResult = runMysqlsh(
                "oidc_bob",
                tokensByName["oidc_bob_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_profile_table ORDER BY k")
        assertTrue(bobProfileResult.status != 0)
        assertTrue(isPermissionDenied(bobProfileResult.output))

        Map carolLoginResult = runMysqlsh(
                "oidc_carol",
                tokensByName["oidc_carol_grafana_query"].path.toString(),
                "SELECT 1")
        assertEquals(0, carolLoginResult.status)

        Map carolFinanceResult = runMysqlsh(
                "oidc_carol",
                tokensByName["oidc_carol_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_finance_table ORDER BY k")
        assertTrue(carolFinanceResult.status != 0)
        assertTrue(isPermissionDenied(carolFinanceResult.output))

        Map erinFinanceResult = runMysqlsh(
                "oidc_erin",
                tokensByName["oidc_erin_grafana_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_finance_table ORDER BY k")
        assertTrue(erinFinanceResult.status != 0)
        assertTrue(isPermissionDenied(erinFinanceResult.output))

        Map frankFinanceResult = runMysqlsh(
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_finance_table ORDER BY k")
        assertEquals(0, frankFinanceResult.status)
        assertTrue(frankFinanceResult.output.contains("22"))

        Map frankLegacyResult = runMysqlsh(
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_legacy_table ORDER BY k")
        assertEquals(0, frankLegacyResult.status)
        assertTrue(frankLegacyResult.output.contains("33"))

        Map frankProfileResult = runMysqlsh(
                "oidc_frank",
                tokensByName["oidc_frank_legacy_query"].path.toString(),
                "SELECT k FROM ${dbName}.${suiteName}_profile_table ORDER BY k")
        assertTrue(frankProfileResult.status != 0)
        assertTrue(isPermissionDenied(frankProfileResult.output))
    } finally {
        if (authenticationChainMutable) {
            try_sql("""ADMIN SET ALL FRONTENDS CONFIG ('authentication_chain' = '${previousAuthenticationChain}')""")
        }
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
