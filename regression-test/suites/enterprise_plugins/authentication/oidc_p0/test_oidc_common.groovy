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
import org.junit.jupiter.api.Assertions
import java.net.URI

class OidcSuiteHelper {
    private final def script

    OidcSuiteHelper(def script) {
        this.script = script
    }

    Map prepareBaseOidcFixture(List<String> requiredTokenNames) {
        String manifestPath = System.getenv("DORIS_OIDC_MANIFEST_PATH")
        if (manifestPath == null || manifestPath.trim().isEmpty()) {
            String repoRoot = new File(script.context.config.suitePath).parentFile.parentFile.canonicalPath
            manifestPath = "${repoRoot}/docker/oidc/tokens/manifest.json"
        }

        File manifestFile = new File(manifestPath)
        String fixtureStartCommand = oidcFixtureStartCommand()
        Assertions.assertTrue(
                manifestFile.exists(),
                ("OIDC token manifest not found at ${manifestFile}. "
                        + "Start the plugin-owned OIDC fixture first with "
                        + "'${fixtureStartCommand}' in enterprise-plugins, or set "
                        + "DORIS_OIDC_MANIFEST_PATH to the generated manifest.").toString())

        Map manifest = (Map) new JsonSlurper().parse(manifestFile)
        Assertions.assertEquals("doris", manifest.audience)
        Assertions.assertEquals(10800, ((Number) manifest.tokenTtlSeconds).intValue())
        Assertions.assertEquals("doris_groups", manifest.groupsClaim)

        Map<String, Map> tokensByName = ((List<Map>) manifest.tokens).collectEntries { Map token ->
            [(token.name.toString()): token]
        }
        requiredTokenNames.each { String tokenName ->
            Assertions.assertTrue(
                    tokensByName.containsKey(tokenName),
                    "Missing token case in manifest: ${tokenName}".toString())
            String tokenPath = resolveTokenPath(manifestFile, tokensByName[tokenName])
            tokensByName[tokenName].path = tokenPath
            Assertions.assertTrue(
                    new File(tokenPath).exists(),
                    "Generated token file does not exist: ${tokenPath}".toString())
        }

        [
                manifest: manifest,
                manifestPath: manifestFile.canonicalPath,
                tokensByName: tokensByName
        ]
    }

    void dropRoleMappingsOnIntegration(String integrationName) {
        def existingRoleMappings = script.sql """
            SELECT NAME
            FROM information_schema.role_mappings
            WHERE INTEGRATION_NAME = '${integrationName}'
            ORDER BY NAME
        """
        existingRoleMappings.each { List row ->
            script.try_sql("DROP ROLE MAPPING IF EXISTS ${row[0].toString()}")
        }
    }

    void createRole(String roleName) {
        script.sql """CREATE ROLE ${roleName}"""
    }

    void createSingleIntTable(String tableName, int value) {
        script.sql """
            CREATE TABLE ${tableName} (
                k INT
            )
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        script.sql """INSERT INTO ${tableName} VALUES (${value})"""
    }

    void grantSelect(String dbName, String tableName, String roleName) {
        script.sql """GRANT SELECT_PRIV ON ${dbName}.${tableName} TO ROLE ${roleName}"""
    }

    void createOidcIntegration(String integrationName, Map manifest, Map options) {
        Map<String, String> properties = [
                "type": "oidc",
                "enable_jit_user": options.getOrDefault("enableJitUser", "true").toString(),
                "oidc.issuer": manifest.issuer.toString(),
                "oidc.jwks_uri": manifest.jwksUri.toString(),
                "oidc.allowed_audiences": options.containsKey("allowedAudiences")
                        ? joinPropertyValue(options.allowedAudiences)
                        : manifest.audience.toString(),
                "oidc.username_claim": options.getOrDefault("usernameClaim", "preferred_username").toString(),
                "oidc.subject_claim": options.getOrDefault("subjectClaim", "sub").toString(),
                "oidc.groups_claim": options.getOrDefault("groupsClaim", manifest.groupsClaim.toString()).toString(),
                "oidc.allowed_algorithms": options.getOrDefault("allowedAlgorithms", "RS256").toString()
        ]
        if (options.containsKey("requiredScopes")) {
            properties["oidc.required_scopes"] = joinPropertyValue(options.requiredScopes)
        }
        if (options.containsKey("allowedClientIds")) {
            properties["oidc.allowed_client_ids"] = joinPropertyValue(options.allowedClientIds)
        }
        if (options.containsKey("extraClaims")) {
            properties["oidc.extra_claims"] = joinPropertyValue(options.extraClaims)
        }

        String propertySql = properties.collect { String key, String value ->
            "            '${escapeSql(key)}' = '${escapeSql(value)}'"
        }.join(",\n")

        script.sql """
            CREATE AUTHENTICATION INTEGRATION ${integrationName}
            PROPERTIES (
${propertySql}
            )
            COMMENT '${escapeSql(options.comment?.toString() ?: "")}'
        """
    }

    void createRoleMapping(String roleMappingName, String integrationName, List<Map> rules, String comment) {
        String rulesSql = rules.collect { Map rule ->
            "        RULE ( USING CEL '${escapeSql(rule.cel.toString())}' GRANT ROLE ${rule.role} )"
        }.join(",\n")

        script.sql """
            CREATE ROLE MAPPING ${roleMappingName}
            ON AUTHENTICATION INTEGRATION ${integrationName}
${rulesSql}
            COMMENT '${escapeSql(comment)}'
        """
    }

    String expectedRoleMappingRules(List<Map> rules) {
        rules.collect { Map rule ->
            "RULE (USING CEL '${rule.cel}' GRANT ROLE ${rule.role})"
        }.join("; ")
    }

    Map prepareLiveLogin(String suiteName, String preferredIntegrationName) {
        String mysqlshBin = System.getenv("MYSQLSH_BIN") ?: "mysqlsh"
        boolean mysqlshAvailable = runShellCommand(
                "command -v ${shQuote(mysqlshBin)} >/dev/null 2>&1").status == 0
        String pluginDir = System.getenv("DORIS_OIDC_PLUGIN_DIR")
        boolean shouldCopyPlugin = pluginDir != null && !pluginDir.trim().isEmpty()

        Map authenticationChainConfig = script.sql_return_maparray(
                "SHOW FRONTEND CONFIG LIKE 'authentication_chain'")[0]
        String previousAuthenticationChain = authenticationChainConfig.Value.toString()
        boolean authenticationChainMutable = Boolean.parseBoolean(authenticationChainConfig.IsMutable.toString())
        List<String> configuredAuthenticationChain = previousAuthenticationChain.split(",")
                .collect { it.trim() }
                .findAll { !it.isEmpty() }
        String integrationName = preferredIntegrationName
        if (!authenticationChainMutable) {
            if (configuredAuthenticationChain.any { it == preferredIntegrationName }) {
                script.logger.info(
                        "Reuse immutable authentication_chain integration ${preferredIntegrationName} for suite ${suiteName}")
            } else if (configuredAuthenticationChain.size() == 1) {
                integrationName = configuredAuthenticationChain[0]
                script.logger.info(
                        "Reuse only immutable authentication_chain integration ${integrationName} for suite ${suiteName}")
            } else {
                Assertions.fail(
                        ("Running Doris FE reports authentication_chain as immutable and does not include "
                                + "${preferredIntegrationName}. Preconfigure "
                                + "authentication_chain=${preferredIntegrationName} before starting FE, "
                                + "or expose a single reusable integration name. "
                                + "Current authentication_chain='${previousAuthenticationChain}'").toString())
            }
        }

        if (!mysqlshAvailable) {
            script.logger.info("Skip live OIDC login checks because mysqlsh is unavailable")
            return [
                    mysqlshAvailable: false,
                    integrationName: integrationName
            ]
        }

        if (shouldCopyPlugin) {
            Assertions.assertTrue(
                    new File(pluginDir).isDirectory(),
                    "DORIS_OIDC_PLUGIN_DIR does not exist or is not a directory: ${pluginDir}".toString())
        } else {
            script.logger.info("Assume OIDC authentication plugin is already installed on FE hosts")
        }

        List<String> feHosts = script.sql_return_maparray("SHOW FRONTENDS").collect { it.Host.toString() }
        String authenticationPluginRoot = null
        String remotePluginDir = null
        if (shouldCopyPlugin) {
            authenticationPluginRoot = script.sql_return_maparray(
                    "SHOW FRONTEND CONFIG LIKE 'authentication_plugins_dir'")[0].Value.toString()
            String pluginBaseName = new File(pluginDir).name
            remotePluginDir = "${authenticationPluginRoot}/${pluginBaseName}"
        }
        Map<String, String> mysqlEndpoint = parseJdbcMysqlEndpoint(script.context.config.jdbcUrl.toString())

        [
                mysqlshAvailable: true,
                mysqlshBin: mysqlshBin,
                mysqlHost: mysqlEndpoint.host,
                mysqlPort: mysqlEndpoint.port,
                pluginDir: pluginDir,
                shouldCopyPlugin: shouldCopyPlugin,
                feHosts: feHosts,
                authenticationPluginRoot: authenticationPluginRoot,
                remotePluginDir: remotePluginDir,
                previousAuthenticationChain: previousAuthenticationChain,
                authenticationChainMutable: authenticationChainMutable,
                configuredAuthenticationChain: configuredAuthenticationChain,
                integrationName: integrationName,
                suiteName: suiteName
        ]
    }

    void withOidcAuthenticationChain(Map liveLogin, Closure body) {
        if (!(liveLogin.mysqlshAvailable as boolean)) {
            return
        }

        try {
            if (liveLogin.shouldCopyPlugin as boolean) {
                liveLogin.feHosts.each { String feHost ->
                    script.sshExec(
                            "root",
                            feHost,
                            "rm -rf ${liveLogin.remotePluginDir} && mkdir -p ${liveLogin.authenticationPluginRoot}")
                    script.scpFiles(
                            "root",
                            feHost,
                            liveLogin.pluginDir.toString(),
                            liveLogin.authenticationPluginRoot.toString(),
                            false)
                }
            }

            if (liveLogin.authenticationChainMutable as boolean) {
                script.sql """ADMIN SET ALL FRONTENDS CONFIG ('authentication_chain' = '${liveLogin.integrationName}')"""
            } else {
                Assertions.assertTrue(
                        ((List<String>) liveLogin.configuredAuthenticationChain).any { it == liveLogin.integrationName })
            }

            body.call()
        } finally {
            if (liveLogin.authenticationChainMutable as boolean) {
                script.try_sql(
                        """ADMIN SET ALL FRONTENDS CONFIG ('authentication_chain' = '${liveLogin.previousAuthenticationChain}')""")
            }
            if (liveLogin.shouldCopyPlugin as boolean) {
                liveLogin.feHosts.each { String feHost ->
                    try {
                        script.sshExec("root", feHost, "rm -rf ${liveLogin.remotePluginDir}", false)
                    } catch (Exception e) {
                        script.logger.warn(
                                "Failed to clean up OIDC plugin directory on ${feHost}: ${e.message}")
                    }
                }
            }
        }
    }

    Map<String, Object> runOidcMysqlsh(Map liveLogin, String userName, String tokenFilePath, String query) {
        return runOidcMysqlsh(liveLogin, userName, tokenFilePath, query, "REQUIRED")
    }

    Map<String, Object> runOidcMysqlsh(Map liveLogin, String userName, String tokenFilePath, String query,
            String sslMode) {
        String mysqlshHome = "/tmp/${liveLogin.suiteName}_mysqlsh_home"
        String mysqlshOutputPath = "/tmp/${liveLogin.suiteName}_mysqlsh.out"
        String mysqlshStatusPath = "/tmp/${liveLogin.suiteName}_mysqlsh.status"
        String wrappedCommand = """
            set +e
            rm -rf ${shQuote(mysqlshHome)}
            rm -f ${shQuote(mysqlshOutputPath)} ${shQuote(mysqlshStatusPath)}
            mkdir -p ${shQuote(mysqlshHome)}
            HOME=${shQuote(mysqlshHome)} ${shQuote(liveLogin.mysqlshBin.toString())} \\
                --sql --sqlc --ssl-mode=${shQuote(sslMode)} \\
                -h ${shQuote(liveLogin.mysqlHost.toString())} -P${liveLogin.mysqlPort} -u ${shQuote(userName)} \\
                --log-file=${shQuote("${mysqlshHome}/mysqlsh.log")} \\
                --authentication-openid-connect-client-id-token-file=${shQuote(tokenFilePath)} \\
                -e ${shQuote(query)} >${shQuote(mysqlshOutputPath)} 2>&1
            STATUS=\$?
            printf '%s' "\${STATUS}" >${shQuote(mysqlshStatusPath)}
            cat ${shQuote(mysqlshOutputPath)}
        """.stripIndent()
        runShellCommand(wrappedCommand)

        File statusFile = new File(mysqlshStatusPath)
        File outputFile = new File(mysqlshOutputPath)
        Assertions.assertTrue(statusFile.exists(), "mysqlsh status file is missing: ${mysqlshStatusPath}".toString())
        Assertions.assertTrue(outputFile.exists(), "mysqlsh output file is missing: ${mysqlshOutputPath}".toString())

        [
                status: Integer.parseInt(statusFile.text.trim()),
                output: outputFile.text
        ]
    }

    boolean containsAuthenticationFailure(String output, String expectedDetail) {
        output.contains(expectedDetail) || containsGenericAccessDenied(output)
    }

    private static boolean containsGenericAccessDenied(String output) {
        // mysqlsh may hide the detailed OIDC rejection reason and only surface FE's generic 1045.
        return output.contains("Access denied for user")
                && (output.contains("MySQL Error 1045") || output.contains("ERROR 1045"))
    }

    boolean isPermissionDenied(String output) {
        output.contains("Permission denied")
    }

    private static String joinPropertyValue(Object value) {
        if (value instanceof Collection) {
            return ((Collection) value).collect { it.toString() }.join(",")
        }
        return value.toString()
    }

    private static String escapeSql(String value) {
        value.replace("'", "''")
    }

    private String oidcFixtureStartCommand() {
        String externalEnvIp = script.context.config.otherConfigs?.get("externalEnvIp")?.toString()?.trim()
        String externalHostOption = externalEnvIp ? " --external-host ${externalEnvIp}" : ""
        "bash tools/run-oidc-fixture.sh up${externalHostOption}"
    }

    private static String resolveTokenPath(File manifestFile, Map token) {
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
        if (existing != null) {
            return existing.canonicalPath
        }
        return candidates.isEmpty() ? "" : candidates[0].path
    }

    private static Map<String, String> parseJdbcMysqlEndpoint(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Cannot parse FE MySQL endpoint from empty jdbcUrl")
        }

        String trimmedJdbcUrl = jdbcUrl.trim()
        try {
            URI uri = URI.create(trimmedJdbcUrl.substring("jdbc:".length()))
            if (uri.host != null && !uri.host.trim().isEmpty()) {
                return [
                        host: uri.host,
                        port: uri.port > 0 ? uri.port.toString() : "9030"
                ]
            }
        } catch (Exception ignored) {
            // Fall through to the regex parser for non-standard JDBC URL variants.
        }

        def matcher = trimmedJdbcUrl =~ /^jdbc:mysql:\/\/(\[[^\]]+\]|[^:\/?,]+)(?::(\d+))?/
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Cannot parse FE MySQL endpoint from jdbcUrl: ${jdbcUrl}".toString())
        }
        [
                host: matcher.group(1),
                port: matcher.group(2) ?: "9030"
        ]
    }

    private static String shQuote(String value) {
        "'" + value.replace("'", "'\"'\"'") + "'"
    }

    private static Map<String, Object> runShellCommand(String command) {
        Process process = ["bash", "-lc", command].execute()
        StringBuffer stdout = new StringBuffer()
        StringBuffer stderr = new StringBuffer()
        process.waitForProcessOutput(stdout, stderr)
        [
                status: process.exitValue(),
                output: stdout.toString() + stderr.toString()
        ]
    }
}

return OidcSuiteHelper
