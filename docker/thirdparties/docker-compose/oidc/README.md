# OIDC Test Environment

This directory hosts the local Keycloak setup used by Doris OIDC integration and regression testing.

Start it through the standard thirdparties entrypoint:

```bash
bash docker/thirdparties/run-thirdparties-docker.sh -c oidc
```

Stop it through the same entrypoint:

```bash
bash docker/thirdparties/run-thirdparties-docker.sh --stop -c oidc
```

The default HTTP port is `8080`. You can change it in [`oidc.env`](./oidc.env) before startup.

Startup automatically generates a reusable access-token fixture set under [`tokens/`](./tokens):

- `tokens/oidc_alice_grafana_query.access_token`
- `tokens/oidc_alice_grafana_missing_scope.access_token`
- `tokens/oidc_alice_agent_query.access_token`
- `tokens/oidc_bob_grafana_query.access_token`
- `tokens/oidc_carol_grafana_query.access_token`
- `tokens/oidc_dave_grafana_query.access_token`
- `tokens/oidc_erin_grafana_query.access_token`
- `tokens/oidc_frank_legacy_query.access_token`
- `tokens/manifest.json`

The manifest records the issuer, JWKS URI, audience, generated file path, and decoded claims for each token. Token lifetime defaults to `10800` seconds (`3` hours).

Default callers and identities:

- Realm: `doris`
- Doris audience: `doris`
- Allowed caller: `grafana-doris-plugin`
- Allowed legacy caller: `legacy-grafana-plugin`
- Disallowed regression caller: `agent-gateway`
- Users: `oidc_alice`, `oidc_bob`, `oidc_carol`, `oidc_dave`, `oidc_erin`, `oidc_frank`

Example: use the generated access token for manual Doris login tests.

```bash
source docker/thirdparties/docker-compose/oidc/oidc.env

mysqlsh --sql --sqlc --ssl-mode=REQUIRED \
  -h127.0.0.1 -P9030 -u oidc_alice \
  --authentication-openid-connect-client-id-token-file=docker/thirdparties/docker-compose/oidc/tokens/oidc_alice_grafana_query.access_token \
  -e "select current_user()"
```

If your shell exports `HTTP_PROXY` or `HTTPS_PROXY`, keep the `--noproxy '*'` flag or set `NO_PROXY=127.0.0.1,localhost`. Otherwise local requests to Keycloak may be intercepted by the proxy and fail with `502 Bad Gateway`.

For the local access-token regression suite, point Doris at the external OIDC plugin directory before running:

```bash
export DORIS_OIDC_PLUGIN_DIR=~/idea/enterprise-plugins/java/authentication/fe-authentication-plugin-oidc/target/plugin/fe-authentication-plugin-oidc
./run-regression-test.sh --run -d auth_p0 -s test_oidc_access_token_auth
```

## End-to-End Regression Flow

This section describes the full local flow for the Doris OIDC access-token regression suite in
[`regression-test/suites/auth_p0/test_oidc_access_token_auth.groovy`](../../../../regression-test/suites/auth_p0/test_oidc_access_token_auth.groovy).

### 1. Build and start a local Doris FE and BE

The regression config defaults to `127.0.0.1:9030` for MySQL, `127.0.0.1:9020` for thrift, and
`127.0.0.1:8030` for HTTP. The simplest path is to keep the default Doris ports.

```bash
cd /path/to/incubator-doris
./build.sh --fe --be -j${DORIS_PARALLELISM}
output/fe/bin/start_fe.sh --daemon
output/be/bin/start_be.sh --daemon
sleep 30
mysql -h127.0.0.1 -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';"
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW FRONTENDS; SHOW BACKENDS;"
```

If you use non-default Doris ports, update
[`regression-test/conf/regression-conf.groovy`](../../../../regression-test/conf/regression-conf.groovy)
locally before running the suite.

### 2. Configure a unique docker container prefix

The standard thirdparties entrypoint requires a non-default `CONTAINER_UID` in
[`docker/thirdparties/custom_settings.env`](../../custom_settings.env). Change it to a unique local
value and do not commit that change.

Example:

```bash
CONTAINER_UID="doris-yourname-"
```

### 3. Start the OIDC fixture and generate token files

```bash
cd /path/to/incubator-doris
export NO_PROXY=127.0.0.1,localhost
bash docker/thirdparties/run-thirdparties-docker.sh -c oidc
```

This starts Keycloak and regenerates the token fixtures under [`tokens/`](./tokens). The manifest at
[`tokens/manifest.json`](./tokens/manifest.json) is the contract consumed by the regression suite.

### 4. Point the suite at the external OIDC plugin

```bash
cd /path/to/incubator-doris
export DORIS_OIDC_PLUGIN_DIR=~/idea/enterprise-plugins/java/authentication/fe-authentication-plugin-oidc/target/plugin/fe-authentication-plugin-oidc
```

If `mysqlsh` is not already on `PATH`, set `MYSQLSH_BIN` too:

```bash
export MYSQLSH_BIN=/path/to/mysqlsh
```

The live-login portion of the suite copies the plugin directory to the Doris FE
`authentication_plugins_dir` through `scp` and `ssh` as `root` to each FE host returned by
`SHOW FRONTENDS`. Make sure that access works in your local environment before enabling the live
checks.

If you already installed the OIDC authentication plugin on the FE side, you do not need to export
`DORIS_OIDC_PLUGIN_DIR`. The suite will use the existing plugin installation directly.

If you only want to validate the manifest and Doris DDL setup first, you can also skip the live
login branch by making `mysqlsh` unavailable from `PATH`.

The suite dynamically switches `authentication_chain` to `test_oidc_access_token_auth_oidc` during
the live-login phase and restores the previous value at the end. The running FE binary must support
mutable `authentication_chain` for this step.

### 5. Run the regression suite

```bash
cd /path/to/incubator-doris
./run-regression-test.sh --run -d auth_p0 -s test_oidc_access_token_auth
```

### 6. Coverage and expected results

The suite validates these cases:

- `oidc_alice_grafana_query`: Doris-scoped access token from the allowed Grafana client succeeds
- `oidc_alice_grafana_missing_scope`: token without `doris.query` fails
- `oidc_alice_agent_query`: token from disallowed client `agent-gateway` fails
- `oidc_frank_legacy_query`: comma-separated `doris_groups` claim is treated as one string, not split

It also validates Doris metadata setup:

- `CREATE AUTHENTICATION INTEGRATION ... type = 'oidc'`
- `CREATE ROLE MAPPING ... USING CEL 'has_scope(...)'`
- `CREATE ROLE MAPPING ... USING CEL 'has_group(...)'`
- `information_schema.authentication_integrations` contains the expected OIDC properties

### 7. Manual smoke check with one generated token

After the plugin is available on the FE side, a quick manual login should work with:

```bash
mysqlsh --sql --sqlc --ssl-mode=REQUIRED \
  -h127.0.0.1 -P9030 -u oidc_alice \
  --authentication-openid-connect-client-id-token-file=docker/thirdparties/docker-compose/oidc/tokens/oidc_alice_grafana_query.access_token \
  -e "select current_user()"
```

### 8. Common failures

- `OIDC token manifest not found`: start the OIDC fixture first with `-c oidc`
- `Communications link failure`: the Doris FE is not running on the configured MySQL port
- `CREATE ROLE MAPPING` parse error: the running FE binary is older than the source tree
- `scp` or `ssh` failure in live checks: local root SSH access to the FE host is missing
