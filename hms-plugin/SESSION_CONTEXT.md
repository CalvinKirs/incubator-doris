# HMS authorization plugin — session context

Updated: 2026-09-20. This file carries the current conversation's decisions into the new worktree; it is not a copy of the chat transcript or a new Codex session.

## Workspace

- Worktree: `/mnt/disk2/gq/doris-worktree/hms_auth_plugin`.
- Branch: `hms_auth_plugin`, created from freshly fetched `rich/branch-4.1` at `a060d016459`.
- Continue all subsequent project work in this worktree. Do not work in the base checkout.
- The active design is `hms-plugin/hms-plugin-design.md`. The other two documents retain earlier SELECT-only discussions and are marked superseded for scope.
- Plugin implementation is now in `hms-plugin/src/main/java/org/apache/doris/plugin/hms/`.
- Factory: `org.apache.doris.plugin.hms.HmsAccessControllerFactory`, registered with the existing ServiceLoader.
- Original design session: `01a089f5-e251-73e1-8585-c7019fae8960` (实现HMS权限同步与联邦查询鉴权).
- Build/run details and actual branch restrictions: `hms-plugin/README.md`.
- FE and plugin main code compiled in this worktree; latest run: 30 tests passed (24 unit/protocol + 6 real Docker HMS integration), 0 failures/errors/skips.
- Checkstyle reactor validation passed. Real Docker HMS 1.1.0 Simple validation passed; Real FE/BE SQL regression also passed; Kerberos and multi-FE process validation have not run.

## Agreed scope

- Build an independent Doris Catalog authorization plugin against `branch-4.1` existing APIs. No kernel, built-in Connector, SPI, core JAR or dependency replacement.
- Plugin packaging/loading uses existing capability and is outside the current implementation scope.
- Hive tables only. Iceberg backed by HMS is conceptually applicable but is not in this iteration's implementation/test scope.
- Main integration target: customer's HMS 1.1.0 using Hive native authorization (confirmed). HMS 2/3/4 protocol checks are references, not completed interoperability tests.
- Support SELECT, INSERT, CREATE and ALTER; map actual SQL subtypes to native Hive privileges/ownership. Existing Doris Connector capabilities still constrain executable SQL.
- Read USER/ROLE grants and role inheritance, merge applicable identities per target table. Do not mirror grants into Doris, manage HMS roles, or write GRANT/REVOKE back.
- Doris and HMS usernames are identical and case sensitive. ROLE names are case insensitive. Resolve remote names before normalizing Hive database/table names; preserve principal type in cache keys.
- No row/column authorization, masking or additional SHOW/DESC permission rules.
- Own HMS connection pool per FE/Catalog plugin instance. Simple and Kerberos; reuse existing compatible authenticator doAs for connection creation/reconnection and permission RPCs, including credential renewal.
- Use Caffeine with expireAfterWrite, no access-based renewal, no asynchronous or scheduled full refresh in the initial implementation. Cache role edges, table grants/required owner information and database information for CREATE. Do not retain long-lived final user-table allow decisions.
- Shared TTL setting; zero disables cross-query caching. Capacity controlled by `cache.maximum.size`, applied separately to each cache. Suggested default maximum 10000 entries; sample TTL 600 seconds is not a confirmed default.
- Explicit/manual cache refresh is deferred. Do not add SQL or HTTP refresh entry points or cross-FE broadcasts.
- Local administrator exception is configurable per Catalog with proposed `doris.admin.bypass.enabled`, suggested default false. True exempts actual Doris administrators from HMS checks, not arbitrary usernames or HMS admin roles. Ordinary local global SELECT does not replace HMS authorization; handle the existing hasGlobal wrappers accordingly.
- Lifecycle-driven pool/background-task release on Catalog removal/recreation is deferred. Normal return of borrowed connections and broken-connection disposal/reconnection remain required. Do not reuse old permissions across distinct Catalog instances.

## Implementation checks, not new user scope questions

- Prioritize verifying how existing branch-4.1 contexts distinguish INSERT INTO/OVERWRITE and ALTER subtypes despite generic LOAD/ALTER predicates. Do not silently broaden privileges or modify the kernel to compensate.
- CREATE TABLE checks database creation entitlement/ownership before a table exists; CTAS and INSERT SELECT also check source SELECT permissions. Verify actual HMS owner after creation by a service identity.
- HMS 1.1.0 lacks catName. Required role and privilege APIs exist and null-principal table-wide privilege listing was verified in upstream source.
- Test grant/revoke propagation, role inheritance, ownership changes, fixed TTL under hot traffic, load failures and same-key concurrency. Successful empty grants may be cached; RPC failures must not become empty-success or stale permission allows.

## Collaboration preferences

- Communicate in concise conversational Chinese; avoid repetitive framework/packaging confirmations and artificial blockers.
- Keep ongoing requirements from this session. New worktrees default to direct children of `/mnt/disk2/gq/doris-worktree/`.
- Latest user instruction: do not commit. The previous local implementation commit was undone with git reset --mixed; all plugin files remain in the worktree. Do not commit unless subsequently requested.
- Follow inherited AGENTS.md for build/test rules. Use build.sh for Doris builds and repository test scripts; do not commit environment bootstrap files.

## Implementation findings (2026-09-11)

- Use StmtExecutor.getParsedStmt() first, then StatementContext.getParsedStatement(), to identify Nereids commands.
  INSERT OVERWRITE requires INSERT + DELETE; CTAS has an internal LOAD check with actual table owner verification.
- Hive 3 client getTable() sends get_table_req. The plugin reuses HiveMetaStoreClient transport/SASL but calls the
  old get_table/get_database/permission Thrift APIs over getTTransport(); no reflection or replacement dependencies.
- Hive 1.1 default database creation follows native SQL Standard's public creation entitlement.
- HiveMetadataOps at this baseline lacks remote column/name/property ALTER implementations; generic interfaces
  throw unsupported after the plugin's permission check. Do not claim the plugin adds Connector execution support.
- TTL is required explicitly. Maximum size defaults to 10000 per cache, pool size to 8, socket timeout to 30 seconds,
  and local-admin bypass to false. No final-decision or expanded-role cache is retained.
- The initial build.sh prerequisite failure was resolved for FE-only builds by copying the real libhdfs.a
  from our Hadoop 2.7.4 container. build.sh --fe subsequently succeeded; no BE build was performed.
- Use hms-plugin/maven.sh via CUSTOM_MVN and EXTRA_FE_MODULES=hms=../hms-plugin with repository scripts.
  It creates and removes a temporary reactor file under fe/, without changing any tracked FE POM.
- FE's Maven auto-clean runs at initialize. For repeat test iterations use MAVEN_ARGS='-Dskip.clean=true'.
  Bound Maven memory (e.g. -Xmx6g) on the shared host. Do not skip main compilation after auto-clean.
- Own Docker HMS 1.1.0 Simple fixture is now available via hms-plugin/docker/run.sh.
  Real FE/BE SQL regression subsequently passed; Kerberos and multi-FE process regression remain unrun.

## Validation completed

- 2026-09-11: repository run-fe-ut.sh with CUSTOM_MVN=hms-plugin/maven.sh and EXTRA_FE_MODULES=hms=../hms-plugin:
  24 tests passed. Final test-only correction run used MAVEN_ARGS='-Dskip.clean=true -Dmaven.main.skip=true'
  to reuse the already compiled, unchanged FE/plugin main classes. Use the documented normal command after source changes.
- hms-plugin/maven.sh -o validate -pl ../hms-plugin -am -DskipTests: passed, zero Checkstyle violations.
- bash -n hms-plugin/maven.sh and git diff --cached --check: passed.
- Existing security/review skill files mentioned by inherited AGENTS.md were unavailable on this branch/environment;
  ordinary implementation self-review results are recorded in README.md, without a separate security review.

## Docker HMS interface validation (earlier phase)

- User requested no commits and a self-provisioned Docker HMS. Previous implementation commit was reset,
  retaining all files. HEAD remains a060d016459; no kernel changes.
- hms-plugin/docker/run.sh downloads the official Hive 1.1.0 archive with a pinned SHA-256, uses the cached
  Java 8/Hadoop 2.7.4 base image, initializes Derby, and runs the repository FE test script.
- 2026-09-11 12:25:18 +08:00: all 30 tests passed, including 6 HmsDockerIntegrationTest cases;
  real USER/ROLE grant/revoke, inheritance/public/admin exclusion, USER case, database/table owner changes,
  controller SELECT/INSERT/OVERWRITE/CREATE/ALTER, hot-traffic TTL across two instances, failed-load recovery,
  server stop/fail-closed/start/reconnect. Test fixture writes use real legacy Thrift RPCs; no mock HMS.
- Initial trial exposed Docker dynamically remapping an automatically published host port on restart.
  Runner now chooses a free host port and binds it explicitly. Final run passed after this harness correction.
- Retained running container: hms-auth-test-1789100662-2834358. URI: thrift://127.0.0.1:58865.
  Image: gq-hms-auth:1.1.0. Hive --version: 1.1.0; Derby schema initialized to 1.1.0.
- Final invocation: MAVEN_ARGS='-Dmaven.main.skip=true' hms-plugin/docker/run.sh.
  Production sources were unchanged; existing compiled FE/plugin classes were reused.
- Checkstyle passed. Logs and copied Surefire results: hms-plugin/target/docker/validation/.
- No Doris cluster SQL execution, Kerberos authentication/renewal, or actual multi-FE process test claimed.
  Two HmsAuthorization instances verify cache separation only. Lifecycle disposal and explicit refresh remain deferred.

## Current cluster SQL validation (2026-09-11)

- User explicitly required real cluster SQL and authorized copying the release to /mnt/disk2/gq/doris-release.
  No-commit instruction remains active. Do not fall back to unit-only validation.
- Copied the existing apache/doris:all-in-one-4.1.3 image's /opt/apache-doris into that directory.
  Reused BE 4.1.3 (7126cf65d96), running /mnt/disk2/gq/doris-release/be.
- Built current branch-4.1 FE + plugin through build.sh --fe, DISABLE_BUILD_UI=ON,
  DISABLE_BE_JAVA_EXTENSIONS=ON, EXTRA_FE_MODULES=hms=../hms-plugin, CUSTOM_MVN=hms-plugin/maven.sh.
  MAVEN_ARGS='-Dskip.clean=true -Dmaven.test.skip=true'; main sources compiled, memory capped at 5g.
  FE now runs at /mnt/disk2/gq/doris-release/hms-auth-fe (MySQL 43027, HTTP 42027).
- BE heartbeat 43047; HDFS 34097 (hms-auth-sql-hdfs), HMS 34102 (hms-auth-sql-hms).
  HDFS/HMS are own Docker containers using host networking with isolated explicit ports.
  All ports in /mnt/disk2/gq/doris-release/hms-auth-ports.json. Preserve the cluster and fixtures.
- FE startup needs setsid with --daemon in this tool environment; plain nohup children did not persist.
- Built regression framework using repository run-regression-test.sh --compile.
- Replaced the prior external-fixture-only regression with a self-provisioning SQL suite.
  Native Thrift fixture client creates grants and tables; real HDFS contains data; actual FE/BE execute SQL.
  USER JDBC connections omit the framework default database to avoid unrelated internal-database permissions.
  Setup drops HMS tables individually before dropping the database (Hive 1.1 Derby cascade FK behavior),
  and revokes an existing test admin membership before regranting (Hive 1.1 rejects duplicate grants).
- IMPORTANT: Doris already patches HiveMetaStoreClient with version-aware legacy RPC support.
  Set hive.version=1.1.0 on the metadata Catalog. Earlier notes about upstream Hive 3 wrappers
  were not a complete description of the Doris core client. The plugin's raw legacy RPCs also work.
- 2026-09-11 14:44:58 +08:00: normal comparison regression run passed (1 suite, 0 failed/skipped/fatal),
  8 ordered result sets and 14 expected denial/failure checks. .out was generated by the regression script.
  SELECT, INSERT VALUES/SELECT, OVERWRITE denied without DELETE then succeeds with it, CREATE/CTAS,
  ALTER auto_analyze_policy success and owner-change denial, source SELECT revocation, role inheritance/revoke,
  public/admin exclusion, local ADMIN bypass flag, global SELECT non-bypass, TTL under SQL traffic,
  two Catalog instances, HMS stop fail-closed and recovery in the same FE all passed.
- FE audit confirms ScanRows=3, ReturnRows=3, isHandledInFe=false. Not a mocked connector or only an analyzer test.
- Run configuration: hms-plugin/target/cluster/regression-conf.groovy.
  Detailed report and repeat command: hms-plugin/CLUSTER_SQL_VALIDATION.md.
  Logs, status and audit evidence: hms-plugin/target/cluster/.
- Remote Hive schema/name/property ALTER still unsupported by this kernel; positive ALTER uses local
  auto_analyze_policy. Kerberos and multiple FE processes are not covered.

## Latest task: case semantics and test sufficiency

- User challenged case coverage. Previous all-lowercase cluster suite was insufficient.
- Added hms_case_authorization.groovy: 9 result sets + 13 expected authorization/name-resolution errors.
  USER HmsCaseUser/hmscaseuser and HmsCaseMember/hmscasemember are separate login identities; USER and ROLE
  hmscaseuser have distinct rights; actual Hive 1.1 SQL mixed-case role creation/grants/revoke; DB/table owner
  case; native HMS normalization; Doris lower_case_database_names/lower_case_table_names modes 0/1/2;
  quoted identifiers and SQL aliases; role revocation and reversed USER grants after TTL.
- Added own HiveServer2 1.1 Docker container hms-auth-sql-hs2, host port 34103, SQL Standard authorization on,
  doAs=false, remote HMS 34102. Hive 1.1 CLI rejects SQL Standard auth; use Beeline via this HS2.
  Beeline 1.1 uses -f /dev/stdin with one statement per line (not one multi-statement -e).
  Local regression config now includes hmsAuthHs2Container and hmsAuthHs2Jdbc.
- HmsNameCaseTest passed via run-fe-ut.sh (1 test, 0 failures/errors/skips), using unchanged compiled main code.
  It sets and restores Turkish default Locale to verify Locale.ROOT behavior and USER/ROLE typed keys.
  Checkstyle reactor validation passed, zero violations.
- New case .out generated via run-regression-test.sh; final normal combined comparison run logs in
  hms-plugin/target/cluster/case-combined-verify.log. Full matrix and limitations: CASE_VALIDATION.md.
- No commits; no production Java or kernel changes in this case-testing phase.

- User requested removal of the extra catalog alias configuration scenarios and related scope claims.
  Removed those fixtures and assertions; retain real case behavior and SQL AS alias checks.
  Updated case suite: 9 result sets and 13 expected errors; combined suites: 17 and 27.
  Golden regenerated by repository script; normal combined verification passed at 2026-09-11 15:18:30.267:
  2 suites, 0 failed/fatal/skipped. Resolver unit fixture now uses case differences only; 1 test passed
  through run-fe-ut.sh, including Maven Checkstyle. Evidence: target/cluster/case-removal-*.log.
  Do not reintroduce unrequested catalog alias support.

## Latest correction: Hive-specific Kerberos configuration

- User requires HMS Kerberos configuration, independently of HDFS credentials.
- HmsPluginConfig now delegates HiveConf and authenticator creation to existing HMSBaseProperties.
  Use hive.metastore.authentication.type, hive.metastore.client.principal/keytab,
  hive.metastore.service.principal (legacy service alias hive.metastore.kerberos.principal retained).
  Default HMS authentication is explicitly simple; never fall back to HDFS credentials.
- PooledHmsPrivilegeSource uses this HMS authenticator for connection creation and every RPC.
  Existing doAs/credential renewal machinery is reused; no kernel changes.
- build.sh --fe passed with Checkstyle. Deployed the newly built plugin JAR to the existing test FE
  and restarted it; BE remains prebuilt 4.1.3. Artifact SHA-256:
  a788449425b657d34c42511b6bf1dd10956245e9da32376c0697587179547ffe.
- 2026-09-11 15:48:40: run-fe-ut.sh explicit list of 9 classes passed 33 tests, 0 failures/errors/skips,
  including 6 actual Docker HMS 1.1 Simple tests and 5 configuration tests.
  Initial package wildcard matched no tests; only the explicit-class rerun counts as validation.
- 2026-09-11 15:47:59.113: both SQL suites passed, 0 failed/fatal/skipped, on the updated plugin.
  Evidence: target/cluster/hive-kerberos-build.log, hive-kerberos-unit-verify.log, hive-kerberos-sql.log.
  Real KDC login/renewal/reconnect validation is not claimed.
- At that point the information_schema finding remained open; it is now fixed as recorded below.

## Current task: local system-table review fix

- User explicitly clarified that the customer platform-secret login is unrelated to this plugin.
  It is out of scope, not a pending item or completion blocker; do not request its protocol for this task.
- Implementing the confirmed R1 information_schema compatibility fix. Identify actual local database/table
  types; delegate their SELECT checks to Doris local controller with original catalog and columns.
  Default user system-schema grants cover internal only; external system-schema SELECT remains separately scoped.
- Added resolver/controller tests and real SQL cases for local grants/revocation, metadata tables/columns/count,
  mysql metadata, special system-table restrictions, and a system/Hive join with denied source permissions.
- build.sh --fe and Checkstyle passed. run-fe-ut.sh explicit 9-class list passed 35 tests,
  0 failures/errors/skips, including 6 real Docker HMS Simple tests.
- Updated test FE plugin JAR and restarted FE; BE remains 4.1.3. SHA-256:
  5aa07b12cdd8e5f281a5ea831cafb37b47110915ea5228f0b3060ce4a303027b.
- Golden generated by repository script; final normal combined regression passed 2026-09-11 21:11:41.245,
  2 suites, 0 failed/fatal/skipped, 23 result sets and 31 expected errors.
  Main functional suite now 14 result sets / 18 expected errors; case suite remains 9 / 13.
- Initial generation failed solely on expected error text for revoked COUNT(*) metadata access, which
  also uses a column permission check. Corrected to Permission denied and reran generation/comparison.
- Evidence: target/cluster/system-tables-*.log; resolution: review/R1-resolution.md.
  No commits, no kernel edits. Real Kerberos KDC validation remains pending.

## 2026-09-14: review fixes, hardening, and Hive-consistent DDL ownership

- Reran all four layers on the untouched 09-11 build first: 35 unit (6 real HMS), 3 SQL suites, 7 Kerberos Java. All passed.
  Kerberos Java tests are gated by HMS_KRB_TEST_URI etc.; `source hms-plugin/target/kerberos/runtime/fixture.env` first.
- FE start_fe.sh refuses to start while http_proxy is set in the shell; unset the proxy variables before restarting the test FE.
- Regression JVM was OOM-killed once when both Simple suites and the Kerberos suite ran in one shell command on the
  shared host; run the Kerberos suite as a separate command.
- Plugin changes (kernel untouched, still uncommitted):
  1. checkDbPriv(CREATE) no longer resolves the target; CREATE DATABASE is allowed for every user, matching Hive 1.1
     Operation2Privilege (URI privileges only). checkDbPriv(DROP) requires the HMS database owner.
  2. DROP TABLE and TRUNCATE TABLE require the HMS table owner (Hive 1.1: OWNER_PRIV). TRUNCATE arrives as LOAD with
     TruncateTableCommand. Local information_schema/mysql tables allow only SELECT via the local controller.
  3. PooledHmsPrivilegeSource retries exactly once on a fresh connection when the cause chain contains
     TTransportException; application errors are not retried. New Docker test proves the pre-restart idle connection no
     longer fails the first post-restart check (verified red without the retry, green with it).
  4. Failure WARN is one line with the root cause's first line (Hive embeds stack traces in MetaException messages);
     stack trace only at DEBUG.
  5. HmsMetrics registers four gauges per catalog and cache on first use (catalog name is only known at check time):
     hms_authorization_cache_{hits,misses,load_failures,evictions}{catalog,cache}. Same name+labels replace the
     gauges of a rebuilt controller. Factory logs config.describe() at INFO without credential paths.
- Kernel facts verified on this baseline: catalog-level checks never reach the plugin (always internal controller);
  REFRESH CATALOG keeps the controller, ALTER/DROP CATALOG rebuild it; HiveMetadataOps.createDbImpl does not set
  Database.ownerName, so a Doris-created database has an empty HMS owner and its creator cannot create tables in it
  or drop it under ownership rules (documented in README, not fixable in the plugin); HMS 1.1 has no truncate_table
  RPC, so TRUNCATE fails in the kernel after authorization passes.
- Final validation on JAR sha256 ee2d5c5db8812ba7...: 44 tests (7 real HMS), 0 Checkstyle violations,
  hms_native_authorization + hms_case_authorization + hms_kerberos_authorization all 0 failed/fatal/skipped.
  New SQL cases are expected-error only; no .out regenerated. Logs: hms-plugin/target/rerun/.
- Independent subagent review of the CREATE DATABASE change: safe; also noted two pre-existing boundaries worth
  documenting: FrontendService checkAuth with DATABASE hierarchy on an HMS catalog is denied for non-admins, and
  non-admins cannot GRANT ... ON hms.db.* because that flow uses checkDbPriv.
- Plugin-directory loading verified: JAR moved from FE lib/ to plugins/authorization/, FE restarted, factory found
  "from directory", all three SQL suites passed, no class loading errors. fe-core's ChildFirstClassLoader.findClass
  reopens the JAR per lookup, so the loader being closed after discovery is harmless; the JAR must stay in place
  while the FE runs. The test FE now runs the plugin from plugins/authorization/ (lib/ copy kept in
  hms-plugin/target/rerun/*.from-lib).
- TRUNCATE: truncate_table RPC exists only from Hive 3.0 (checked 2.3.9/3.0.0/3.1.3/4.0.1 IDL); Doris 4.1 sends it
  regardless of hive.version. Workaround verified on 1.1: INSERT OVERWRITE with an empty result set clears the table.
- Still open: nothing committed (user instruction stands); stale copies of the three design docs remain in
  /mnt/disk1/gq/idea/incubator-doris/.worktrees/hms-plugin/ (worktree copies are current).

## 2026-09-16: evidence the deep review asked for

- Unit: revocation during an in-flight load is served stale for one TTL measured from load completion (fake ticker);
  same-key coalescing now proves the second caller blocks on the in-flight load; pool test proves at most pool-size
  callers are inside the client, a transport failure mid-batch destroys exactly its two attempts' connections, and
  active connections return to zero after two rounds. Replacement-vs-reuse counts depend on interleaving, so the
  assertions use the new package-private activeConnections()/idleConnections() hooks rather than created counts.
- SQL (hms_native_authorization, .out regenerated by the repository script): ROLE-owned database hms_auth_roledb
  (ownerType ROLE = hms_auth_team) accepts CTAS from a role member, denies the USER owner of the other database, and
  denies the member after role revocation; server-side prepared statements via JDBC useServerPrepStmts=true
  (COM_STMT_PREPARE, audit shows `values (?)`) keep INSERT vs OVERWRITE requirements: prepared append succeeds with
  INSERT, prepared overwrite is denied without DELETE and succeeds with it.
- `PREPARE ... FROM` text syntax is not supported on this branch; server-side prepare only via the binary protocol.
- Validation: 46 tests (7 real HMS), 0 Checkstyle violations, both Simple suites and the Kerberos suite pass on the
  JAR deployed in plugins/authorization/.

## 2026-09-18: fixture moved into run-thirdparties-docker.sh (opt-in)

- Decision (user): the plugin will be contributed to Doris. Its docker fixture is a thirdparty component that is
  never started by default: only `run-thirdparties-docker.sh --hms-auth` starts it and only
  `--hms-auth --stop` stops it. It is in neither DEFAULT_COMPONENTS nor ALL_COMPONENTS.
- New component docker/thirdparties/docker-compose/hms-auth/: compose template with five host-network services
  (hdfs 8720, hms 9783, hs2 10783, kdc 8788, krb-hms 9793; container names doris-${CONTAINER_UID}-hms-auth-*),
  settings env, two Dockerfiles, five start scripts, seven config templates, README. runtime/ (rendered config,
  krb5.conf, keytabs, fixture.env), the rendered yaml and the Hive archive are ignored. The repository ignores
  docker-compose/*/*.env, so the component .gitignore re-includes hms-auth_settings.env.
- run-thirdparties-docker.sh: --hms-auth option, start_hms_auth(), dispatch, usage. Also clears the legacy
  positional `COMPONENTS=$2` inside option parsing: without -c it turned the second option into an invalid
  component list (e.g. "--hms-auth --stop"; "--no-load-data --stop" was already broken the same way).
- Verified through the launcher (pass-through sudo shim because this host's sudo needs a password; dry-run shim
  first to prove --stop scope): start healthy, `--hms-auth --stop` removed exactly its 5 containers and runtime/
  while 54 other containers were untouched, restart healthy in under 30s. On this fixture:
  HmsDockerIntegrationTest 7/7, HmsKerberosIntegrationTest 7/7, hms_native_authorization + hms_case_authorization
  and hms_kerberos_authorization all 0 failed/fatal/skipped (conf: target/cluster/regression-conf-compose.groovy).
- Local state: docker/thirdparties/custom_settings.env has CONTAINER_UID="gqhms" (local only, never commit);
  test FE's krb5.conf now points at docker-compose/hms-auth/runtime/krb/krb5.conf (backup of fe.conf in
  target/rerun/fe.conf.before-compose). The six hand-started hms-auth-* containers from 09-11 still run and are
  now redundant; hms-plugin/docker/ scripts are superseded by the component but not yet removed.
- Still to do for upstreaming: move the three suites under regression-test/suites with an enable flag, add the
  hmsAuth*/hmsKrb* keys to regression-test/conf/regression-conf.groovy, decide the module location of hms-plugin.

## 2026-09-20: copied into selectdb/enterprise-plugins

- User decision: the plugin also lives in ~/idea/enterprise-plugins (the path was given as "enterpris-plugin").
  Copied, not moved; this Doris worktree stays the source baseline. Worktree there:
  .worktrees/feat-hms-authorization, branch feat/hms-authorization-plugin, nothing committed.
- Layout there: java/authorization/fe-authorization-plugin-hms (main, tests, README, docs/), domain pom
  java/authorization/pom.xml, docker/hms-auth (fixture), regression-test/{suites,data}/enterprise_plugins/
  authorization/hms_p0, tools/run-hms-auth-fixture.sh (up/down/status; syntax-checked only, not run, because the
  Doris-side fixture holds the same ports).
- User requirement: the plugin MUST build against branch-4.1 fe-core and the README must say so. Done: README leads
  with that section (why, install under a separate revision, self-check commands). Build wiring: root property
  doris.fe.core.version, module behind profile `authorization` (default build excludes it), domain pom aligns
  fe-foundation/fe-authentication-* with fe-core's version, imports org.apache.doris:fe as a BOM, and declares the
  ASF snapshot repository under id `snapshots` for org.apache.doris:je.
- This host's ~/.m2 org.apache.doris:*:1.2-SNAPSHOT is NOT branch-4.1 (has LegacyAccessControllerPlugin, no hasGlobal
  overloads, no HMSBaseProperties) and its poms keep an unresolved ${revision} parent, so even the untouched
  enterprise-plugins master fails `mvn -o validate` here. Installed branch-4.1 FE additively as 4.1.0-hms-SNAPSHOT
  (mvn -o install -pl fe-core -am -Drevision=4.1.0-hms-SNAPSHOT); 1.2-SNAPSHOT untouched.
- Found there: Apache parent 29 brings surefire 2.22.2, which silently runs 0 tests with junit-jupiter 5.14
  ("Tests run: 0", BUILD SUCCESS). Pinned surefire 3.5.2 in root pluginManagement; likely affects OIDC tests too
  (not verified, OIDC cannot build on this host for the reason above).
- Verified there with -Pauthorization -Ddoris.fe.core.version=4.1.0-hms-SNAPSHOT: spotless check, 39 unit tests,
  HmsDockerIntegrationTest 7/7 and HmsKerberosIntegrationTest 7/7 against the running fixture, jar (17 plugin
  classes only) and plugin zip. SQL suites were copied but not run from that repository.
- Evidence check the same day: a JAR rebuilt from current sources is byte-identical (17 classes + services file)
  to the one deployed in the test FE. The test FE was stopped gracefully (SIGTERM) on 2026-09-18 18:00, not by
  this session; it is still down.

## 2026-09-20: binary compatibility with official 4.1.3, and the user's decision

- Static linkage check of the deployed plugin JAR (all 210 external class/method/field references, 86 classes):
  0 missing against the branch-4.1 head FE it ran in (control), exactly 1 missing against the official 4.1.3 FE:
  AlterTableCommand.getNereidsOps(), added after 4.1.3 by branch-4.1 backport #66032. 4.1.3 only has getOps()
  returning legacy AlterTableClause. On a 4.1.3 FE an ALTER TABLE on the HMS catalog would fail closed with
  NoSuchMethodError; every other statement type links fine (including hive-catalog-shade 3.1.1 there vs 3.1.3 on
  head, and plugin-directory loading). Checker: scratchpad linkcheck.py (javap constant-pool references resolved
  through the class hierarchy against a lib directory).
- User decision: this does not matter for the customer ("那不影响"). No getOps() fallback is to be implemented.
  If the customer turns out to run 4.1.3, document "ALTER TABLE unavailable on 4.1.3" instead of changing code.
- Behaviour on a 4.1.3 FE was never run, only link-checked.
