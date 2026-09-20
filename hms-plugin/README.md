# HMS 原生鉴权插件

基于本 worktree 的 Doris `branch-4.1`（`a060d016459`）现有 CatalogAccessController 实现。
代码、测试和构建适配均在 `hms-plugin/`，不修改 Doris 内核及 FE reactor 配置。
使用 `org.apache.doris.plugin.hms.HmsAccessControllerFactory` 工厂。
插件加载、安装和依赖打包沿用部署方已有能力，本目录不提供替换内核 JAR 的方案。

## 权限规则

| 操作 | 检查 |
|---|---|
| SELECT | 本人或有效角色具有表 SELECT |
| INSERT INTO | 目标表 INSERT；查询源表另查 SELECT |
| INSERT OVERWRITE | 目标表同时具备 INSERT、DELETE；可来自不同有效身份 |
| CREATE TABLE | 目标库 owner 为本人或有效角色；Hive 1.1 的 default 库允许所有用户建表 |
| CTAS | 目标库建表资格及源表 SELECT；后续 LOAD 检查实际 HMS 表 owner 和库建表资格 |
| ALTER 列、表名、表属性 | 实际 HMS 表 owner 为本人 |
| ALTER ADD/DROP PARTITION | 分别映射 INSERT、DELETE；4.1 Hive Connector 当前会拒绝这些 SQL，映射不扩展 Connector 能力 |
| DROP TABLE | 实际 HMS 表 owner 为本人；SQL Standard 没有 DROP 授权 |
| TRUNCATE TABLE | 实际 HMS 表 owner 为本人；`truncate_table` RPC 自 Hive 3.0 才有，1.x/2.x 服务端上内核执行会报 Invalid method name。替代：INSERT OVERWRITE 空结果集，需 INSERT+DELETE |
| DROP DATABASE | 库 owner 为本人或有效角色 |
| CREATE DATABASE | 与 Hive 一致，任何用户可建；本基线内核建库时不写 HMS owner，见下文 |

注意：此基线的 HiveMetadataOps.createDbImpl 不设置 Database.ownerName，Doris 建出的库在 HMS 中 owner 为空。
按 owner 规则，建库者随后不能在该库建表或删库，需要在 Hive 侧补 owner 或由本地管理员绕过处理。
Hive 自身的 CREATE DATABASE 会把会话用户写成 owner；这一差异在内核，插件不写回 HMS。

注意：此基线的 HiveMetadataOps 没有覆盖远端改列、改名、改表属性的实现，实际执行会落到
ExternalMetadataOps 的 unsupported 默认方法；ALTER 鉴权映射不等于新增这些 SQL 能力。
Doris 本地的 auto_analyze_policy 属性修改仍走既有路径，也要求表 owner。

表 owner 本身不隐含 SELECT、INSERT。表 owner 按 Hive 1.1 的 USER 字段解析；库 owner 支持 USER/ROLE。
Doris 建 Hive 表时默认将当前用户名写入 owner，但显式 owner 属性及服务端行为可能改变它。
插件读取 HMS 实际值；不写回 GRANT/REVOKE，也不为普通 CREATE 新表补初始授权。
因此普通 CREATE 后查询/写入新表仍需要 HMS 中实际存在的 SELECT/INSERT 授权。

按 USER/ROLE 分别缓存角色关系，展开角色继承并去重；USER 区分大小写，ROLE 不区分。
自动启用 public，排除 admin 及仅通过 admin 可达的角色；不提供 SET ROLE。
库表名先经 Doris 解析取得实际远端名称，再用 Locale.ROOT 转小写。
大小写规则与实测边界见
[CASE_VALIDATION.md](CASE_VALIDATION.md)。
库 owner 不展开成库内所有表的读取权。列检查落实为表检查，不提供行过滤或脱敏。
SHOW/DESC 没有新增权限限制；其他未支持谓词/写命令拒绝，不按通用 LOAD/ALTER 猜测。
DROP、TRUNCATE、DROP DATABASE 按 Hive 1.1 Operation2Privilege 的 OWNER_PRIV 判定；CREATE DATABASE 按其规则放开。
Doris 内置 information_schema / mysql 系统表按实际数据库和表类型识别，SELECT 转交本地权限检查，
保留当前 Catalog 的权限范围和列检查；不会向 HMS 查询这些本地对象的授权。
用户默认的 internal 系统库权限不自动扩展到外部 Catalog，可按需要授予
`GRANT SELECT_PRIV ON hms_auth.information_schema.* TO user`。真实 Hive 表仍检查 HMS 权限。

本地全局 SELECT 不绕过插件。四种带 `hasGlobal` 的默认接口均已覆盖。
`doris.admin.bypass.enabled=true` 时，仅 Doris 实际 ADMIN 权限可以跳过 HMS 检查。
管理员身份使用内置鉴权器判断，不按用户名或 HMS admin 角色判断。
Doris 自身的内部查询跳过鉴权、Catalog 可见性等入口仍遵循现有内核行为。

## 配置

```sql
CREATE CATALOG hms_auth PROPERTIES (
    'type' = 'hms',
    'hive.metastore.uris' = 'thrift://hms.example.com:9083',
    'hive.version' = '1.1.0',
    'access_controller.class' = 'org.apache.doris.plugin.hms.HmsAccessControllerFactory',
    'access_controller.properties.hms.uri' = 'thrift://hms.example.com:9083',
    'access_controller.properties.hive.metastore.authentication.type' = 'simple',
    'access_controller.properties.hive.metastore.username' = 'hive',
    'access_controller.properties.cache.ttl.seconds' = '600',
    'access_controller.properties.cache.maximum.size' = '10000',
    'access_controller.properties.hms.pool.size' = '8',
    'access_controller.properties.hms.socket.timeout.seconds' = '30',
    'access_controller.properties.doris.admin.bypass.enabled' = 'false'
);
```

| 插件参数（省略 access_controller.properties. 前缀） | 默认/约束 |
|---|---|
| hms.uri | 必填；逗号分隔的 thrift://host:port，与元数据 Catalog 指向同一 HMS |
| cache.ttl.seconds | 必填、非负整数；0 不保留跨查询缓存；600 仅为示例 |
| cache.maximum.size | 10000，正整数；分别限制角色、表、库三个缓存 |
| hms.pool.size | 8，正整数；每个插件实例独立连接池 |
| hms.socket.timeout.seconds | 30，正整数；池耗尽时最多等待 60 秒 |
| doris.admin.bypass.enabled | false，严格校验 true/false |
| hive.metastore.authentication.type | simple 或 kerberos，默认 simple；HMS 认证独立选择 |
| hive.metastore.username | Simple 服务身份，默认 hadoop；兼容既有 hadoop.username 别名 |

Kerberos 使用与当前分支 HMS Catalog 相同的配置，以下每项均带 `access_controller.properties.` 前缀。
客户端 principal/keytab 缺失时拒绝初始化；服务 principal 按实际 HMS 配置填写：

```text
hive.metastore.authentication.type=kerberos
hive.metastore.client.principal=doris-hms@EXAMPLE.COM
hive.metastore.client.keytab=/path/on/each/fe/doris-hms.keytab
hive.metastore.service.principal=hive/_HOST@EXAMPLE.COM
```

配置解析和认证器创建复用当前分支 `HMSBaseProperties`，支持 `hive.conf.resources`、`hive.*` 配置及
`hadoop.security.auth_to_local`。服务 principal 兼容 `hive.metastore.kerberos.principal` 别名。
插件显式选择 HMS 认证模式，不借用 `hadoop.kerberos.principal/keytab` 等 HDFS 凭据。
插件独立配置认证，Catalog 元数据客户端参数不会自动传入；HDFS 认证仍由 Catalog 原有配置负责。
新建连接、权限 RPC 和故障后的新连接均进入现成 HadoopAuthenticator 的 doAs，凭据续期复用该认证器。
无权限是普通拒绝；RPC/解析失败抛出带操作和对象信息的异常并保留 cause，不缓存空成功，也不读过期授权放行。
失败连接销毁，下一次借用重建；成功请求归还连接。没有定时刷新或连接池清理线程。
传输层失败（`TTransportException`，典型是 HMS 重启后池中空闲连接已断）会换一条新连接重试一次；
应用层错误（对象不存在、MetaException 等）不重试。重试仍失败按原样报错。

日志与指标：控制器创建时 INFO 输出 URI、认证类型、TTL、容量、池大小、超时和管理员绕过开关，
不输出 keytab 路径和其他 Hive 属性；内核紧接着一行记录 catalog 名。HMS 读取失败每次检查只记一行 WARN，
堆栈在 DEBUG。首次检查时按 catalog 向 FE 指标注册四个 gauge，可在 `/metrics` 读取：
`hms_authorization_cache_hits`、`hms_authorization_cache_misses`、`hms_authorization_cache_load_failures`、
`hms_authorization_cache_evictions`，标签 `catalog` 和 `cache`（roles / tables / databases）。
ALTER CATALOG 重建控制器后，同名同标签的 gauge 指向新实例。

缓存使用 Caffeine expireAfterWrite，命中不续期。不缓存最终用户表判断或派生角色闭包。
角色、表、库各自按加载完成时间计时；各 FE 各实例独立，不是全局一致快照。
TTL 不取消已经执行中的鉴权/查询。容量是条目数，不是字节数，单条表授权量仍影响内存。
`HmsAccessController.getCacheStats()` 提供每类缓存命中、加载、失败及淘汰统计，并按上文通过 FE 指标暴露。
配置对象不可变，配置变更应通过现有部署机制重建控制器。没有显式刷新；Catalog 生命周期主动释放仍延期。

## 编译与单测

从 worktree 根目录运行。`maven.sh` 生成临时 reactor，将当前 FE 和插件纳入同一次 Maven 构建，退出时清理。

```bash
CUSTOM_MVN="$PWD/hms-plugin/maven.sh" \
EXTRA_FE_MODULES='hms=../hms-plugin' \
DISABLE_BUILD_UI=ON FE_MAVEN_THREADS=4 ./build.sh --fe

MAVEN_OPTS='-Xms512m -Xmx6g -XX:MaxMetaspaceSize=1g' \
MAVEN_ARGS='-Dskip.clean=true' \
CUSTOM_MVN="$PWD/hms-plugin/maven.sh" \
EXTRA_FE_MODULES='hms=../hms-plugin' \
./run-fe-ut.sh --run 'org.apache.doris.plugin.hms.*Test'

hms-plugin/maven.sh validate -pl ../hms-plugin -am -DskipTests
```

`build.sh` 需要完整第三方安装，缺少 `installed/lib/hadoop_hdfs/native/libhdfs.a` 时会重建第三方目录。
不能仅凭 `.worktree_initialized` 判断完整构建依赖就绪。此次从自建 Hadoop 2.7.4 镜像复制真实的
libhdfs.a 补齐 FE 构建的前置检查，`build.sh --fe` 已成功；没有编译或链接新的 BE。
FE 构建使用 `DISABLE_BE_JAVA_EXTENSIONS=ON`，BE 及其 Java 扩展直接复用 4.1.3 软件包。

## Docker HMS 1.1.0 集成测试

`docker/run.sh` 使用 Java 8 / Hadoop 2.7.4 基础镜像和 Apache 官方 Hive 1.1.0 安装包，
启动独立 HMS + Derby，绑定随机的本地端口。测试通过真实 Thrift RPC 创建库表、角色和授权记录，
插件本身仍然只读。无需已有 HMS、HDFS 或 HiveServer2。

```bash
hms-plugin/docker/run.sh
# 仅主代码已经编译且未修改时，可复用本 worktree 的编译结果：
MAVEN_ARGS='-Dmaven.main.skip=true' hms-plugin/docker/run.sh
```

脚本通过仓库 `run-fe-ut.sh` 运行全部插件测试。新增 `HmsDockerIntegrationTest` 覆盖真实 USER/ROLE
授权、继承/public/admin、owner 修改、INSERT/OVERWRITE 区分、撤权 TTL、独立缓存实例、
失败加载恢复和容器停机后的连接池重连。无 `HMS_AUTH_TEST_URI` 时这些集成测试自动跳过。
重启用例还需要 `HMS_AUTH_TEST_CONTAINER`，仅传入专用于此测试的容器名。

脚本保留容器及数据，并输出容器名、日志位置和删除命令。fixture 使用随机库/角色名，运行后不删库表。
可手动对已有测试容器重复运行（URI 和容器名替换为实际值）：

```bash
HMS_AUTH_TEST_URI=thrift://127.0.0.1:58865 HMS_AUTH_TEST_CONTAINER=hms-auth-test-1789100662-2834358 \
MAVEN_OPTS='-Xms512m -Xmx6g -XX:MaxMetaspaceSize=1g' \
MAVEN_ARGS='-Dskip.clean=true -Dmaven.main.skip=true' \
CUSTOM_MVN="$PWD/hms-plugin/maven.sh" EXTRA_FE_MODULES='hms=../hms-plugin' \
./run-fe-ut.sh --run 'org.apache.doris.plugin.hms.*Test'
```

这些测试验证插件与真实 HMS 的互通及权限决策。控制器测试直接调用现有鉴权接口；
不等同于启动 Doris 集群执行 SQL、Kerberos 认证或多 FE 进程回归。

## 测试环境(thirdparty docker 组件)

HMS 1.1.0、HDFS、HiveServer2、KDC 和 SASL HMS 五个容器已收进仓库标准入口,**默认不启动**,
只有显式 `--hms-auth` 才会启动或停止,不在默认组件和 `--stop` 全量列表里:

```bash
cd docker/thirdparties
./run-thirdparties-docker.sh --hms-auth          # 启动
./run-thirdparties-docker.sh --hms-auth --stop   # 只停这一组
source docker-compose/hms-auth/runtime/fixture.env   # Java 集成测试所需环境变量
```

端口、容器名和回归配置项见 `docker/thirdparties/docker-compose/hms-auth/README.md`。
本目录 `docker/` 下的 run.sh 和 kerberos/start.sh 是接入前的独立脚本,功能已被该组件覆盖。

## 集群 SQL 回归

已搭建独立 FE + BE + HMS 1.1.0 + HDFS 集群。运行目录是用户指定的
`/mnt/disk2/gq/doris-release`；FE 来自当前 branch-4.1 构建，BE 复用官方 4.1.3 二进制。
具体进程、端口、SQL 结果和复跑说明见 [CLUSTER_SQL_VALIDATION.md](CLUSTER_SQL_VALIDATION.md)。

`regression/suites/hms/hms_native_authorization.groovy` 自动准备专用 HMS 的库表/角色/授权、
HDFS 测试文件和 Doris 用户/Catalog。需要配置 `hmsAuthPassword`、`hmsAuthUri`、
`hmsAuthHdfsUri`、`hmsAuthContainer`。此 suite 会清理上次的同名 fixture 并重启指定 HMS 容器，
只针对自建的专用测试环境。结束后保留库表和数据用于排查。

```bash
JAVA_HOME=/mnt/disk1/gq/jdk17 JAVA_OPTS='-Xmx2g' \
./run-regression-test.sh --conf "$PWD/hms-plugin/target/cluster/regression-conf.groovy" \
  --run -d hms -s hms_native_authorization
```

首次使用仓库脚本 `-genOut`/`-forceGenOut` 生成结果，再不带生成参数执行对比校验；
`regression/data/hms/hms_native_authorization.out` 由脚本自动生成，无手写结果。
验证 HDFS 实际查询和写入、OVERWRITE 的 INSERT+DELETE、CREATE/CTAS、ROLE 作为库 owner 的 CTAS、
JDBC 服务端预处理语句（useServerPrepStmts=true）下的追加与覆盖、owner 变更、
public/角色继承/admin 排除、本地全局 SELECT、按 Catalog 控制的管理员绕过、
TTL 撤权、HMS 停机后 SQL 拒绝和恢复重连。ALTER 成功用例是现有内核支持的 auto_analyze_policy。
Kerberos 和多 FE 进程联调仍未运行；两个 Catalog 的独立实例测试不代表多 FE 进程验证。
HMS 2/3/4 未承诺互通，不支持非默认 HMS Catalog 命名空间。

## 插件目录加载验证（2026-09-14）

生产部署方式是把插件 JAR 放到 FE 的 `authorization_plugins_dir`（默认 `$DORIS_HOME/plugins/authorization`，
散装 JAR，不需要子目录和 manifest），FE 通过 `ClassLoaderUtils.loadServicesFromDirectory` 用 fe-core 的
`ChildFirstClassLoader` 发现工厂。已在测试 FE 上把 JAR 从 `lib/` 移到 `plugins/authorization/` 后重启验证：
启动日志为 `Found Access Controller Plugin Factory: hms-native from directory.`，三个 SQL suite 全部通过，
fe.log 无 NoClassDefFoundError / ClassNotFoundException。两种放置方式（`build.sh` 的 EXTRA_FE_MODULES 进 lib，
或运维放进插件目录）行为一致。

内核细节：发现工厂后 `ChildFirstClassLoader` 会被 close，但其 `findClass` 每次按路径重新打开 JAR 读取字节，
所以插件其余类在首次建 Catalog 控制器时仍能加载。前提是 JAR 文件在 FE 运行期间保持在原路径且内容不变；
不要在运行中原地替换或删除插件 JAR，升级插件要重启 FE。

## 协议实现依据

使用 branch-4.1 自带的 `hive-catalog-shade:3.1.3`，不替换 Hive/Hadoop/Thrift 依赖。
HiveMetaStoreClient 负责建立 transport/SASL 连接；插件通过公开 getTTransport() 接口在连接上构造同协议客户端，
调用旧 `get_database`、`get_table`、`list_privileges`、`get_role_grants_for_principal`，避免 Hive 3 包装方法
发送 `get_table_req` 或给库名添加 Catalog 前缀。权限请求不设置 catName。

- [Hive 1.1 操作权限映射](https://github.com/apache/hive/blob/e60744d017ef79f1b17f474c0b969d4ca5592462/ql/src/java/org/apache/hadoop/hive/ql/security/authorization/plugin/sqlstd/Operation2Privilege.java)
- [Hive 1.1 owner/default 库规则](https://github.com/apache/hive/blob/e60744d017ef79f1b17f474c0b969d4ca5592462/ql/src/java/org/apache/hadoop/hive/ql/security/authorization/plugin/sqlstd/SQLAuthorizationUtils.java)
- [Hive 3 客户端包装行为](https://github.com/apache/hive/blob/rel/release-3.1.3/standalone-metastore/src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java)


## 本次验证与自查

2026-09-11：此前在本 worktree 编译了 FE 和插件主代码。本轮通过 `docker/run.sh` 调用仓库
`run-fe-ut.sh`，共 30 个测试通过（24 个单元/协议测试 + 6 个真实 HMS 集成测试），
失败 0、错误 0、跳过 0。本轮仅增加测试和 Docker 配置，使用 `-Dskip.clean=true -Dmaven.main.skip=true`
复用已编译的主代码；修改主代码后须按上面的普通命令编译。
Checkstyle reactor 校验、构建适配脚本语法和 Git 差异格式检查通过。

| 检查项 | 结论 |
|---|---|
| 接口与范围 | 使用当前 4.1 API，插件 ServiceLoader 发现测试通过，无内核修改 |
| 权限正确性 | 操作类型、owner、身份合并、管理员及 hasGlobal 覆盖测试通过；未支持谓词拒绝 |
| 缓存与并发 | 固定 TTL、撤权、空成功、失败不缓存、容量淘汰、并发同键加载测试通过 |
| 连接与错误 | 创建/RPC/重建均经认证器，失败连接销毁测试通过；真实 Kerberos 续期仍待联调 |
| 名称与兼容 | USER/ROLE 区分、模式 0/1/2、真实 Hive 1.1 SQL 角色归一化已测，详见 CASE_VALIDATION.md |
| 可维护性与性能 | 名称解析、操作映射、权限事实、缓存和连接池分离；无最终决定缓存，无内存上限或吞吐量承诺 |
| 验证边界 | build.sh --fe 成功，Docker HMS 1.1.0 Simple 及真实 FE/BE SQL 回归已执行；Kerberos/多 FE 进程回归未运行 |

保留的接口测试 HMS（集群 SQL 的 HMS 见上文报告）：`hms-auth-test-1789100662-2834358`，`thrift://127.0.0.1:58865`。
Hive `--version` 确认为 1.1.0，Derby schema 1.1.0；服务重启后端口不变。
测试摘要和日志保存在 `target/docker/validation/`，完整服务日志可通过 `docker logs` 及
容器 `/tmp/root/hive.log` 查看。按用户最新要求，所有改动保留在工作区，未提交。

## HMS 认证配置修正验证（2026-09-11）

插件改为复用 HMSBaseProperties 的 Hive 配置与认证器，不再根据 HDFS 凭据创建 HMS 认证器。
验证了 HMS Kerberos 凭据缺失不能由 HDFS 凭据补齐、HMS Kerberos/HDFS Simple、
HMS Simple/HDFS Kerberos、服务 principal 别名及错误认证类型拒绝。

build.sh --fe 和 Checkstyle 通过。run-fe-ut.sh 明确指定 9 个插件测试类，33 项通过，
其中 6 项使用真实 Docker HMS 1.1 Simple；失败、错误、跳过均为 0。
新插件已替换测试 FE JAR 并重启，两个集群 SQL suite 对比通过，BE 仍为 4.1.3。
真实 Kerberos KDC 登录、续期和重连尚未联调；上述配置测试不代表 KDC 联调通过。
证据日志：target/cluster/hive-kerberos-build.log、hive-kerberos-unit-verify.log、hive-kerberos-sql.log。

## 内置系统表修复验证（2026-09-11）

修复了 information_schema/mysql 本地系统对象被当成远端 Hive 表鉴权的问题，
保留目标 Catalog 的 Doris 本地表/列权限检查。35 项插件测试和两个真实 SQL suite 均通过；
build.sh --fe 与 Checkstyle 通过。详细权限边界和证据见 [review/R1-resolution.md](review/R1-resolution.md)。
