# HMS 插件集群 SQL 验证

2026-09-11 14:44:58 +08:00，仓库 `run-regression-test.sh` 正常对比校验通过：
1 suite，0 失败，0 fatal，0 跳过；8 组有序查询结果、14 项预期拒绝/故障检查。
首次结果由回归脚本生成，随后不带生成参数重新执行并比较通过。

## 运行环境

| 组件 | 版本 / 位置 | 端口 |
|---|---|---|
| FE | 本 worktree branch-4.1，基线 a060d016459；build.sh --fe 构建；运行于 /mnt/disk2/gq/doris-release/hms-auth-fe | MySQL 43027，HTTP 42027，RPC 43017，edit log 43007 |
| BE | 官方 4.1.3，7126cf65d96；/mnt/disk2/gq/doris-release/be；直接复用二进制和 Java 扩展 | heartbeat 43047，BE 43057，HTTP 42037，brpc 42057 |
| HMS | Hive 1.1.0 + Derby；Docker hms-auth-sql-hms | 34102 |
| HDFS | Hadoop 2.7.4；Docker hms-auth-sql-hdfs；1 NameNode + 1 DataNode | RPC 34097，DataNode 34099 |

4.1.3 软件包从本机已有 `apache/doris:all-in-one-4.1.3` 镜像提取到用户指定的
`/mnt/disk2/gq/doris-release`。原始发行版 FE 仍保留在该目录的 `fe/`；测试运行的是 `hms-auth-fe/`。
当前 FE 日志版本显示 `doris-4.1.4-rc04-Unknown`，来源为本 worktree 的 branch-4.1，未替换内核类。
独立端口使用同一偏移量 33997；FE/BE 通过 127.0.0.1 通信。

Catalog `hms_auth` 关闭管理员绕过，`hms_auth_admin` 开启；均连接相同 HMS，缓存 TTL 为 2 秒。
元数据 Catalog 显式设置 `hive.version=1.1.0`，启用 Doris 已有的旧 HMS 协议路径。
插件 JAR 由标准 build.sh 的 EXTRA_FE_MODULES 机制打包到 FE lib，启动日志确认发现 hms-native 工厂。

## 实际结果

| 场景 | 结果 |
|---|---|
| 有 SELECT 的 reader 读取 HDFS 表 | 返回 1、2、3 |
| reader 无 INSERT / CREATE / 表 owner | INSERT、CREATE、ALTER 被拒绝 |
| 本地全局 SELECT + HMS admin 角色，无有效 HMS SELECT | 查询被拒绝；HMS admin 不自动启用 |
| public 角色授权 | 普通用户返回 1、2、3 |
| 本地 ADMIN 绕过开关 | root 在关闭的 Catalog 被拒绝，开启时查询成功；普通用户仍被拒绝 |
| writer 执行 INSERT VALUES、INSERT SELECT | 目标 Parquet 表返回 1、2、3、7 |
| INSERT OVERWRITE 缺 DELETE | 被拒绝，原数据保留 |
| 补充 DELETE 后 INSERT OVERWRITE | 执行成功，目标表只剩 8 |
| 库 owner CREATE TABLE / CTAS | 建表成功，CTAS 数据为 1、2、3 |
| owner ALTER auto_analyze_policy | 成功；在 HMS 更换表 owner 后旧 owner 被拒绝 |
| INSERT SELECT / CTAS 源表撤销 SELECT | 两种 SQL 都被拒绝 |
| USER → team ROLE → leaf ROLE | 角色用户读到 1、2、3；撤销角色并过 TTL 后被拒绝 |
| SELECT 撤权，持续发送 SQL | 固定 TTL 后拒绝；两个 Catalog 实例均拒绝 |
| HMS 停机且缓存过期 | SQL 返回 HMS authorization: failed to read role grants |
| HMS 恢复，同一 FE | 自动重连，查询再次返回 1、2、3 |

FE 审计日志中实际查询包含 `ScanRows=3`、`ReturnRows=3`、`isHandledInFe=false`，
对应 HDFS 文件扫描经 BE 执行。审计证据提取在 `target/cluster/sql-audit-evidence.json`。

ALTER 成功场景是内核支持的 `auto_analyze_policy`。当前 HiveMetadataOps 不支持远端改列、改名、
改表属性，插件没有扩展这些执行能力。未进行 Kerberos 或多个 FE 进程验证。

## 复跑

从当前 worktree 根目录执行；suite 会重建专用 fixture，并停止/启动 hms-auth-sql-hms。
用户、Catalog、HDFS 文件、HMS grants 都由 suite 准备，结束后保留用于排查。

```bash
JAVA_HOME=/mnt/disk1/gq/jdk17 JAVA_OPTS='-Xmx2g' \
./run-regression-test.sh --conf "$PWD/hms-plugin/target/cluster/regression-conf.groovy" \
  --run -d hms -s hms_native_authorization

mysql -h127.0.0.1 -P43027 -uroot
```

- suite：`regression/suites/hms/hms_native_authorization.groovy`
- 自动生成并校验的结果：`regression/data/hms/hms_native_authorization.out`
- 正常校验日志：`target/cluster/regression-verify.log`
- FE 构建日志：`target/cluster/fe-build.log`
- FE/BE 状态：`target/cluster/cluster-status.txt`
- 环境端口：`/mnt/disk2/gq/doris-release/hms-auth-ports.json`
- HDFS/HMS 配置与启动脚本：`/mnt/disk2/gq/doris-release/hms-auth-hadoop/`

FE 构建命令：

```bash
MAVEN_OPTS='-Xms512m -Xmx5g -XX:MaxMetaspaceSize=1g' \
MAVEN_ARGS='-Dskip.clean=true -Dmaven.test.skip=true' \
CUSTOM_MVN="$PWD/hms-plugin/maven.sh" EXTRA_FE_MODULES='hms=../hms-plugin' \
DISABLE_BUILD_UI=ON DISABLE_BE_JAVA_EXTENSIONS=ON FE_MAVEN_THREADS=2 ./build.sh --fe
```

只为 FE 构建前置检查，从自建 Hadoop 2.7.4 镜像复制真实 libhdfs.a 到 worktree 的第三方目录。
本轮没有编译 BE。未提交代码，未修改 Doris 内核。

## 大小写覆盖补充

2026-09-11 15:18:30.267：删除额外配置场景后，hms_case_authorization 与本 suite 串行对比校验通过，
2 suites，0 失败/跳过。日志：target/cluster/case-removal-verify.log。
删除额外配置场景后，新用例 9 组输出和 13 项预期错误，详细矩阵见 [CASE_VALIDATION.md](CASE_VALIDATION.md)。
新增独立 HiveServer2 1.1.0（hms-auth-sql-hs2，34103），用于实际 Hive SQL 大小写角色操作。

## Hive 专用认证配置修正后的复验

2026-09-11 15:47:59.113：构建并更新插件后重启 FE，两个 suite 正常对比通过，0 失败/跳过。
插件 Simple 配置改用 hive.metastore.authentication.type / hive.metastore.username。
BE 保持 4.1.3，数据集和结果文件未改。日志：target/cluster/hive-kerberos-sql.log。
该 SQL 集群仍为 Simple 模式，不是 Kerberos 联调。

## 系统表权限兼容修复复验

2026-09-11 21:11:41.245：更新插件并重启测试 FE 后，两个 suite 正常对比通过，0 失败/跳过。
功能 suite 新增 6 组输出、4 项预期错误，现在 14 组输出、18 项预期错误；
加上大小写 suite 合计 23 组输出、31 项预期错误。结果文件由仓库脚本重新生成。
覆盖系统表的本地授权/撤权、表/列元数据和 COUNT、mysql 元数据查询、
受限系统表及与无权 Hive 表联查；Hive 数据权限仍按 HMS 检查。
单测 35 项通过，含 6 项真实 HMS Simple 测试；build.sh --fe 与 Checkstyle 通过。
详细结果见 review/R1-resolution.md，日志为 target/cluster/system-tables-*.log。
