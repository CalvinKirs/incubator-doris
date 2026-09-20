# Kerberos 专项验证

测试文件独立于 Simple 套件：

- `src/test/java/org/apache/doris/plugin/hms/HmsKerberosIntegrationTest.java`
- `regression/suites/hms/hms_kerberos_authorization.groovy`
- `regression/data/hms/hms_kerberos_authorization.out`，由仓库回归脚本生成。
- 环境脚本与复跑步骤：[`docker/kerberos/README.md`](docker/kerberos/README.md)。

## 环境与认证边界

KDC 容器 `hms-auth-krb-kdc`，独立 Hive 1.1 HMS 容器 `hms-auth-krb-hms`，
SASL endpoint `thrift://127.0.0.1:47321`；KDC 端口 43361。
Realm `HMS.AUTH.TEST`，服务 principal `hive/localhost@HMS.AUTH.TEST`，
普通客户端 `doris-hms@HMS.AUTH.TEST`，60 秒 TGT 客户端 `doris-renew@HMS.AUTH.TEST`。
密钥随机生成，仅保存在忽略的 `target/kerberos/runtime/`，未进入源文件。

FE 使用当前 branch-4.1 的既有构建及已部署插件，MySQL 43027；BE 继续使用
`doris-4.1.3-rc02-7126cf65d96`。本轮仅增加测试和 Docker 配置，未改插件生产代码及 Doris 内核。
FE JVM 指向独立 krb5.conf；主机 `/etc/krb5.conf` 未修改。

HDFS 仍为 Simple。测试 HDFS 仅增加本机、指定测试用户的 Hive 服务端代理规则及名称映射。
这属于 Hive 1.1 Secure HMS 服务端访问文件系统的行为；插件不会按 Doris 查询用户名创建 proxy UGI。
插件用服务 principal 登录，并将查询用户名作为 USER/ROLE 权限判断的身份。

## 覆盖矩阵

| 场景 | 验证方式 |
|---|---|
| 正确 principal/keytab 与服务 principal | 真实 KDC 登录、SASL RPC、FE/BE 查询 |
| HMS 凭据独立于 HDFS | Java 放入不可用 HDFS 凭据仍可查询 HMS；SQL Catalog 的 HDFS 为 Simple |
| 服务身份与查询身份分离 | 服务用户拥有 SELECT，但无授权的 Doris 用户仍拒绝 |
| 大小写与角色 | `KrbReader`/`krbreader` 分离；角色授权查询及撤权 |
| 实际读写 | HDFS 文件读取，INSERT 写 Parquet 后读取；无写权限、无 DELETE 的 OVERWRITE 拒绝 |
| TTL 撤权 | 用户授权、角色成员关系撤销后，超过 TTL 拒绝；重新授权后恢复 |
| 错误 keytab | 使用另一个 principal 的 keytab，Java 与 SQL 都拒绝 |
| 缺失 keytab | Java 与 SQL 都拒绝 |
| 错误服务 principal | KDC 找不到目标服务，Java 与 SQL 都拒绝 |
| Simple 客户端连接 SASL HMS | 首次 RPC 被服务端拒绝；不能降级获得授权 |
| 票据刷新 | 同一认证器、UGI、连接池连续 RPC，超过原 TGT 到期时间；读取 Subject 内 TGT 时间确认更新 |
| FE 票据刷新 | 同一个 Catalog 首次查询后等待超过 60 秒，再执行查询；KDC 记录新 AS_REQ |
| SASL 重连 | 停止独立 HMS，原连接报错；重启后同一池重新认证并成功查询 |

错误 SQL Catalog 的元数据凭据保持正确，仅改变插件的认证配置，因此拒绝来自权限插件。
认证失败的两条路径分别检查 `cannot borrow client` 和 `failed to read role grants`。
正常无权限场景检查 Doris/HMS 的权限拒绝消息。

## 证据

首轮 SQL 结果生成于 2026-09-11 21:37:35 通过，6 组结果均为 `11, 22, 33`，10 个预期错误。
KDC 在 13:37:34 UTC 为 FE 使用的 `doris-renew` 重新签发 TGT；该时段没有同时运行 Java 用例。
原有 Simple 与大小写 SQL 套件于 21:39:00 正常对比通过，2 suites，0 failed/fatal/skipped。

最终 Java 与 Kerberos SQL 对比结果见下方追加记录。
日志保存在 `target/kerberos/`：`unit-final.log`、`sql-generation.log`、`sql-verify.log`、
`simple-verify.log`、`style-final.log` 和 `kdc-*.log`。
Checkstyle reactor 校验通过；Docker shell 脚本通过 `bash -n`，配置生成脚本实际执行通过。

环境接入阶段修正了测试 HDFS principal 名称映射、回归 JVM 的 Java 17 opens 参数、
回归框架 Hive/Thrift wrapper 不兼容和预期错误文本。生产插件未作修改。
回归 fixture 直接用 GSSAPI transport 管理原生 HMS 数据，生产插件继续使用 Doris 的 HiveMetaStoreClient。

这轮验证不覆盖多 FE 进程、跨 realm、KDC HA、keytab 在线轮换或长期运行稳定性。
显式缓存刷新及生命周期资源释放仍按既有范围延后。

## 追加记录

- 2026-09-11 21:40（unit-final.log）：HmsKerberosIntegrationTest 7 项通过，0 失败/错误/跳过。
- 2026-09-11 21:37（sql-generation.log）：hms_kerberos_authorization 生成并通过，1 suite，0 失败/fatal/跳过；
  21:39（simple-verify.log）两个 Simple suite 对比通过。
- 2026-09-14：在含重试、指标、DDL owner 判定的插件上复跑，Kerberos Java 7 项通过，Kerberos SQL suite
  0 失败/fatal/跳过（hms-plugin/target/rerun/krb-unit.log、sql-krb-drop.log）。
  Java 用例需先 `source target/kerberos/runtime/fixture.env`，否则被 JUnit 条件跳过。
