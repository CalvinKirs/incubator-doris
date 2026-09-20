# 大小写规则与测试充分性

之前的覆盖不充分：原集群 SQL suite 全部使用小写用户/库表，USER 大小写只有接口测试，
ROLE 大小写主要由单测构造。不能据此宣称大小写场景已充分覆盖。

新增 `hms_case_authorization` 在当前 FE + 4.1.3 BE + HMS/HiveServer2 1.1.0 + HDFS 上执行。
用例包含 9 组输出及 13 项预期拒绝/解析失败检查；结果文件由仓库回归脚本生成并对比。
另外新增 HmsNameCaseTest，在土耳其语默认 Locale 下验证插件使用 Locale.ROOT，并恢复原 Locale。

## 实际验证矩阵

| 范围 | 构造与判定 | 结论 |
|---|---|---|
| USER 大小写 | 同时创建 HmsCaseUser / hmscaseuser，前者能读 11、后者被拒绝；反转授权并过 TTL 后反转结果 | 用户名必须保留大小写，不能统一小写 |
| 角色成员 USER 大小写 | Hive SQL 给 HmsCaseMember 授角色；同名小写 hmscasemember 不获得角色 | 成员查询和缓存键保留 USER 大小写 |
| ROLE 大小写 | 真实 Hive SQL 创建 HmScAsEuSeR，使用 HMSCASEUSER 授权，再用 hMsCaSeUsEr 撤销 | SQL Standard 存储为 hmscaseuser，角色大小写归一化正确 |
| USER/ROLE 同名 | USER hmscaseuser 只读 user_only=22；ROLE hmscaseuser 的成员只读 role_only=33；交叉访问均拒绝 | 缓存键必须包含 PrincipalType，不能只用名字 |
| HMS 库表命名 | 用 HmS_Case_Db / MiXeD_Table 建元数据，再以全大写 HMS RPC 读取 | 实际存储 hms_case_db / mixed_table；owner 仍为 HmsCaseUser |
| Doris 模式 0 | 原小写名成功；全大写库或表名解析失败 | 不应由插件把不可解析的 SQL 名字强行转小写放行 |
| Doris 模式 1 / 2 | 分别使用全大写、混合大小写库表名读取同一个实际表 | 均返回 11；鉴权跟随解析后的远端对象 |
| 列名、引号、查询别名 | ID / Id，反引号库表名，AS Src；无权源表伪装成有权表名 USER_ONLY | 合法查询返回 11，伪装别名仍被拒绝 |
| 库/表 USER owner | HmsCaseUser CREATE 和 ALTER 成功；小写同名用户 CREATE / ALTER 拒绝 | owner 判断区分用户名大小写 |
| 缓存/撤权 | 两种 USER 和多个 Catalog 预热后授权反转；真实 Hive SQL 撤销混合大小写角色 | 过 TTL 后按新权限判断，不串用身份 |
| 默认 Locale | tr-TR 下 ROLE BILLING_I、对象 BILLING.ITEMS；USER I/i | ROLE/对象得到 ROOT 小写，USER 保持原样、类型键不冲突 |

ROLE 用例经过原生 HiveServer2 1.1 SQL Standard 鉴权器，没有在测试代码里先 lower-case 再冒充验证。
通过 Beeline 1.1 执行 CREATE ROLE / GRANT ROLE / GRANT SELECT / REVOKE ROLE，真实 HMS 保存其结果。
Hive 1.1 不允许在 CLI 中启用 SQL Standard 鉴权，因此使用独立 HiveServer2。

## 覆盖边界

核心大小写鉴权规则的常规边界已覆盖。
非标准客户端直接写入的非规范 ROLE 数据、非 ASCII 身份的端到端场景、Kerberos 名称映射不在本轮覆盖内。

## 本轮执行结果

2026-09-11 15:18:30.267：删除额外配置场景后，脚本重新生成结果，两个 SQL suite 串行正常对比通过，
0 失败、0 跳过。大小写用例 9 组输出和 13 项预期错误；合计 17 组输出和 27 项预期错误。
名称解析单测改为实际大小写场景，run-fe-ut.sh 执行 1 项通过，Maven Checkstyle 阶段通过。

## 复跑与证据

运行目录及 FE/BE/HMS/HDFS 端口见 CLUSTER_SQL_VALIDATION.md。新增：
HiveServer2 容器 hms-auth-sql-hs2，127.0.0.1:34103，Hive 1.1.0，开启 SQL Standard 鉴权。
配置增加 hmsAuthHs2Container 和 hmsAuthHs2Jdbc，已写入本地 regression-conf.groovy。

```bash
JAVA_HOME=/mnt/disk1/gq/jdk17 JAVA_OPTS='-Xmx2g' \
./run-regression-test.sh --conf "$PWD/hms-plugin/target/cluster/regression-conf.groovy" \
  --run -d hms -s hms_native_authorization,hms_case_authorization -parallel 1
```

必须串行，原 suite 会停止/重启专用 HMS。fixture 在每轮开始重建，结束保留。

- SQL 用例：regression/suites/hms/hms_case_authorization.groovy
- 脚本生成结果：regression/data/hms/hms_case_authorization.out
- Locale 单测：src/test/java/org/apache/doris/plugin/hms/HmsNameCaseTest.java
- SQL/原生 Hive/单测/Checkstyle 日志：target/cluster/case-*.log（本次删除后的复验为 case-removal-*.log）

未提交代码，未修改插件生产代码或 Doris 内核。
