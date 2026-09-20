# R1 系统表兼容修复

原审查报告 deep-review-agent.md 的位置和 SQL 证据对应修复前代码，保留作为历史记录。

## 行为

HMS Catalog 中实际属于 Doris ExternalInfoSchemaDatabase/Table 或 ExternalMysqlDatabase/Table
的对象，SELECT 转交 Doris 内部控制器，保留原 Catalog、库、表及列参数。普通 Hive 查询
继续读取 HMS 角色和授权；仅凭 information_schema/mysql 字符串不会放行。

用户默认角色的系统库 SELECT 仅针对 internal。原审查中的 hmscaseuser 没有目标外部
Catalog 的本地系统库授权：修复后这类查询应得到本地权限拒绝，而非 Hive 对象解析异常。
新回归明确授予目标系统库权限后验证查询成功，并撤销同一权限验证正确拒绝。
没有把修复实现为任意用户无条件读取系统表。

## 验证

- build.sh --fe 与 Checkstyle 通过；更新测试 FE 插件 JAR 并重启，BE 保持原 4.1.3。
- run-fe-ut.sh 明确指定 9 个插件测试类，35 项通过，0 失败/错误/跳过，包含 6 项真实 Docker HMS Simple 测试。
- 新增单测覆盖实际系统对象识别、同名 HMS 对象不误判、缺失对象、本地表/列允许与拒绝、
  hasGlobal 参数不能替代实际本地检查、系统表权限检查不调用 HMS。
- 真实 SQL 已验证表名列表为 public_table/source_table/target_table、计数 3、列名 id，
  mysql.user 查询合法但本用例返回空集；本地 SELECT 撤销、cluster_snapshots 特殊权限、
  系统表与无权 Hive 表联查都被拒绝。
- 生成用例通过；2026-09-11 21:11:41.245 最终双 suite 正常对比通过，0 失败/跳过。
  合计 23 组输出、31 项预期错误；功能 suite 为 14/18，大小写 suite 为 9/13。

实现：src/main/java/org/apache/doris/plugin/hms/HmsAccessController.java、HmsNameResolver.java。
SQL：regression/suites/hms/hms_native_authorization.groovy。
证据：target/cluster/system-tables-build.log、system-tables-unit.log、
system-tables-generation-verify.log、system-tables-verify.log。

真实 Kerberos 联调仍未完成；客户已明确平台密钥认证不属于本插件范围。未修改内核、未提交。
