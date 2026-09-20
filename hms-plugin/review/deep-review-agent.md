# HMS 插件独立深度审查

审查日期：2026-09-11。范围：当前 worktree 的 `hms-plugin` 生产实现、单元/协议/Docker 测试、两个 Groovy SQL suite，以及 branch-4.1 的实际权限调用链。仅新增本报告，未改实现、测试或集群状态，未提交。

结论：确认 **1 个 P2 功能兼容问题**：Catalog 内置 `information_schema` 表被当成远端 Hive 表鉴权，正常元数据查询报错。本轮未确认 SELECT/INSERT/OVERWRITE/CTAS 的权限绕过；这不是完整的安全无缺陷证明。

已读 `AGENTS.md`、`SESSION_CONTEXT.md`、`hms-plugin-design.md`。根目录未提供 `SECURITY.md` / `threat-model.md`，当前可用技能中没有要求的 `code-review` 技能，不假称按照不存在的威胁模型分类 Doris 漏洞。SHOW/DESC 放行、HMS admin 角色排除，以及已推迟的显式刷新和生命周期资源释放均按既定范围处理；不重新引入已删除的名称映射要求。

## 确认问题 R1：P2 — 内置 information_schema 查询被错误拒绝

直接位置：`hms-plugin/src/main/java/org/apache/doris/plugin/hms/HmsNameResolver.java:63`。

原文：

```java
        if (!(target instanceof HMSExternalTable)) {
            throw new HmsAuthorizationException("HMS authorization: cannot resolve Hive table " + table);
        }
```

调用位置：`hms-plugin/src/main/java/org/apache/doris/plugin/hms/HmsAccessController.java:123`。

```java
            HmsOperation operation = HmsOperation.resolve(wanted, wanted == PrivPredicate.SELECT ? null : plan.get());
            HmsObjectName name = names.remoteTable(ctl, db, tbl);
```

**可达条件与复现**：普通 Doris 用户查询安装本插件的 HMS Catalog 内置元数据表；默认管理员绕过关闭时，本地管理员也会走同一条路径。主 agent 在现存 FE/BE 集群通过只读 SQL 复核：

```sql
-- 用户 hmscaseuser，FE 127.0.0.1:43027
SELECT table_name FROM hms_case_sensitive.information_schema.tables LIMIT 1;
-- ERROR 1105: HMS authorization: cannot resolve Hive table tables

SHOW TABLES FROM hms_case_sensitive.hms_case_db;
-- 正常列出五张表

SELECT table_name FROM internal.information_schema.tables LIMIT 1;
-- 正常返回 tables
```

主 agent 保存的本轮只读验证记录：`hms-plugin/review/read-only-sql-probes.txt`。

**内核证据**：`fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalCatalog.java:946` 实际为此库创建本地系统对象，而不是 HMS 数据库：

```java
        if (localDbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            return new ExternalInfoSchemaDatabase(this, dbId);
        }
```

其表是 `ExternalInfoSchemaTable`，不是 `HMSExternalTable`。`fe/fe-core/src/main/java/org/apache/doris/nereids/rules/analysis/UserAuthentication.java:101` 按该表所属 Catalog 调用插件；有投影列时走 `checkColumnsPriv`，插件再转 `checkTblPriv`，无列时直接检查表权限：

```java
        if (CollectionUtils.isEmpty(authColumns)) {
            if (!accessManager.checkTblPriv(connectContext, ctlName, dbName, tableName, PrivPredicate.SELECT)) {
```

`RoleManager.java:248` 的既有默认用户角色逻辑还明确注释为 `// grant read privs to database information_schema & mysql`。上述行为不是在 HMS 中补 GRANT 就能修复的问题：插件在发 HMS RPC 前已经因本地对象类型报错。

**影响**：使用 Catalog 内置 `information_schema.tables` 等表发现元数据的 SQL 和客户端元数据浏览流程不可用；SHOW TABLES 正常不代表这一入口也正常。没有据此声称发生数据越权。

**建议**：在插件内部区分真正的 Doris 内置系统表和远端 Hive 表，对确认的系统表沿用 Doris 系统表权限语义；不要仅按用户输入的库名作无条件放行，也不要放宽所有非 HMS 表的类型检查。无需修改 Doris 内核。修复后验证普通账户元数据查询正常、真实 Hive 表缺失 SELECT 仍拒绝，并核对系统表本身需要的本地权限与筛选规则。

**现有测试遗漏原因**：`HmsNameResolverTest` 仅 mock `HMSExternalCatalog/HMSExternalDatabase/HMSExternalTable` 和缺失对象；两个 Groovy suite 的查询对象均为真实 Hive 表，没有 SELECT Catalog 内置系统表。类型判断在这些测试中只表现为正确的“拒绝未知对象”，没有覆盖合法的本地系统表类型。

## 核心检查结论

| 检查项 | 代码核对结论与实际限度 |
|---|---|
| SELECT / 空投影 / 本地全局权限 | `AccessControllerManager` 的表、列检查都会调用带 `hasGlobal` 的插件入口，插件覆盖后不使用该全局值；`UserAuthentication` 对空列集合也查表权限。主 agent 只读复核无权表的 COUNT、常量投影、CTE、EXPLAIN 均拒绝，有权表 COUNT 成功。未发现这些路径的绕过。 |
| SQL 结果缓存 | `NereidsSqlCacheManager.tryParseSql:341` 调用 `privilegeChanged`；后者在 `:588` 对记录的表再次调用 `UserAuthentication.checkPermission`。读取结果缓存前仍有插件权限检查，不能仅因命中 SQL 缓存就推断跳过撤权。现有热查询测试没有证明每次实际命中了结果缓存。 |
| INSERT / OVERWRITE | `InsertIntoTableCommand:271` 与 `InsertOverwriteTableCommand:203` 都检查目标 LOAD；插件通过原始命令区分追加与覆盖，并在覆盖要求 INSERT+DELETE。用户、角色授予的不同权限按同一身份集合合并。实际 SQL suite 已覆盖追加与覆盖的允许/拒绝。 |
| CREATE / CTAS | 普通 CREATE 在目标表尚未存在时只解析已有库，符合 `CreateTableInfo:453` 调用时机。`CreateTableCommand:134` 在同一 executor 中运行内部 INSERT，插件可以识别 CTAS 并检查目标库资格及实际表 USER owner；CTAS 的源查询先经过 planner 鉴权。现有真实 SQL 已覆盖建表、CTAS、源 SELECT 撤销和 owner 变化。 |
| PREPARE 上下文 | `ExecuteCommand:134` 在执行底层命令前把 executor 的 parsed statement 换成实际 logical plan，因此不能据“外层是 ExecuteCommand”推断 LOAD 必然误判。未运行专门的服务端预处理 SQL 测试。 |
| ALTER | 允许的结构/属性操作按 owner；ADD/DROP PARTITION 分别映射 INSERT/DELETE；未知子类抛错。实际内核已有的远端 ALTER 执行限制已写明，不把限制归为插件实现缺陷。现有成功 SQL 为 `auto_analyze_policy`，不代表完成远端 schema ALTER 联调。 |
| USER / ROLE / owner | USER 保留原大小写，ROLE 使用 `Locale.ROOT`，主键包含 PrincipalType。库 owner 保留 USER/ROLE 类型；目标 Hive 1.1 表 owner 按 USER；所有权不自动展开 SELECT。角色图去重可终止环，public 加入，admin 及仅经 admin 可达的路径排除。与当前约定一致。 |
| 名称解析 | 先解析实际 Catalog/db/table 对象再取 remote name，之后按 ROOT 归一化；正常模式 0/1/2、引号与 SQL 别名已有真实 SQL 覆盖。确认缺陷见 R1，合法系统表类型没有被区分。 |
| 缓存 / 撤权 | 三个缓存均 `expireAfterWrite`；不保留展开角色闭包或最终 allow；TTL=0 使用立即过期配置；值复制为不可变集合。加载异常直接传播，不缓存为空成功。已有固定时钟、角色撤销、热 SQL 撤销及容量测试。尚不能把同键并发测试当作“撤权发生于在途加载期间”的验证。 |
| 连接与恢复 | Commons Pool 借用保证单连接不并发执行 Thrift；建连和 RPC 都经 authenticator；异常使连接 invalidated，后续借用新建。现有模拟测试及真实停机重连覆盖串行路径。恢复后旧空闲连接的第一次调用仍可能失败，然后后续调用恢复，符合当前代码机制。 |
| Hive 1.1 兼容 | 插件复用 Hive transport/SASL，使用旧 get_table/get_database/权限 RPC，不设置 catName；本地协议测试覆盖 binary/compact，真实 Docker 和 SQL fixture 为 Hive 1.1 Simple。不能从这推导真实 Kerberos 或其他 HMS 版本已验证。 |
| 配置变更 / 实例隔离 | 核对 `CatalogIf.notifyPropertiesUpdated:96` → `ExternalCatalog.resetToUninitialized:629` → `onClose:823` → `removeAccessController`；后续查找会重建插件。已排除“ALTER CATALOG 永久保留旧控制器”的静态疑点。插件 URI 和元数据 URI 由管理员按已声明契约配成相同源，不将违反契约的误配置虚构为普通用户攻击。 |
| 可维护性 / 构建边界 | 生产逻辑按名称、操作、身份、事实缓存、连接读取分层；工厂使用既有 SPI；POM 依赖 FE 为 provided。未发现需要内核补丁的实现改动。已有 Checkstyle 与 build.sh 证据，本轮只读审查没有重新编译，也没有声称重新运行完整测试。 |

## 后续验证建议，非确认缺陷

1. **在途加载与撤权时间边界**：`HmsAuthorizationTest.java:171-174` 在提交第二个任务后立即释放第一请求。该测试可能在第二任务尚未进入缓存时就完成首个加载；此时“同一对象/只加载一次”只能证明第二次命中缓存。若需要证明在途请求合并，应让两个调用确实重叠，并单独安排“旧事实已读取、撤权、延迟响应、完成加载”的测试，记录 TTL 从缓存写入计时的实际撤权窗口。这是测试证明不足，不据此断言 Caffeine 本身有并发 bug。
2. **连接池并发/失败竞争**：现有 `PooledHmsPrivilegeSourceTest` 的模拟认证器使用单个 AtomicBoolean，测试路径是串行的；补充多个不同 key 并发借连接、部分连接失败时其余调用仍正确且池容量不泄漏的场景，才可证明池实际并发行为。
3. **更完整的真实写入身份回归**：已有真实 Thrift 测到 ROLE 库 owner，真实 SQL CREATE/CTAS 主要使用 USER owner；角色库所有者经 Doris 实际 CTAS 的允许/撤销，以及服务端 PREPARE 的追加/覆盖，仍适合补入 SQL suite。当前静态调用链没有证明这些场景存在错误。
4. **运行范围**：Kerberos 登录/续期/重连、多 FE 分别缓存的撤权时间仍未联调，文档已如实声明。这些缺口不能用 Simple 模式加模拟 doAs 测试代替。

未对已推迟的生命周期释放、显式刷新提出本期修复要求；未运行或声称完成 Apache Doris CI Code Review Runner。

## 后续修复记录

R1 已于 2026-09-11 修复并通过单测和真实 SQL 复验，详见 [R1-resolution.md](R1-resolution.md)。
以上发现及行号保留为修复前审查记录。
