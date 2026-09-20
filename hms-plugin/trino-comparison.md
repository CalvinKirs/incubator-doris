# HMS 鉴权插件与 Trino SQL Standard 对比

> 2026-09-11：本文保留早期 SELECT 方案与 Trino 的对比。当前插件范围以 [HMS 插件设计](hms-plugin-design.md)为准：已加入 INSERT、CREATE、ALTER，主要测试 HMS 1.1.0，基于 Doris `branch-4.1` 开发。下文“本插件不做写权限”等旧范围不再适用，缓存刷新方式也以新设计的最终规则为准。

日期：2026-09-10
关联文档：[Doris HMS 表级 SELECT 鉴权功能文档](hms-select-authorization-functional-spec.md)

这份文档主要讲：Trino 怎么做，我们准备怎么做，尤其是连接和缓存有什么不同。目前插件还没实现，默认 TTL、管理员例外、视图和写操作的规则还要定，见功能文档。

**我们这边的前提是不改 Doris 内核。** 用已有的 FE Catalog 鉴权接口接入，插件自己配 HMS、建连接池、管权限缓存。FE/BE、内置 Connector 和 SPI 都不改。

Doris 接口就按 `branch-4.1`，加载用已有能力，打包和安装不用管。下面会比较接入方式，HMS 2/3/4 的兼容性也要测。

## 1. 功能上有什么区别

我们只做 Trino `sql-standard` 里面的“表级 SELECT + USER/ROLE 授权”这一部分。

| 能力 | Trino `sql-standard` | 本插件首期方案 |
|---|---|---|
| 权限来源 | HMS | HMS |
| 查询时执行鉴权 | Trino 执行 | Doris FE 通过插件执行 |
| USER 直接授权 | 支持 | 支持 |
| ROLE 授权与角色继承 | 支持 | 支持 |
| USER 与有效 ROLE 的权限合并 | 取并集 | 相同 |
| `public` | 启用 | 计划支持 |
| `SET ROLE` | 支持 | 不做，采用固定默认角色规则 |
| HMS `admin` | 必须显式启用 | 不自动启用，不新增启用入口 |
| GRANT/REVOKE 写回 HMS | 支持 | 不做，仍在 Hive 侧管理 |
| HMS 角色创建、删除和授予 | 有对应管理能力 | 不做 |
| 写入、DDL 权限检查 | 有对应操作检查 | 本期仅做 SELECT；写操作处理仍须明确 |
| 列级授权、行过滤、列脱敏 | 该鉴权实现未提供 | 不做 |
| 按权限过滤库表名称列表 | 该实现的列表过滤方法直接返回输入集合 | 不新增列表隐藏保证，受 Doris 现有检查影响 |
| SHOW/DESC 的额外规则 | 各元数据操作有各自实现，不能概括成全部不检查 | 不新增 HMS 元数据鉴权，仅做现有接口兼容 |

Trino 还管角色切换、授权管理和视图等操作，范围更大。我们先把普通用户查询 Hive 表时的 USER/ROLE SELECT 判断做好。另外，Doris 管理员和 HMS 的 `admin` 角色是两回事，要分别处理。

依据：[Trino 授权文档](https://trino.io/docs/current/connector/hive.html#sql-standard-based-authorization)、[SqlStandardAccessControl](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/security/SqlStandardAccessControl.java)。

## 2. HMS 连接怎么用

Trino 的鉴权器集成在 Hive Connector 中，通过 Connector 的 Metastore 访问链路读取授权。本插件作为独立的 Doris Catalog 鉴权实现，自己建立 HMS 连接池。

```text
Trino：
  查询鉴权 → Hive Connector 的 SqlStandardAccessControl
           → Connector 的 Metastore 访问链路 → HMS

本插件：
  Doris 现有查询鉴权入口 → CatalogAccessController 插件
                        → 插件的权限缓存与独立 HMS 连接池 → HMS

  Doris Catalog 的元数据读取仍走原有 Connector 路径。
```

| 项目 | Trino | 本插件 |
|---|---|---|
| HMS 配置 | Hive Connector 的配置 | 插件显式配置 |
| 客户端管理 | Connector 的 Metastore 实现负责 | 插件负责创建、借还和释放连接 |
| 元数据与权限访问 | 同一 Connector 体系内 | 两条独立访问链路 |
| 启用方式 | 配置已有 `sql-standard` 模式 | 通过现有机制绑定 Catalog；打包安装不在本次范围 |
| 核心改动 | 已集成在产品中 | 不允许修改 Doris 内核 |

这里的“同一 Connector 体系”不表示 Trino 的所有调用都共享同一个 Thrift 连接或采用与本插件相同的连接池实现。

连接池自己建以后，插件要负责这些事：

- 插件处理自身认证、连接池、超时及依赖隔离。
- 插件的 HMS 端点和命名空间必须与目标 Catalog 对应。
- 插件服务身份需要能读取授权；该身份与被鉴权的 Doris USER 分开。
- 不为每个登录用户建立独立连接池。
- 按 Doris `branch-4.1` 现有接口实现，测试相关鉴权路径的实际行为。

Kerberos 连接沿用现有认证封装：客户端创建/重连和权限 RPC 均通过 `authenticator.doAs()`，按需登录及更新凭据，不自行新增一套票据更新框架。HMS 版本兼容以 2.3.9、3.1.3、4.0.1 的接口核对为起点，另行完成真实服务互通；不以 HiveServer2 版本代替 HMS 版本。`HiveObjectRef.catName` 是 HMS 的远端命名空间，不是 Doris 外部 Catalog 名。具体要求及示例见[功能文档 F02/F03](hms-select-authorization-functional-spec.md)。

依据：[SqlStandardAccessControlMetastore](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/security/SqlStandardAccessControlMetastore.java)。

## 3. 一次查询怎么判断权限

基本判断方式可以和 Trino 一样：先看用户有哪些有效角色，再看目标表的授权。用户自己或任一有效角色有 SELECT，就允许查询。

示例：

```text
alice 的有效身份：
  USER alice
  ROLE analyst
  ROLE public

sales.orders 上拥有 SELECT 的身份：
  ROLE analyst
  USER bob

匹配结果：ROLE analyst → 允许 alice 查询 orders
```

本插件建议流程：

1. 获取当前已认证用户名。
2. 读取角色关系并展开继承，形成有效角色集合。
3. 读取目标表的授权列表。
4. 检查 USER 或任一有效 ROLE 是否有 SELECT。
5. 多表查询逐表判断，任一表不通过则整条查询失败。

不提前生成“全部用户 × 全部表”的最终权限矩阵。角色关系和表授权可以分别缓存，多个用户复用同一表的授权列表。

本插件先按 `branch-4.1` 取得实际远端名称，再将实际 HMS 库表名按大小写不敏感规则规范化，请求和缓存键保持一致。USER 严格区分大小写、原样保留；ROLE 按 Hive SQL Standard 的大小写不敏感规则规范化，身份缓存键包含 USER/ROLE 类型。详细要求见[功能文档 F03.2/F04](hms-select-authorization-functional-spec.md)及 [Hive 用户与角色命名规则](https://cwiki.apache.org/confluence/display/Hive/SQL%2BStandard%2BBased%2BHive%2BAuthorization)。

默认角色规则参考 Trino：USER 直接授权始终参与，普通角色及其继承关系参与，`public` 启用，`admin` 不自动启用。本插件不增加 Trino 的 `SET ROLE` 会话选择能力。

依据：[Trino 角色解析](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/metastore/thrift/ThriftMetastoreUtil.java)、[表权限检查](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/security/SqlStandardAccessControl.java)。

## 4. 缓存什么，怎么配

| 项目 | Trino | 本插件建议 |
|---|---|---|
| 缓存实现 | Trino 自身的 Metastore 缓存封装 | 用 Doris 已有的 Caffeine，现有封装合适就复用 |
| 缓存内容 | 表授权、角色列表、角色授予关系等 | 表授权、角色授予关系及判定必需信息 |
| 配置归属 | 跟随通用 Metastore 元数据缓存 | 插件独立权限配置 |
| 跨查询缓存 TTL | 当前文档默认 `0s` | 建议默认 30 秒，待确认 |
| 缓存过期后重读 | 支持 | 支持 |
| 访问触发异步刷新 | 可配置 | 首期不做 |
| 周期性全量扫描 | 这套缓存机制不是全量同步任务 | 不做 |
| 最终允许/拒绝结果 | 根据授权信息检查 | 首期不额外长期缓存 |
| 实例范围 | Metastore 缓存实例 | 每个 FE 上对应的插件实例 |
| 容量限制 | 有最大缓存大小配置 | 配置最大条目数，并验证实际内存 |

Trino 主要配置是 `hive.metastore-cache-ttl` 和 `hive.metastore-refresh-interval`。权限缓存使用通用元数据缓存策略，不能将统计信息缓存的 TTL 当作权限 TTL。跨查询元数据缓存默认 `0s` 也不表示完全没有事务内缓存。

我们的权限缓存单独配置。调整表结构、分区缓存时，不会顺带改变撤权的生效时间。

缓存库已选定为 Caffeine，不新增通用缓存框架。需注意：本次核对的本地 Doris `CacheFactory` 及 Connector 缓存相关路径采用 `expireAfterAccess`，热点权限可能因访问不断续期。本插件要求 `expireAfterWrite` 的固定有效期；现有封装不支持时，在插件内部使用 Caffeine builder 配置，不修改内核封装。复用缓存库不意味着共享 Catalog 的缓存实例。

依据：[Metastore 配置文档](https://trino.io/docs/current/object-storage/metastores.html#general-metastore-configuration-properties)、[CachingHiveMetastore](https://github.com/trinodb/trino/blob/master/lib/trino-metastore/src/main/java/io/trino/metastore/cache/CachingHiveMetastore.java)。

### 4.1 我们准备缓存这两类数据

```text
角色关系缓存：
  USER alice   → analyst
  ROLE analyst → reader

表授权缓存：
  sales.orders → ROLE analyst:SELECT、USER bob:SELECT
```

每个缓存项受硬 TTL 约束；读取命中不续期。同一张表的授权可以在同一个插件实例内被不同用户复用。

成功读取到的空授权列表也可缓存，从而减少无权限查询产生的 RPC。代价是新增授权同样可能等到缓存更新才生效。

如果把多级角色展开成一个完整集合，这个集合也要跟着原始角色关系到期，不能重新算一遍 TTL。第一版先不长期缓存“用户 + 表 → 允许查询”这种最终结果，省得底层授权变了，还得追着清理这些结果。

## 5. 什么时候刷新，撤权多久生效

### 5.1 Trino：TTL 加可选异步刷新

示例配置，非默认值：

```properties
hive.metastore-cache-ttl=60s
hive.metastore-refresh-interval=10s
```

缓存年龄达到刷新间隔后，下一次访问触发异步刷新；触发刷新的访问仍可能取得旧值。没有访问时，不会仅因到达该间隔就执行全量更新。成功重新加载后开始新一轮有效期。

示例时间线，假设 RPC 在 1 秒内成功：

```text
00 秒：读取并缓存 SELECT 授权
05 秒：Hive 侧撤销授权
12 秒：查询访问旧缓存，触发异步刷新，本次仍可能允许
13 秒：刷新完成，后续读取该缓存可以看到撤权
```

刷新间隔不等于最大撤权延迟。若缓存达到硬过期时间，下一次访问需要重新加载；具体可见性还受请求耗时、在途读取和事务内缓存影响。

依据：[刷新配置说明](https://trino.io/docs/current/object-storage/metastores.html#general-metastore-configuration-properties)。

### 5.2 我们：第一版到期再读

以建议 TTL 为 30 秒为例：

```text
00 秒：加载并缓存角色关系、表授权
05 秒：Hive 侧撤销授权
20 秒：新查询仍可能使用有效的旧缓存
31 秒：新鉴权发现缓存过期，读取 HMS
       读取到撤权 → 拒绝
       读取失败   → 报错
```

这是角色与表授权同时加载时的简化示例；实际需要分别跟踪各缓存项的有效时间。

这样做的好处和代价：

- 没有后台刷新任务和相关状态，更新过程更容易解释与验证。
- Caffeine 使用 `expireAfterWrite`，首期不设置 `refreshAfterWrite`，也不以 `expireAfterAccess` 作为权限时效保证。
- 硬过期后的首次查询需要等待 HMS，可能增加鉴权延迟。
- 合并同一缓存项的并发加载，避免过期时重复发起大量请求。
- 缓存有效期间可以按缓存判断；缺失或过期且读取失败时不放行。
- 授权和撤权都按同样的 TTL 更新，不能只关注撤权。

我们希望做到：HMS 已经能读到新权限后，最多经过一个 TTL，新开始的鉴权就用新权限。不过，设好 Caffeine 的 TTL 还不算做完。正在进行的 HMS 请求、多级角色缓存，也都可能影响生效时间，需要一起设计和测试。

### 5.3 从 Trino 改权限和从 Hive 改权限，有什么不同

Trino 自身执行表 GRANT/REVOKE 时，会失效相关表授权缓存；授予或撤销角色时会失效角色授予关系缓存。这是相关缓存实例上的失效，不能理解为自动通知所有独立 Trino 集群。

本插件不写回 HMS，因此不存在这条由自身授权操作触发的更新路径。Hive 侧的变更主要通过 TTL 后重读发现。

Trino 还有 `flush_metadata_cache` 过程，但不能泛化为所有版本、所有权限关系都立即更新：本次核对的 master 中，`flushCache()` 清理表授权和角色列表，却未包含 `roleGrantsCache`。若未来对齐主动刷新，应按部署版本验证具体覆盖范围。

本插件首期不增加主动刷新命令；后续若增加，应同时覆盖角色关系、表授权及指定 FE 范围。

依据：[缓存及授权失效实现](https://github.com/trinodb/trino/blob/master/lib/trino-metastore/src/main/java/io/trino/metastore/cache/CachingHiveMetastore.java)、[FlushMetadataCacheProcedure](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/procedure/FlushMetadataCacheProcedure.java)。

## 6. 多 FE、内存和 HMS 负载

我们每个 FE 各自缓存、各自访问 HMS。所以撤权后的一小段时间里，有的 FE 可能已经读到新权限，有的还在用有效的旧缓存。第一版要求各个 FE 都在约定时间内更新，不要求同一瞬间切换。

已经通过鉴权并开始执行的查询，首期不会因撤权自动取消。计划和结果复用路径仍须验证，不能因命中查询缓存而跳过新查询所需的权限检查。

权限数据量主要由授权关系条数决定，不只是库表数。角色授权可以被多个用户复用，因此建议缓存原始关系，避免将其全量展开为每个用户的全部表权限。

运行时成本：

| 情况 | 主要成本 |
|---|---|
| 热缓存 | 角色集合计算、授权匹配和内存访问 |
| 冷缓存 | 角色关系及目标表授权的 HMS RPC |
| 多级角色继承 | 可能需要多轮角色读取 |
| 多表查询 | 读取并判断各目标表授权 |
| 多 FE | 每个实例各自承担加载和缓存内存 |
| 缓存同时过期 | 需要请求合并及连接池限制控制重复加载 |

容量上限、TTL 和连接池大小，要结合实际用户数、角色数、授权条数和查询并发来定。表级权限通常比分区元数据小，但还是要设容量上限，也要测刚启动或缓存过期时会打多少 HMS 请求。

## 7. 第一版先做到哪

第一版建议做：USER/ROLE 表级 SELECT、角色继承和 public。插件自己连 HMS，权限按需缓存，设容量上限，到期重读；该读 HMS 时读失败，就报错。

暂不增加：异步刷新、主动刷新广播、授权写回、SET ROLE、行列策略及完整视图管理语义。

还要定的是管理员是否例外、视图怎么查，以及写操作怎么处理，再按定下来的规则在 `branch-4.1` 上测试。SHOW/DESC 已经说好了，只保持现有行为，不加 HMS 权限规则。实现中如果碰到现有接口做不到的地方，就说明具体问题，不能跳过鉴权或改内核来补。Doris 分支和插件加载能力已经定了，不用反复确认。

Trino 部分查的是 483 文档和当时的 master 源码。`current`/`master` 链接以后会变，开发时要固定参考版本，再核对默认值、刷新范围和事务行为。我们这边写的是准备怎么做，代码和性能、兼容性测试还没完成。
