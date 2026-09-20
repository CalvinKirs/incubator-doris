# HMS 鉴权插件设计

更新日期：2026-09-11。当前开发范围以本文为准；另外两份文档保留前期讨论和 Trino 对比。

## 功能概览

- 支持 HMS 鉴权，鉴权粒度（库表），不考虑行列权限以及Datamask。
- 只支持hive 表就可以（理论上iceberg 也适用 本期不关注，不测试这个）
- 不修改内核，固定基于 Doris `branch-4.1` 的现有鉴权框架开发。使用已有插件加载能力，打包安装不属于这次工作；本期不做其他 Doris 分支或 5.0 的适配。
- 用户名称和doris 用户名称完全一致。严格区分大小写。（HMS user 是严格区分大小写的）
- 主要联调和测试客户的 HMS 1.1.0。HMS 2.3.9、3.1.3、4.0.1 保留接口核对结果，未实际测试的版本不写成已验证支持。
- 客户已确认使用 Hive 原生授权。插件从 HMS 读取 USER/ROLE 授权、角色关系和库表所有者，不对接其他授权系统。Doris 侧不管理这些外部授权，Doris 的角色不参与 HMS 角色合并。
- Doris 本地管理员是否跳过 HMS 检查，由每个 Catalog 的插件配置控制，具体见下文。普通用户的本地全局 SELECT 不替代 HMS 检查。
- 暂不处理 Catalog 删除、重建时的连接池和后台任务主动释放，不作为本期开工前提。正常请求仍要归还连接，坏连接仍要销毁和重建。
- 显式刷新鉴权缓存暂不做。当前只实现插件，权限通过 TTL 到期后按需重读更新，不新增 SQL、HTTP 刷新入口或跨 FE 刷新广播，不修改内核。
- 第一版支持 SELECT、INSERT、CREATE、ALTER 鉴权，覆盖 `branch-4.1` Hive Catalog 已支持的相应操作，不再按只读插件设计。具体 SQL 和权限对应见下文。
- 2026-09-14 补充：DROP TABLE、TRUNCATE TABLE、DROP DATABASE 按 Hive SQL Standard 以 owner 判定；CREATE DATABASE 与 Hive 一致对所有用户放开。内核建库不写 HMS owner 是已知基线限制，见 README。

## 关键功能点具体设计

### 缓存

  使用 Caffeine，第一版按 `expireAfterWrite` 固定 TTL 实现：命中不续期，缺失或过期时从 HMS 重新加载。暂不增加显式刷新、异步刷新或定时全量刷新任务。加载失败时报错，不使用过期授权放行。不做复杂的内存计量，增加 `cache.maximum.size` 控制每个缓存实例的最大条目数，达到上限后由 Caffeine 淘汰条目。具体内存还取决于每个条目里的授权数量，不能只根据表数、用户数推算。
  - 表权限信息缓存：表授权列表，以及 ALTER 判断需要的所有者等信息；保留 SELECT、INSERT 等所需权限，不在加载时只留下 SELECT。
  - 角色关系缓存：`USER → ROLE` 和 `ROLE → ROLE`。
  - 库权限信息缓存：CREATE 判断需要的库所有者，以及所选授权规则实际需要的库授权信息。
  简而言之：角色关系单独缓存，表授权按表缓存，查询时再匹配。
  ① 读取 orders 的表授权，筛出有 SELECT 的身份
       → ROLE analyst
       → USER bob

    ② 获取 alice 本人和她当前启用的角色
       → USER alice
       → ROLE analyst
       → ROLE public

    两边有交集：ROLE analyst
    → 允许查询


### 隔离
- 每个 FE 上，每个 Catalog 插件实例有自己的 HMS 池和缓存。

### 认证
- Kerberos
- Simple
这里直接用内核现成的框架，支持多kerberos+自动续期
## 功能项

编号
功能
第一版做什么
依赖
F01
Catalog 鉴权模式管理
启用、参数校验和配置生效处理；生命周期资源主动释放暂不做
无
F02
插件 HMS 连接管理
插件独立配置连接、认证和连接池，访问权限 API
F01
F03
HMS 权限读取 映射 转化
读取 USER/ROLE 授权、角色关系、库表所有者，转换为内部权限数据
F02
F04
用户与角色权限合并
解析有效角色，将直接授权与角色授权合并判断
F03
F05
库表 SELECT / INSERT / CREATE / ALTER 鉴权

检查源表读取权限、目标表写入或修改权限，以及目标库的建表资格；复合语句的各项检查都要通过
F01、F04
F06
权限缓存与更新
按需缓存、容量限制、TTL、撤权及多 FE 行为
F03、F04
F07
错误处理、日志和指标
区分无权限和读取失败，提供日志与缓存指标
F02—F06
F08
与现有 Doris 行为的配合
本地权限关系、对象解析、视图及模式外行为
F01、F05
F09
测试和使用说明
单元测试、真实 HMS 回归、配置和运维说明
F01—F08



## SELECT、INSERT、CREATE、ALTER 怎么判断

支持这四类操作已经确定，客户也已确认使用 Hive 原生授权。下面按 Hive SQL Standard 规则设计；联调验证实际返回的授权和所有者信息，不再把权限来源作为待确认项。

| 操作 | Doris 4.1 已有检查入口 | 插件需要判断什么 |
|---|---|---|
| SELECT | SELECT | 当前 USER 或有效 ROLE 是否有源表 SELECT |
| INSERT INTO | 目标表 LOAD | 当前 USER 或有效 ROLE 是否有目标表 INSERT；INSERT SELECT 还要逐张检查源表 SELECT |
| CREATE TABLE | 新表名称上的 CREATE | 检查目标库的建表资格；按 SQL Standard 以库所有权为基础，不能查询尚不存在的新表授权来决定 |
| CREATE TABLE AS SELECT | 建表检查及源查询检查 | 目标库建表资格和所有源表 SELECT 都要通过 |
| ALTER TABLE | 目标表 ALTER | 普通结构、属性修改按表所有权判断；不能把 Doris 的 ALTER 直接当作 HMS 中同名的可授予权限 |

库所有者按 HMS `ownerName` / `ownerType` 与 USER、有效 ROLE 匹配。表所有者按目标 HMS 的实际字段和 SQL Standard 规则解析，不能假定 `list_privileges` 一定返回一条名为 OWNERSHIP 的记录。因此权限读取还需要已有的 `get_database` / `get_table` 接口，并缓存判断所需的所有者信息。

SQL 子类型需要逐项映射：Hive 1.1.0 中 INSERT OVERWRITE 要求 INSERT 和 DELETE，ALTER ADD PARTITION 和 DROP PARTITION 也分别涉及 INSERT、DELETE，不能把所有 ALTER 都归成表所有权。Doris 的 LOAD 谓词也不能独自区分追加、覆盖等操作。开发时要列出客户使用、且 Hive Catalog 已支持的具体 SQL，通过 `branch-4.1` 现有可用上下文完成区分并测试，不新增内核接口，不因同属 LOAD/ALTER 就少检查权限。

建表后还要验证 HMS 中实际记录的 owner 和初始授权。Catalog 的服务身份与 Doris 登录用户可能不同，不能默认新表 owner 就是当前登录用户；鉴权插件仍不执行 GRANT/REVOKE 写回。目标库为 default 的特殊行为按客户 Hive SQL Standard 配置核对；本地管理员按下面的配置处理，普通用户不能因本地全局权限而跳过 HMS。

这里的库级判断用于对应的库操作和建表资格，不自动把库所有权或某条库授权展开成所有表的 SELECT。对不存在的目标表，先映射已有的目标库，再按远端命名规则处理待创建的表名，不要求先加载出新表对象。

## 使用方式

Catalog 中声明鉴权工厂和插件属性。下面工厂类名及插件参数名是本插件的设计约定，参数值是示例；`access_controller.class` 和 `access_controller.properties.` 是 Doris 现有入口。

```sql
CREATE CATALOG hive_prod PROPERTIES (
    'type' = 'hms',
    'hive.metastore.uris' = 'thrift://hms.example.com:9083',
    'access_controller.class' =
        'org.apache.doris.plugin.hms.HmsAccessControllerFactory',
    'access_controller.properties.hms.uri' = 'thrift://hms.example.com:9083',
    'access_controller.properties.cache.ttl.seconds' = '600',
    'access_controller.properties.cache.maximum.size' = '10000',
    'access_controller.properties.doris.admin.bypass.enabled' = 'false'
);
```

- `hms.uri` 给插件自己的连接池使用；Catalog 读取元数据用的 HMS 参数不会自动传进插件。
- HMS 认证复用当前分支 `HMSBaseProperties`，使用 `hive.metastore.authentication.type`、
  `hive.metastore.client.principal`、`hive.metastore.client.keytab` 和 `hive.metastore.service.principal`。
  这些插件属性均带 `access_controller.properties.` 前缀。默认 HMS simple；不借用 HDFS principal/keytab。
  底层登录、doAs 和凭据刷新仍复用 Doris HadoopAuthenticator。
- `cache.ttl.seconds`：各类权限缓存共用的有效期，单位秒。示例是 600 秒，未定为默认值。设置为 0 时关闭跨查询缓存，允许单次鉴权内合并重复读取，不创建定时刷新任务。配置是字符串，不计算 `60*10` 这样的表达式。
- `cache.maximum.size`：各类缓存共用一个配置值，分别应用为每个缓存实例的 `maximumSize`。建议默认 10000，要求为正整数；它不是所有 FE、所有 Catalog 加起来的总额，也不是字节数上限。TTL 为 0 时不保留跨查询缓存。
- `doris.admin.bypass.enabled`：按 Catalog 控制 Doris 本地管理员是否跳过插件的 HMS 检查，建议默认 false。false 时管理员也检查 HMS；true 时仅本地管理员可以跳过。管理员身份按 Doris 已认证身份及管理员权限判断，不按用户名硬编码，不把 HMS 的 admin 角色当成本地管理员。此配置不改变 Doris 登录认证和其他平台权限检查。
- 不提供显式刷新命令或接口。每个 FE 按各自缓存的 TTL 更新角色、库和表权限，不要求所有 FE 在同一时刻更新。

实现时需要处理 `branch-4.1` 带 `hasGlobal` 的鉴权方法，不能让默认的全局放行逻辑覆盖上述配置。普通用户即使有本地全局 SELECT，也必须通过 HMS 检查。

## 测试与 case 构造
- USER大小写，HIve user严格区分大小写
- 库表大小写：先通过 Doris Catalog 取得实际远端库表名，再按 Hive 规则统一小写，构造 HMS 请求和缓存键。不能跳过 Catalog 对象解析、直接把 SQL 原始名字转小写后查询权限。
- 缓存更新：覆盖首次加载、有效期内命中、持续访问不续期、到期重读、重读失败，以及 TTL 为 0 时每次鉴权不复用跨查询缓存；验证授权和撤权在各 FE 上的生效时间。无需测试显式刷新命令。
- 缓存容量：覆盖超过 `cache.maximum.size` 后的淘汰和重新加载，确认淘汰只影响命中率，不改变权限判断；用实际授权条数验证内存占用。
- 写操作：覆盖有权限和无权限的 INSERT、CREATE、ALTER；INSERT SELECT、CTAS 同时检查源和目标；撤销 INSERT、角色移除、owner 变化后按缓存更新规则生效。检查新建表在 HMS 的实际 owner；覆盖 Connector 已支持的追加/覆盖、ALTER 子类型，避免只测一个 LOAD/ALTER 谓词。
- 管理员配置：分别测试 true/false、本地管理员和普通用户；普通用户持有本地全局 SELECT 时仍须检查 HMS，HMS admin 角色不能被误当作本地管理员。
- HMS 兼容性：主要测试客户 HMS 1.1.0，记录发行版、补丁版本、认证方式和权限来源。2/3/4 暂保留接口核对结果，需要支持其他客户版本时再补联调。



## Hive/HMS 版本兼容

插件直接连接 HMS，不经过 HiveServer2、不执行 Hive SQL。兼容对象是“插件中的 HMS 客户端与 HMS 服务端”，不能只按 HiveServer2 的版本判断。

已经查过下面四个版本的权限 Thrift 定义。表中表示接口存在，不表示已经完成互通测试：

| 接口或字段 | Hive 1.1.0 | Hive 2.3.9 | Hive 3.1.3 | Hive 4.0.1 |
|---|---|---|---|---|
| `get_role_grants_for_principal` | 存在 | 存在 | 存在 | 存在 |
| `list_privileges` | 存在 | 存在 | 存在 | 存在 |
| `get_privilege_set` | 存在 | 存在 | 存在 | 存在 |
| USER/ROLE、权限名称等核心字段 | 存在 | 存在 | 存在 | 存在 |
| `HiveObjectRef.catName` | 无 | 无 | 有，可选 | 有，可选 |

先选择一套兼容 Doris `branch-4.1` 的客户端依赖，重点联调客户 HMS 1.1.0，包括权限 API、库表所有者读取和 Kerberos。1.1.0 服务端已有 `list_privileges(null, null, tableRef)` 按表读取全部授权的处理，不需要改成按角色全量读取。只在发现实际差异时于插件内部适配，不预先引入多套客户端，也不修改 Doris 内核。

兼容验证包含：

- 客户端连接及权限 RPC 的协议互通，新增字段/缺失字段的处理。
- USER/ROLE 授权、继承、public、空授权和撤权行为。
- HMS 1.1.0 不设置 `catName`，按库表名定位。以后联调 Hive 3/4 时再验证默认 Catalog 定位和非默认命名空间，不因字段存在就承诺支持。
- 普通认证、Kerberos/SASL、凭据更新后新建连接及断线重连。
- Hive/Hadoop/Thrift 依赖与目标 Doris 插件类加载环境的兼容。
测试完记录具体的服务端版本、客户端版本、认证方式和结果。其他发行版或厂商定制版，需要测过再说支持。

HMS 权限读取

从 HMS 读取角色关系、表授权，以及 CREATE/ALTER 所需的库表所有者。需要判断库授权的操作，再按 DATABASE 对象读取对应授权。

需要获得的信息：

数据
示例
用途
USER 表授权
alice 对 sales.customers 有 SELECT
直接授权
ROLE 表授权
analyst 对 sales.orders 有 SELECT
角色授权
USER 的角色关系
alice 被授予 analyst
角色解析
ROLE 的角色关系
analyst 被授予 reader
角色继承
必要的对象属性
实际远端库表名、库 ownerName/ownerType、表 owner 等
正确定位对象，以及 CREATE/ALTER 所有权判断

HMS 接口候选：

- get_role_grants_for_principal：查询授予 USER 或 ROLE 的角色。
- list_privileges：查询目标表的授权，可按 principal 过滤；Trino 也有不指定 principal、按表读取授权的路径。
- get_privilege_set：按目标表、用户、组返回分类权限；已核对的 Hive 1.1.0 和 3.1.3 表权限实现会展开关联角色层级。
- get_database / get_table：读取库表对象和所有者，为 CREATE/ALTER 判断提供依据。CREATE TABLE 只读取目标库，不查尚不存在的表。
首期建议采用“角色关系 + 按表授权”的读取方式，便于复用缓存及控制启用角色。是否使用 get_privilege_set 是技术方案选择，不新增为独立用户功能；使用它也必须符合 F04 的角色规则。

授权信息成功读取但没有当前操作所需权限，与 RPC 失败必须区分。目标对象使用 HMS 远端库表名；Doris Catalog 名不能直接当作 HMS 的 Catalog 命名空间。

验收：用真实 HMS 验证直接授权、角色授权、继承关系、空授权，以及目标版本的权限字符串表示。

HiveObjectRef.catName 的含义

catName 是 HMS 内部 Catalog 命名空间的名称，用于定位授权所对应的元数据对象；它不是 Doris 外部 Catalog 的名称，也不是新增的 Catalog 级权限类型。

Hive 3/4 的元数据对象可以表示为：

HMS Catalog → Database → Table
例如：hive → sales → orders

Hive 3.1.3 的 metastore.catalog.default 默认值为 hive。实际使用哪个默认命名空间，应核对客户端配置和部署环境，不能把“字段没传”直接等同于所有环境都选择同一个值。

例如 Doris 用户执行：

SELECT * FROM prod_hive.sales.orders;

如果 Doris 的 prod_hive 对应 HMS 的默认 hive 命名空间，则目标对象在支持 catName 的客户端中可表示为：

HiveObjectRef object = new HiveObjectRef();
object.setObjectType(HiveObjectType.TABLE);
object.setCatName("hive");
object.setDbName("sales");
object.setObjectName("orders");

其中 prod_hive 用来选择 Doris Catalog 和所绑定的鉴权插件，hive 用来定位远端 HMS 命名空间，两个名字不要求相同。

上面的 `catName` 示例用于 Hive 3/4。主要测试的 HMS 1.1.0 不设置这个字段，直接使用 `sales.orders` 定位。

配置及缓存设计要求：插件的远端命名空间与 Catalog 的元数据来源保持一致；表授权缓存的对象身份须包含或由实例配置固定该命名空间，配置变化后旧缓存失效。对 Hive 1.1.0/2 不使用不存在的命名空间能力；对 Hive 3/4 非默认命名空间必须验证服务端权限 API 的实际定位结果。

库表名大小写与对象解析

标准 Hive/HMS 的库表名不区分大小写，按其规则规范化后用于权限查询和缓存。 鉴权对象必须与查询实际访问的 HMS 对象相同：先通过 Doris Catalog 取得实际远端名称，再规范化实际 HMS 库表名，不能直接转换 SQL 原始拼写。

处理要求：

1. 核对 branch-4.1 鉴权接口传入库表名的来源，并与该分支现有 Catalog 名称解析、大小写配置保持一致。
2. 使用现有 Catalog 接口获得实际 HMS 库表名，再构造权限请求；不新增内核接口，不反射读取内部状态。
3. 对实际 HMS 库表名按大小写不敏感规则统一规范化为小写，请求和缓存键使用相同规则；转换采用不受系统默认 locale 影响的方式。
4. 表授权缓存以解析后的 HMS 对象身份组织：Catalog/插件实例、实际 HMS 命名空间、库名和表名。等价拼写指向同一对象时不应产生互相矛盾的授权结果；不同远端对象不能因错误折叠大小写而共享授权。
5. USER 严格区分大小写，原样保留；ROLE 按 Hive SQL Standard 的大小写不敏感规则规范化。身份缓存键必须包含 principal 类型，不能将 USER 和 ROLE 混用。
6. SQL 查询中的表别名不作为授权对象。
7. 不采用“原名查不到就尝试其他大小写并合并权限”的猜测方式；无法可靠定位时应报错，避免检查其他表的授权。
验收覆盖：同一可解析表的大小写拼写、带引号标识符、表别名、大小写相关 Catalog 配置，以及缓存命中与未命中时结果一致。测试根据目标 HMS/branch-4.1 实际支持的命名语义构造，不假设所有环境都允许仅大小写不同的两张表共存。

F03.3：HMS 调用与缓存流程

以 USER alice 查询实际 HMS 表 sales.orders 为例；HMS 1.1.0 没有 catName，Hive 3/4 再结合配置的远端命名空间定位：

1. 从角色关系缓存读取 USER:alice；缺失或过期时，调用 get_role_grants_for_principal，参数为 principal_name="alice"、principal_type=USER。
2. 对返回的角色继续以 principal_type=ROLE 读取授予关系，复用角色关系缓存，展开继承并去重；按 F04 规则加入 public、排除未启用的 admin。
3. 从表授权缓存读取目标对象；缺失或过期时，构造 HiveObjectRef(TABLE, dbName, objectName, ...)，在支持的客户端及命名空间配置下处理 catName。
4. 首选调用 list_privileges(null, null, tableRef) 按表获取授权列表，多个用户复用。目标 HMS 的接口互通测试需覆盖这种不指定 principal 的调用；按 principal 查询的变体不改变权限合并语义。
5. 将 HiveObjectPrivilege 的 principal 类型、名称及 grantInfo 转换为插件权限数据，按 F04 与当前有效身份匹配 SELECT。

以上是 SELECT 路径。INSERT 复用角色关系和表授权缓存，按具体写入操作匹配 INSERT 等所需权限；CREATE 读取目标库权限信息；ALTER 读取表权限信息，并按 SQL 子类型检查所有权或所需授权。源表与目标表分别检查，所有检查都通过才允许执行。
所有缓存未命中后的 RPC 经过统一调用封装：借用插件连接池客户端，在 authenticator.doAs() 中执行 RPC，成功后归还客户端；发生连接故障时按连接池策略销毁坏连接。新建或重建客户端也必须在对应认证上下文中执行。普通的无授权结果不作为连接故障。

成功返回空授权可以缓存；RPC 异常不能转换成空授权或允许结果。角色和表授权缓存都有效时不请求 HMS，直接在内存中判断。冷缓存的调用次数取决于需展开的角色关系及目标表数，不是每次 SELECT 都重新建连接。

上述为调用设计，不是已完成实现。具体 Java 类型、方法签名和认证封装以 branch-4.1 及选定 HMS 客户端依赖为准，不新增内核接口。


示例：

alice 的直接授权
alice 的有效角色授权
查询结果
customers：SELECT
orders：SELECT
两张表均可查询
无
orders：SELECT
可以查询 orders
orders：SELECT
无
可以查询 orders
无
无
拒绝查询 orders

验收：直接授权、角色授权、多级继承、public、重叠授权和撤销其中一路授权符合规则；alice 与 Alice 不共享用户授权或缓存身份，ROLE 大小写变体的处理保持一致。



权限缓存与更新

权限不用每次都去 HMS 读，先放进缓存。但缓存要到期，Hive 那边撤销后，Doris 不能一直用旧权限。

F07：错误处理、日志和指标

这里有问题就是内核框架的问题，比如插件信息被再包裹了一层导致真实信息被吞了，插件保证报错信息可读就好



## 开发时先验证的实现细节

现在可以开始开发。以下作为开发任务先验证，不再作为要求客户反复确认的功能范围问题；如果发现 4.1 现有接口确实无法满足某项要求，再记录具体限制：

- 验证固定 TTL 下的撤权生效时间、加载失败和在途加载处理；派生角色集合不能重新计时延长底层权限的有效期。
- 按已定的管理员配置规则处理本地全局权限和 Catalog 入口，核对不同写操作子类型在 `branch-4.1` 中可取得的上下文。
- 显式刷新已移出本期，不再作为接口验证或开工前置事项。

Catalog 删除、重建时的连接池和后台任务主动释放暂不纳入本期。这个延期不改变正常借还连接、坏连接重建和旧权限不能跨 Catalog 实例复用的要求。

## 参考文档

- [Hive 1.1.0 权限接口](https://github.com/apache/hive/blob/e60744d017ef79f1b17f474c0b969d4ca5592462/metastore/if/hive_metastore.thrift)：主要测试版本的权限对象及读取 API。
- [Hive 1.1.0 SQL Standard 操作权限映射](https://github.com/apache/hive/blob/e60744d017ef79f1b17f474c0b969d4ca5592462/ql/src/java/org/apache/hadoop/hive/ql/security/authorization/plugin/sqlstd/Operation2Privilege.java)：CREATE、ALTER、INSERT 及子类型的权限要求。表中设计针对库表鉴权，文件系统和 URI 的访问仍由对应存储认证授权负责。

- Trino Hive SQL Standard 授权文档：默认角色启用规则及该模式的范围。
- Hive SQL Standard 用户与角色命名规则：USER 区分大小写、ROLE 不区分大小写。
- Hive DDL 文档及 HMS 表权限实现：表名大小写语义及库表权限对象规范化。
- Trino SqlStandardAccessControl：基于目标表授权和有效身份的检查。
- Trino 角色解析：USER、有效 ROLE、public 和 admin 的处理。
- Hive 3.1.3 Thrift 定义：角色授予关系、表授权和分类权限的数据结构与 API。
- Hive 2.3.9 Thrift 定义和 Hive 4.0.1 Thrift 定义：跨版本权限接口及字段核对。
- Hive 3.1.3 MetastoreConf：默认 HMS Catalog 配置。
- Hive 3.1.3 ObjectStore：按用户读取表权限及展开角色层级的实现。


## 2026-09-11 实现核对

实现和运行参数见 [README.md](README.md)。TTL 要求显式配置；容量默认每缓存 10000 条，
池大小默认 8，socket 超时默认 30 秒，管理员绕过默认 false。
Hive 1.1 SQL Standard 的 default 库所有用户建表规则已实现，并纳入单测。

已确认 4.1 可从 StmtExecutor.getParsedStmt() 的 LogicalPlanAdapter 取得 INSERT/OVERWRITE/ALTER 命令；
StatementContext.getParsedStatement() 作为现有入口的补充，不解析 SQL 文本。
CTAS 内部追加检查使用实际 HMS 表 owner 和目标库资格，源表 SELECT 仍由内核逐表调用插件检查。

Hive 3 的 getTable 包装接口发送 get_table_req，不直接兼容 HMS 1.1。
插件复用当前分支 HiveMetaStoreClient 的连接/认证，通过其公开 getTTransport() 调用旧 get_table、
get_database 和授权接口；不反射、不替换依赖、不修改内核。

此基线 HiveMetadataOps 尚未实现远端 ALTER 列、表名、表属性，权限通过后仍可能得到 unsupported；
ADD/DROP PARTITION 在外部表 SQL 校验阶段即受限。插件实现权限映射，不扩展 Connector 执行能力。
自建 Docker HMS 1.1.0 Simple 集成测试已通过：真实授权/角色/owner、TTL 撤权及停机重连。
当前 branch-4.1 FE + 4.1.3 BE 已执行真实 SQL 回归，含 HDFS 数据读写、CTAS、权限拒绝、
撤权和 HMS 停机重连。Kerberos 和多 FE 进程联调未完成，两个 Catalog 实例不等于多 FE 集群回归。
Doris 内置 HiveMetaStoreClient 自身已有按 hive.version 选择旧 RPC 的逻辑；
连接 HMS 1.1.0 的元数据 Catalog 必须配置 hive.version=1.1.0。

## 2026-09-11 大小写实测修正

已按真实能力验证：USER 大小写严格分离、同名 USER/ROLE 分离、HiveServer2 1.1 SQL Standard
对混合大小写角色的创建/授权/撤销、表/库 owner 用户名大小写、Doris 库表模式 0/1/2、
带引号的标识符和查询别名、大小写用户授权反转后的 TTL。插件 Locale.ROOT 另有土耳其语
默认 Locale 单测。用例及边界结论见 CASE_VALIDATION.md。

## Doris 内置系统表兼容

HMS Catalog 中的 information_schema / mysql 是 Doris 本地系统对象。插件仅在实际数据库和表
分别属于 ExternalInfoSchemaDatabase/Table 或 ExternalMysqlDatabase/Table 时，将 SELECT
交给 Doris 本地控制器，保留目标 Catalog、表和列权限参数；库名本身不构成放行条件。
本地系统表不查询 HMS 角色/表授权，也不进入 Hive 表授权缓存。普通 Hive 表权限不受影响。
系统表的内核特殊权限检查（例如 cluster_snapshots 的管理员限制）继续生效。

客户已明确平台密钥认证与本插件无关，不纳入实现、验收或完成度判断。
