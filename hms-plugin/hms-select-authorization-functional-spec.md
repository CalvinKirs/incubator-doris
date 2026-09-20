# Doris HMS 表级 SELECT 鉴权功能文档

> 2026-09-11：本文保留早期 SELECT 方案供参考，当前开发范围以 [HMS 插件设计](hms-plugin-design.md)为准。第一版已加入 INSERT、CREATE、ALTER，主要测试 HMS 1.1.0，基于 Doris `branch-4.1` 开发；以下只读范围、版本测试范围和刷新建议不再作为当前要求。

状态：功能方案，少数细节还要定，见第 8 节
日期：2026-09-10

接口就按 Doris `branch-4.1` 来。插件加载用已有能力，打包、安装这次不用管，主要做鉴权、配置和测试。

配套说明：[与 Trino SQL Standard 的功能、实现和缓存对比](trino-comparison.md)。

## 1. 这次要做什么

用户从 Doris 查询 Hive 表时，直接沿用 Hive Metastore（HMS）里已有的权限，由 Doris 来判断能不能查。这样不用接 Ranger，也不用在 Doris 里再配一遍表权限。

Doris 登录用户名与 HMS 授权记录中的 USER 名称一致。HMS 作为权限来源，Doris 负责登录认证、读取权限及查询时的权限判断。

判断规则很简单：用户自己有这张表的 SELECT，或者他的有效角色里有 SELECT，就可以查；两边都没有，就拒绝。

做成独立的 Doris FE Catalog 鉴权插件，**前提是不改内核**。代码写在插件里，通过已有接口接入，自己管理 HMS 连接池和权限缓存。FE/BE 核心代码、内置 Connector、SPI 和核心 JAR 都不改，具体见第 10 节。

这里说的“同步”，就是查询时按需读取 HMS 权限，在插件里缓存，到期再读。不会把这些授权写进 Doris 本地权限表。

## 2. 哪些已经定了，哪些还要定

### 2.1 已经定下来的

- 不依赖 Ranger，鉴权在 Doris 内完成。
- 仅支持表级 SELECT，合并 USER 与 ROLE 授权。
- Doris 用户名与 HMS USER 名称直接对应。
- 不修改 Doris 内核，通过现有 Catalog 鉴权插件扩展点接入。
- Doris 接口固定以 `branch-4.1` 为准，不将其他开发分支的新接口作为实现依赖。
- 插件显式配置 HMS 连接参数，独立建立并管理连接池，不依赖 Catalog/Connector 的内部客户端。
- 支持联邦查询中每张 Hive 基表的权限检查。
- 包含权限缓存、过期更新和撤权生效行为。
- 缓存采用 Doris 现有的 Caffeine 技术体系，复用兼容的现有封装，不自建通用缓存框架；权限实例及有效期由插件独立管理。
- SHOW/DESC 不新增 HMS 鉴权或元数据隐藏功能，只保留现有接口所需的兼容适配，不作为独立功能项或待确认项。

### 2.2 第一版建议这样做

- 按 Catalog 显式启用，默认关闭。
- 启用用户直接或间接拥有的普通角色及 `public`，不自动启用 HMS `admin`。
- 采用按需加载、硬过期的权限缓存，建议默认 TTL 为 30 秒，可关闭跨查询缓存。
- 首期不新增根据 HMS 权限隐藏库表名称的功能。
- 首期不提供 HMS 授权写回、角色管理和 `SET ROLE`。
- 首期不提供主动刷新命令，以 TTL 保证约定范围内的更新时效。

上面这些先按建议讨论，还没全部定下来。管理员是否例外、视图怎么查、写操作怎么处理，也放在第 8 节一起说明。

## 3. 开发分成哪些功能

| 编号 | 功能 | 第一版做什么 | 依赖 |
|---|---|---|---|
| F01 | Catalog 鉴权模式管理 | 启用、参数校验、模式切换及生命周期处理 | 无 |
| F02 | 插件 HMS 连接管理 | 插件独立配置连接、认证和连接池，访问权限 API | F01 |
| F03 | HMS 权限读取 | 读取 USER/ROLE 授权、角色关系，转换为内部权限数据 | F02 |
| F04 | 用户与角色权限合并 | 解析有效角色，将直接授权与角色授权合并判断 | F03 |
| F05 | 表级 SELECT 拦截 | 查询涉及的每张 Hive 基表均经过鉴权 | F01、F04 |
| F06 | 权限缓存与更新 | 按需缓存、容量限制、TTL、撤权及多 FE 行为 | F03、F04 |
| F07 | 错误处理、日志和指标 | 区分无权限和读取失败，提供日志与缓存指标 | F02—F06 |
| F08 | 与现有 Doris 行为的配合 | 本地权限关系、对象解析、视图及模式外行为 | F01、F05 |
| F09 | 测试和使用说明 | 单元测试、真实 HMS 回归、配置和运维说明 | F01—F08 |

## 4. 每个功能具体怎么做

### F01：Catalog 鉴权模式管理

管理员可以给指定的 HMS Catalog 开启这套鉴权，其他 Catalog 不受影响。

功能要求：

1. 按 Catalog 配置；不同 Catalog 的模式和权限数据相互隔离。
2. 同一个 Catalog 使用一种明确的鉴权模式，不同时配置 Ranger 与本模式。
3. 未启用的 Catalog 保持原有行为。
4. 支持配置权限缓存有效期、最大缓存条目数；具体配置名在技术设计阶段确定。
5. 校验 Catalog 类型、模式冲突以及参数取值。
6. 通过 `branch-4.1` 现有生命周期能力处理插件资源及缓存失效，不依赖新增内核回调；具体对象生命周期属于实现细节。
7. 同名 Catalog 删除重建不能复用旧实例的权限数据。

验收：能够单独开启一个 Catalog；其他 Catalog 不受影响；重建及模式切换后不使用旧授权。

### F02：插件 HMS 连接管理

插件自己连 HMS，连接参数通过现有的插件配置入口传进来。

插件通过现有鉴权插件属性通道接收 HMS 地址、配置资源、认证身份、Kerberos principal/keytab、连接池容量及超时等参数。连接参数应指向被鉴权 Catalog 对应的 HMS；可以填写与 Catalog 相同的参数，但不自动读取或共享其内部客户端。

连接配置如何与对应 Catalog 保持一致，需要在使用文档中说明；HMS 端点或命名空间变更时，应按部署流程同步更新插件配置，不能继续向旧 HMS 查询授权。

身份区分：

```text
插件服务账号   → 连接 HMS、读取授权
Doris 登录用户 → 本次需要判断权限的 USER
```

例如插件以 `doris_service` 连接 HMS，通过该连接读取 `alice` 的授权；不要求为 `alice` 单独建连接或切换连接身份。

连接池由插件创建、借还和释放。每个 FE 上的 Catalog 鉴权插件实例维护自己的连接资源，不跨用户逐个建池，也不跨 Catalog 隐式共享。使用目标版本现有生命周期回调释放资源；不增加 FE/Connector 桥接接口。

前置条件：插件服务账号能够读取所需授权和角色关系。元数据查询成功不代表权限 API 一定可用，必须通过联调验证。

测试时要在 `branch-4.1` 上跑通连接和权限读取，检查普通认证、Kerberos，以及连接能否正确释放。打包、安装和加载机制本身不用测。

#### F02.1：Kerberos 认证与票据管理

插件连接支持普通模式和 Kerberos 模式。Kerberos 属于插件访问 HMS 的服务认证，不改变 Doris 用户登录方式，也不代替表级 SELECT 授权。

需要配置或部署：插件客户端 principal、keytab 文件路径、HMS 服务 principal、HMS SASL 相关配置，以及 FE 运行环境使用的 krb5.conf/KDC/realm 配置。各个承担鉴权的 FE 均须具备对应文件、读取权限和网络条件。具体参数名由目标版本的插件配置入口确定，不将示例名称当作已有功能。

功能要求：

1. 使用插件服务身份通过 keytab 登录，并在其认证上下文中创建和使用 HMS 客户端。
2. 自动检查票据有效期，按所用认证库的方式重新登录或更新凭据，不依赖人工定时执行 kinit。
3. 处理凭据更新后的新连接认证、失效连接销毁与重建，验证长时间运行及空闲后恢复。
4. 普通模式和 Kerberos 模式不能在失败时自动相互回退。
5. 不要求为每个 Doris 用户提供 keytab；服务身份读取目标 USER/ROLE 授权，不默认启用用户代理。
6. 不把票据有效期与 Caffeine 权限 TTL 混为一谈：前者控制连接认证，后者控制授权信息新鲜度。
7. 认证失败时，所需权限缓存全部有效可以继续按缓存判断；缓存缺失或过期则报错，不续用过期授权。
8. 复用 `branch-4.1` 可用的现有认证能力，不另建票据管理框架。此前在其他本地开发分支发现的 `fe-kerberos` 模块仅为参考，实际包名、接口及凭据更新方式以 `branch-4.1` 为准。
9. 验证类加载、UGI 全局配置及 JVM Kerberos 配置与其他 Catalog 的共存，不以修改内核、替换内置依赖或无条件覆盖全局登录身份解决问题；首期不承诺每个 Catalog 使用独立 krb5.conf。

测试要包括首次登录、票据更新后建连接、断线重连、空闲后再查、KDC 暂时不可用、错误 principal/keytab 和多 FE。还要确认不影响已有 Catalog 的认证。需要至少跑过一次票据更新，光是第一次连上还不够。

**doAs 的具体使用方式**：沿用已核对的现有 HMS 模式，创建/重建客户端以及执行权限 RPC 都进入服务身份的认证上下文。优先使用兼容的 `HadoopAuthenticator.doAs()` 封装；本地实现会先调用 `getUGI()`，Kerberos 实现负责按需登录和更新凭据。单独保存一个 UGI 并反复调用 `ugi.doAs()`，不能被当作已经覆盖票据更新。

```text
创建/重建连接 → authenticator.doAs(() -> 创建 HMS 客户端)
读取权限 RPC → authenticator.doAs(() -> 调用 HMS 权限接口)
```

上面是流程示意，不是最终接口代码。每次操作不需要重新登录或新建连接；按需更新凭据，连接池继续复用。客户端重连路径也须保持同一认证上下文，认证逻辑与插件 Hadoop 类加载上下文一起验证。

#### F02.2：Hive/HMS 版本兼容

插件直接连接 HMS，不经过 HiveServer2、不执行 Hive SQL。兼容对象是“插件中的 HMS 客户端与 HMS 服务端”，不能只按 HiveServer2 的版本判断。

已经查过下面三个版本的 Thrift 定义，所需接口都有。这里只确认了接口，和真实 HMS 的联调还没做：

| 接口或字段 | Hive 2.3.9 | Hive 3.1.3 | Hive 4.0.1 |
|---|---|---|---|
| `get_role_grants_for_principal` | 存在 | 存在 | 存在 |
| `list_privileges` | 存在 | 存在 | 存在 |
| `get_privilege_set` | 存在 | 存在 | 存在 |
| USER/ROLE、权限名称等核心字段 | 存在 | 存在 | 存在 |
| `HiveObjectRef.catName` | 无 | 有，可选 | 有，可选 |

建议先选择一套兼容 Doris `branch-4.1` 的客户端依赖，使用稳定权限接口，分别联调 HMS 2.3、3.1、4.0 的明确小版本。仅在发现实际差异时于插件内部适配，不预先引入三套客户端，也不修改 Doris 内核。

兼容验证包含：

- 客户端连接及权限 RPC 的协议互通，新增字段/缺失字段的处理。
- USER/ROLE 授权、继承、public、空授权和撤权行为。
- Hive 3/4 的默认 HMS Catalog 定位；非默认命名空间需要额外验证，不因字段存在就承诺支持。
- 普通认证、Kerberos/SASL、凭据更新后新建连接及断线重连。
- Hive/Hadoop/Thrift 依赖与目标 Doris 插件类加载环境的兼容。

测试完记录具体的服务端版本、客户端版本、认证方式和结果。其他发行版或厂商定制版，需要测过再说支持。

### F03：HMS 权限读取

从 HMS 读两类数据：角色关系，以及表上授给用户和角色的权限。

需要获得的信息：

| 数据 | 示例 | 用途 |
|---|---|---|
| USER 表授权 | `alice` 对 `sales.customers` 有 SELECT | 直接授权 |
| ROLE 表授权 | `analyst` 对 `sales.orders` 有 SELECT | 角色授权 |
| USER 的角色关系 | `alice` 被授予 `analyst` | 角色解析 |
| ROLE 的角色关系 | `analyst` 被授予 `reader` | 角色继承 |
| 必要的对象属性 | 表的远端名称、授权解析所需属性 | 正确定位及解释授权 |

HMS 接口候选：

- `get_role_grants_for_principal`：查询授予 USER 或 ROLE 的角色。
- `list_privileges`：查询目标表的授权，可按 principal 过滤；Trino 也有不指定 principal、按表读取授权的路径。
- `get_privilege_set`：按目标表、用户、组返回分类权限；Hive 3.1.3 的表权限实现会展开关联角色层级。

首期建议采用“角色关系 + 按表授权”的读取方式，便于复用缓存及控制启用角色。是否使用 `get_privilege_set` 是技术方案选择，不新增为独立用户功能；使用它也必须符合 F04 的角色规则。

授权信息成功读取但没有 SELECT，与 RPC 失败必须区分。目标对象使用 HMS 远端库表名；Doris Catalog 名不能直接当作 HMS 的 Catalog 命名空间。

验收：用真实 HMS 验证直接授权、角色授权、继承关系、空授权，以及目标版本的权限字符串表示。

#### F03.1：HiveObjectRef.catName 的含义

`catName` 是 HMS 内部 Catalog 命名空间的名称，用于定位授权所对应的元数据对象；它不是 Doris 外部 Catalog 的名称，也不是新增的 Catalog 级权限类型。

Hive 3/4 的元数据对象可以表示为：

```text
HMS Catalog → Database → Table
例如：hive → sales → orders
```

Hive 3.1.3 的 `metastore.catalog.default` 默认值为 `hive`。实际使用哪个默认命名空间，应核对客户端配置和部署环境，不能把“字段没传”直接等同于所有环境都选择同一个值。

例如 Doris 用户执行：

```sql
SELECT * FROM prod_hive.sales.orders;
```

如果 Doris 的 `prod_hive` 对应 HMS 的默认 `hive` 命名空间，则目标对象在支持 `catName` 的客户端中可表示为：

```java
HiveObjectRef object = new HiveObjectRef();
object.setObjectType(HiveObjectType.TABLE);
object.setCatName("hive");
object.setDbName("sales");
object.setObjectName("orders");
```

其中 `prod_hive` 用来选择 Doris Catalog 和所绑定的鉴权插件，`hive` 用来定位远端 HMS 命名空间，两个名字不要求相同。

配置及缓存设计要求：插件的远端命名空间与 Catalog 的元数据来源保持一致；表授权缓存的对象身份须包含或由实例配置固定该命名空间，配置变化后旧缓存失效。对 Hive 2 不使用不存在的命名空间能力；对 Hive 3/4 非默认命名空间必须验证服务端权限 API 的实际定位结果。

#### F03.2：库表名大小写与对象解析

**标准 Hive/HMS 的库表名不区分大小写，按其规则规范化后用于权限查询和缓存。** 鉴权对象必须与查询实际访问的 HMS 对象相同：先通过 Doris Catalog 取得实际远端名称，再规范化实际 HMS 库表名，不能直接转换 SQL 原始拼写。

处理要求：

1. 核对 `branch-4.1` 鉴权接口传入库表名的来源，并与该分支现有 Catalog 名称解析、大小写配置保持一致。
2. 使用现有 Catalog 接口获得实际 HMS 库表名，再构造权限请求；不新增内核接口，不反射读取内部状态。
3. 对实际 HMS 库表名按大小写不敏感规则统一规范化为小写，请求和缓存键使用相同规则；转换采用不受系统默认 locale 影响的方式。
4. 表授权缓存以解析后的 HMS 对象身份组织：Catalog/插件实例、实际 HMS 命名空间、库名和表名。等价拼写指向同一对象时不应产生互相矛盾的授权结果；不同远端对象不能因错误折叠大小写而共享授权。
5. USER 严格区分大小写，原样保留；ROLE 按 Hive SQL Standard 的大小写不敏感规则规范化。身份缓存键必须包含 principal 类型，不能将 USER 和 ROLE 混用。
6. SQL 查询中的表别名不作为授权对象。
7. 不采用“原名查不到就尝试其他大小写并合并权限”的猜测方式；无法可靠定位时应报错，避免检查其他表的授权。

验收覆盖：同一可解析表的大小写拼写、带引号标识符、表别名、大小写相关 Catalog 配置，以及缓存命中与未命中时结果一致。测试根据目标 HMS/`branch-4.1` 实际支持的命名语义构造，不假设所有环境都允许仅大小写不同的两张表共存。

#### F03.3：HMS 调用与缓存流程

以 USER `alice` 查询实际 HMS 对象 `hive.sales.orders` 为例：

1. 从角色关系缓存读取 `USER:alice`；缺失或过期时，调用 `get_role_grants_for_principal`，参数为 `principal_name="alice"`、`principal_type=USER`。
2. 对返回的角色继续以 `principal_type=ROLE` 读取授予关系，复用角色关系缓存，展开继承并去重；按 F04 规则加入 public、排除未启用的 admin。
3. 从表授权缓存读取目标对象；缺失或过期时，构造 `HiveObjectRef(TABLE, dbName, objectName, ...)`，在支持的客户端及命名空间配置下处理 `catName`。
4. 首选调用 `list_privileges(null, null, tableRef)` 按表获取授权列表，多个用户复用。目标 HMS 的接口互通测试需覆盖这种不指定 principal 的调用；按 principal 查询的变体不改变权限合并语义。
5. 将 `HiveObjectPrivilege` 的 principal 类型、名称及 `grantInfo` 转换为插件权限数据，按 F04 与当前有效身份匹配 SELECT。

所有缓存未命中后的 RPC 经过统一调用封装：借用插件连接池客户端，在 `authenticator.doAs()` 中执行 RPC，成功后归还客户端；发生连接故障时按连接池策略销毁坏连接。新建或重建客户端也必须在对应认证上下文中执行。普通的无授权结果不作为连接故障。

成功返回空授权可以缓存；RPC 异常不能转换成空授权或允许结果。角色和表授权缓存都有效时不请求 HMS，直接在内存中判断。冷缓存的调用次数取决于需展开的角色关系及目标表数，不是每次 SELECT 都重新建连接。

上述为调用设计，不是已完成实现。具体 Java 类型、方法签名和认证封装以 `branch-4.1` 及选定 HMS 客户端依赖为准，不新增内核接口。

### F04：用户与角色权限合并

用户自己拿到的权限和角色带来的权限合在一起判断，查询时不用再指定角色名。

普通用户的建议判定规则：

```text
有效身份 = USER(当前登录用户名)
         + ROLE(public)
         + USER 直接或间接拥有的普通 ROLE

允许 SELECT = 目标表存在授给任一有效身份的 SELECT
```

功能要求：

1. 使用已认证的 Doris 用户名，不包含 host 部分。USER 严格区分大小写：`alice` 与 `Alice` 是不同身份。请求、匹配和缓存键都原样保留，不转小写、不使用忽略大小写比较；Doris 与 HMS 用户名一致包含大小写完全一致。
2. ROLE 不区分大小写：`analyst` 与 `Analyst` 按同一角色处理。角色名按统一规则规范化后参与请求、匹配、缓存及继承去重；循环关系不能导致无限遍历。`USER:alice` 与 `ROLE:alice` 始终是不同身份。
3. USER 直接授权与 ROLE 授权取并集，任一路径满足即可允许。
4. 普通 SELECT 不要求 `WITH GRANT OPTION`。
5. 撤销一条授权后，其他有效授权仍然生效；REVOKE 不等于增加 DENY。
6. 按目标 HMS 的 SQL Standard 语义解析 SELECT 及可能覆盖它的授权表示。
7. 不因数据库所有权自动推导出对库内所有表的 SELECT。
8. 建议不自动启用 HMS `admin`，也不将同名 HMS 角色映射成 Doris 管理员。
9. 本期不新增 GROUP 授权语义。

示例：

| alice 的直接授权 | alice 的有效角色授权 | 查询结果 |
|---|---|---|
| customers：SELECT | orders：SELECT | 两张表均可查询 |
| 无 | orders：SELECT | 可以查询 orders |
| orders：SELECT | 无 | 可以查询 orders |
| 无 | 无 | 拒绝查询 orders |

验收：直接授权、角色授权、多级继承、`public`、重叠授权和撤销其中一路授权符合规则；`alice` 与 `Alice` 不共享用户授权或缓存身份，ROLE 大小写变体的处理保持一致。

### F05：表级 SELECT 拦截

查询用到哪几张 Hive 表，就逐张检查 SELECT。有一张没权限，整条查询就失败。

需要覆盖：

- 普通单表 SELECT。
- JOIN、UNION、子查询、CTE。
- 跨库、跨 Catalog 查询。
- `COUNT(*)`、`SELECT 1 FROM t` 等没有显式引用业务列的查询。
- 视图展开后的基表访问，最终语义见第 8 节。
- 计划复用、预编译语句和结果缓存等已启用的查询路径。

查询涉及多个 Catalog 时，各对象使用所属 Catalog 的鉴权规则。任意源表不满足 SELECT，整条查询失败，不返回部分数据。

命中计划或结果缓存不能绕过当前所要求的权限检查。检查应在读取或返回受限表数据之前完成。

验收：使用无权限表参与上述查询，均按规则拒绝；只包含有权限表的正常查询不受影响。

### F06：权限缓存与更新

权限不用每次都去 HMS 读，先放进缓存。但缓存要到期，Hive 那边撤权后，Doris 不能一直用旧权限。

首期建议设计：

| 项目 | 行为 |
|---|---|
| 缓存实现 | Caffeine，优先复用目标版本中满足权限过期语义的现有封装 |
| 加载方式 | 按需加载，不全量扫描用户和表 |
| 缓存内容 | 角色关系、表授权及权限解析必需的信息 |
| 隔离范围 | Catalog 实例；各 FE 独立持有 |
| 默认 TTL | 建议 30 秒，待确认 |
| TTL 为 0 | 关闭跨查询权限缓存；允许单次鉴权中的重复读取合并 |
| 过期策略 | 硬过期，命中不续期，过期后重新加载 |
| 容量 | 可配置最大条目数；结合实际授权规模评估内存 |
| 并发请求 | 合并同一缓存项的并发加载，减少重复 RPC |
| 最终允许结果 | 首期不额外长期缓存，使用有效原始信息现场判断 |
| 主动刷新 | 首期暂不提供；作为后续增强 |

权限缓存的有效期独立于表结构、分区和统计信息缓存。

**Caffeine 这里要注意过期方式。** 查过的本地 `master-memory-cache` 分支中，`org.apache.doris.common.CacheFactory` 和 Connector 缓存的相关底层路径用了 `expireAfterAccess`。每访问一次就续期，热门表的旧权限可能一直不过期，所以权限缓存不能直接照搬这个配置。

权限缓存需要写入/成功加载后固定有效期的语义，采用 `expireAfterWrite`，首期不启用 `refreshAfterWrite`。如果 `branch-4.1` 已有封装支持该策略，就直接复用；如果封装只支持访问续期，则在插件内使用现有 Caffeine 的 builder/API 完成配置，不修改内核封装，也不另写通用缓存框架。此前其他分支的代码核对仅作参考，实际库版本和可复用接口以 `branch-4.1` 为准。

复用的是缓存库及适用封装，不是 Catalog 的缓存实例或全局注册中心。插件维护自己的角色关系、表授权缓存，并利用现有缓存能力实现容量限制、加载和统计。Caffeine 过期不自动等于所有 FE 的权限广播，也不替代在途加载与端到端时效验证。

建议这样约定生效时间：从 HMS 已经能读到新权限开始算，最多经过一个 TTL，之后开始鉴权的新查询就要用新权限。测试也按这两个时间点来算。

实现需要覆盖角色移除、角色继承变化、USER 撤权和 ROLE 撤权。派生结果不能重新计时延长原始权限寿命；并发加载不能在失效后将旧结果重新发布。目标 HMS 读取一致性及在途 RPC 对时效的影响，需要在技术设计和测试中明确。

多 FE 在 TTL 窗口内可能暂时看到不同权限，本期不承诺原子一致。所有 FE 均须遵守时效约定。已经通过鉴权并执行中的查询不因缓存过期自动终止。

验收：缓存命中减少请求；硬过期后更新；多 FE 撤权时效、冷缓存并发和 Catalog 重建符合约定。

### F07：错误处理、日志和指标

要让用户和排查问题的人分得清：到底是没有权限，还是 HMS 没连上、权限没读成功。

| 场景 | 预期行为 |
|---|---|
| 权限读取成功，无有效 SELECT | 返回权限不足 |
| HMS 不可用，所需缓存全部有效 | 可按有效缓存判断 |
| HMS 不可用，任一必需信息缺失或过期 | 返回权限读取失败，不放行 |
| 服务账号不能读取权限 | 返回明确的读取失败原因 |
| 角色读取不完整或权限无法正确解释 | 报错，不将不完整结果视为完整权限 |
| Catalog 配置不支持当前模式 | 配置校验或初始化报错 |

不得因权限读取异常自动回退到本地授权并放行。

日志至少包含用户、Catalog、库表、请求权限和失败类别；详细角色信息放在调试日志。新增观测项包括缓存命中/未命中、缓存条目数、HMS 加载次数、耗时和失败次数。

验收：能区分真正无权限、HMS 超时和服务账号读取权限不足；过期授权不会在故障时无限延用。

### F08：与现有 Doris 行为的配合

这套鉴权只在启用它的 Catalog 上生效。还要处理好它和 Doris 本地权限、对象解析、元数据操作的关系。

1. Doris 继续负责认证、Catalog 入口及其他平台权限。
2. HMS SQL Standard 没有对应 Doris 外部 Catalog 的一层授权；本功能不从 HMS 同步 Doris Catalog 权限。
3. 建议普通用户的 Doris 本地 SELECT 不能替代启用模式下的 HMS 表检查；现有全局放行路径需要显式适配。
4. 不将数据库所有权或 Doris Catalog SELECT 当作 Hive 全表放行依据。
5. 对象解析、大小写处理后，授权对象和缓存键仍须对应实际 HMS 表；详细规则及验收见 F03.2。
6. SHOW/DESC 不新增 HMS 权限规则：不根据本插件的 SELECT 授权隐藏库表或字段，也不把 DESC 转换成新的数据读取鉴权需求。现有 Doris 元数据访问规则继续适用。
7. 不修改 HMS 授权，不创建 Doris 用户/角色，不同步密码。
8. 普通 SELECT、写入/DDL和视图的边界仍按本期范围处理；SHOW/DESC 仅做必要接口兼容。若 `branch-4.1` 将元数据操作路由到同一鉴权插件，应避免因仅实现 SELECT 就一律拒绝其他谓词，但不因此扩大为元数据权限功能。

验收：其他模式无回归；普通用户无法依靠本地宽泛授权跳过新模式；第 8 节决策落实后有对应测试。

### F09：测试和使用说明

需要补齐这些测试和说明：

- FE 单元测试：权限合并、继承、缓存及异常处理。
- 真实 HMS 回归：用户/角色授权、撤权、跨表查询和多 FE。
- 性能验证：冷/热缓存 HMS 请求量、鉴权延迟，以及代表性权限规模下的缓存内存。
- 使用文档：功能开启方式、独立 HMS 配置、角色规则、缓存配置、撤权时效和故障排查，不包含打包及安装说明。
- 功能验证：基于 `branch-4.1` 的现有接口验证鉴权功能，不修改内核。已有插件加载机制不作为本次待验证功能。

测试使用仓库规定脚本。具体性能门槛在了解目标表数、用户数、角色数、授权条数和查询并发后确定，不预设未经验证的数字。

## 5. 用一条查询串起来看

已有授权：

```text
USER alice       → sales.customers：SELECT
USER alice       → ROLE analyst
ROLE analyst     → sales.orders：SELECT
```

alice 执行：

```sql
SELECT o.order_id, c.customer_name
FROM hive_catalog.sales.orders o
JOIN hive_catalog.sales.customers c
  ON o.customer_id = c.customer_id;
```

处理流程：

1. Doris 完成 alice 的登录认证，解析查询涉及的实际表。
2. 选择 hive_catalog 绑定的鉴权插件，使用该插件独立的 HMS 连接池。
3. 从有效缓存或 HMS 读取 alice 的角色关系与两张表的授权。
4. orders 命中 ROLE analyst 的 SELECT；customers 命中 USER alice 的 SELECT。
5. 两张表均通过，执行查询。
6. Hive 撤销 analyst 对 orders 的 SELECT 后，在约定的缓存时效之后，新鉴权拒绝该查询，除非还有其他有效授权路径。

## 6. 本期不做

| 能力 | 不纳入原因 |
|---|---|
| Ranger 对接 | 使用 HMS 原生授权作为来源 |
| 行过滤、列授权、列脱敏 | 超出表级 SELECT 范围 |
| HMS 权限控制 INSERT/UPDATE/DELETE/DDL | 需要额外的操作权限映射与验证 |
| Doris 向 HMS 执行 GRANT/REVOKE | 授权继续在 Hive 侧管理 |
| Doris 创建/删除 HMS 角色及 SET ROLE | 首期采用固定的有效角色规则 |
| 全量镜像到 Doris 本地权限表 | 避免维护第二份持久化授权 |
| 全量定时扫描权限 | 按需缓存即可覆盖首期查询 |
| 新增隐藏无权限库表功能 | 与是否允许 SELECT 分开立项 |
| SHOW/DESC 的新增 HMS 鉴权规则 | 查看库表名称和字段信息不属于本次新增功能，仅保持现有接口兼容 |
| 撤权自动取消在途查询 | 需要独立的查询终止机制 |
| 多 FE 权限原子切换 | 首期采用有界陈旧的缓存语义 |
| 主动刷新命令及跨 FE 失效广播 | 作为 TTL 方案之外的后续增强 |
| FE/BE、内置 Connector 或 SPI 修改 | 不修改内核是硬性前提，不能作为实现补丁 |
| 插件打包、安装及加载机制验证 | 用户已明确不属于本次工作，使用已有加载能力 |
| Doris 多版本接口适配 | 接口基线已固定为 `branch-4.1`；HMS 2/3/4 兼容验证仍保留 |

## 7. 按什么顺序做

| 阶段 | 做到什么程度 | 主要功能 |
|---|---|---|
| A：读取与接口实现 | 按 `branch-4.1` 接口实现鉴权入口，独立连接 HMS 并读取 USER/ROLE 授权 | F01、F02、F03 |
| B：基本查询可用 | 模式开启、USER/ROLE 合并、查询拒绝与允许 | F01、F04、F05 |
| C：补齐缓存和异常处理 | 缓存、多 FE 时效、生命周期、故障及兼容性 | F06、F07、F08 |
| D：测试和使用说明 | 回归、性能验证和使用说明 | F09 |

功能已经梳理到可以开始看实现细节的程度。接口按 `branch-4.1`，打包安装不用管，这两点已经定了。之前说的 2–3 周只是粗估，具体时间还得看接口实现和 HMS 联调的工作量。

## 8. 开发前还要定的几个细节

| 事项 | 建议做法 | 影响什么 |
|---|---|---|
| Hive/HMS 版本及权限来源 | 先固定联调版本，验证 SQL Standard 授权确实存在 | 不同部署不一定启用或开放这些 API |
| 插件连接配置 | 显式配置与 Catalog 对应的 HMS 地址、命名空间及读取身份 | 插件独立连接，需防止元数据与授权来源不一致 |
| Doris 管理员例外 | 保留明确的 root/管理员例外；普通用户仍必须查 HMS | 需要与现有全局权限提前放行路径一致 |
| 视图语义 | 首期要求访问者拥有展开后 Hive 基表 SELECT | 不自动承诺仅授予视图即可访问 |
| 写入和 DDL | 建议启用模式的 Catalog 作为只读入口，明确拒绝数据/对象写操作 | 未实现写权限映射不能被误解为已支持 |
| 权限 TTL | 默认 30 秒，可配置及关闭跨查询缓存 | 决定撤权延迟与 HMS 负载 |
| 本地权限关系 | 普通用户本地 SELECT 不替代 HMS 表权限 | 防止合并两套授权意外扩大访问范围 |
| public/admin 规则 | public 启用，HMS admin 不自动启用 | 明确与 Trino 默认行为的对应关系 |

做到这些部分之前，先把规则定下来，再按定下来的规则写测试。

## 9. 参考依据

- [Trino Hive SQL Standard 授权文档](https://trino.io/docs/current/connector/hive.html#sql-standard-based-authorization)：默认角色启用规则及该模式的范围。
- [Hive SQL Standard 用户与角色命名规则](https://cwiki.apache.org/confluence/display/Hive/SQL%2BStandard%2BBased%2BHive%2BAuthorization)：USER 区分大小写、ROLE 不区分大小写。
- [Hive DDL 文档](https://hive.apache.org/docs/latest/language/languagemanual-ddl/)及 [HMS 表权限实现](https://github.com/apache/hive/blob/rel/release-3.1.3/standalone-metastore/src/main/java/org/apache/hadoop/hive/metastore/ObjectStore.java)：表名大小写语义及库表权限对象规范化。
- [Trino SqlStandardAccessControl](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/security/SqlStandardAccessControl.java)：基于目标表授权和有效身份的检查。
- [Trino 角色解析](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/metastore/thrift/ThriftMetastoreUtil.java)：USER、有效 ROLE、public 和 admin 的处理。
- [Hive 3.1.3 Thrift 定义](https://github.com/apache/hive/blob/rel/release-3.1.3/standalone-metastore/src/main/thrift/hive_metastore.thrift)：角色授予关系、表授权和分类权限的数据结构与 API。
- [Hive 2.3.9 Thrift 定义](https://github.com/apache/hive/blob/rel/release-2.3.9/metastore/if/hive_metastore.thrift)和 [Hive 4.0.1 Thrift 定义](https://github.com/apache/hive/blob/rel/release-4.0.1/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift)：跨版本权限接口及字段核对。
- [Hive 3.1.3 MetastoreConf](https://github.com/apache/hive/blob/rel/release-3.1.3/standalone-metastore/src/main/java/org/apache/hadoop/hive/metastore/conf/MetastoreConf.java)：默认 HMS Catalog 配置。
- [Hive 3.1.3 ObjectStore](https://github.com/apache/hive/blob/rel/release-3.1.3/standalone-metastore/src/main/java/org/apache/hadoop/hive/metastore/ObjectStore.java)：按用户读取表权限及展开角色层级的实现。

参考内容基于本次讨论中的源码核对。Trino 的 current/master 链接会变化，实施时应固定用于兼容验证的版本。本方案中的独立权限 TTL、模式边界等建议属于 Doris 需求设计，不表示 Trino 已有同名配置或完全相同的行为。

## 10. 不改内核，插件怎么接入

### 10.1 插件负责什么

采用 FE 的 Catalog 鉴权插件扩展机制，主要通过 `AccessControllerFactory`、`CatalogAccessController` 接入。以下逻辑应归属插件：

- HMS 授权信息的读取适配。
- USER/ROLE 有效权限合并。
- 表级 SELECT 判定，以及将列检查入口归约为表级判定。
- 权限缓存、TTL、加载合并和错误处理。
- 插件配置、日志、指标及资源清理。

不新增 SQL 授权语法，不修改 FE/BE 核心代码、内置 Connector 或 SPI。这里的“插件”指 Doris FE 鉴权插件，不是 Codex 插件。

### 10.2 独立 HMS 连接方案

插件通过已有 `AccessControllerFactory` 属性入口接收独立的 HMS 参数，创建自己的客户端连接池和权限缓存。

```text
原版 Doris FE 现有查询鉴权入口
  → 现有 CatalogAccessController 插件接口
  → HMS 鉴权插件：用户/角色判断与权限缓存
  → 插件自己的 HMS 连接池
  → 与目标 Catalog 对应的 HMS
```

不共享 Catalog/Connector 的内部客户端，不新增连接访问接口，不通过反射访问内部连接池。插件依赖的 Hive/Hadoop/Thrift 库必须按现有插件加载机制组织和隔离，不通过替换 Doris 自带依赖解决冲突。

### 10.3 本地全局权限与现有鉴权入口

已有本地代码核对提供了 `hasGlobal` 默认放行方法及覆写方式的参考。最终以 `branch-4.1` 的 `CatalogAccessController` 和调用路径为准，通过该分支现有扩展点落实普通用户与管理员的约定，不依赖其他分支的接口变化。

测试要检查 `branch-4.1` 的 SELECT、计划/结果复用和插件生命周期。SHOW/DESC 只确认接入插件后还能按原有规则工作，不加新权限规则。如果现有入口确实有做不到的地方，就把具体问题说明白，不能跳过鉴权，也不能改内核来补。插件加载已经有了，不需要再作为前置问题讨论。

### 10.4 实现时按这几条来

1. 所有新增逻辑放在插件里，内核不改。
2. 这次做鉴权代码、示例配置、功能说明和测试；打包、安装不用管。
3. 只使用 `branch-4.1` 已有扩展点，不新增 SPI、生命周期回调或 SQL 语法。
4. 插件加载使用现有能力，不开发或重新验证加载机制本身。
5. 在 `branch-4.1` 上测试；可以调整测试所需的运行配置，不改内核代码和内置依赖。
6. Doris 只按这个分支做，HMS 2/3/4 仍然要分别联调。
