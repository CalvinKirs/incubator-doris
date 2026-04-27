# Cloud 与内核 S3 Properties Unification 设计方案

日期：2026-04-27  
状态：方案草案  
范围：本期端到端覆盖内核普通 `S3Resource`、Cloud `S3StorageVault` / `StorageVaultMgr`、`ObjectStoreInfoPB`、Cloud MetaService / Recycler、BE `ObjectStoreInfoPB` 读取路径

## 1. 背景

Cloud 和内核当前在 S3-compatible object storage 的用户配置行为上没有完全统一，主要体现在参数入口、alias、默认值、校验规则、认证方式和最终访问能力上：

1. 内核普通 S3-compatible 路径逐步收敛到 `StorageProperties` / `AbstractS3CompatibleProperties` 体系，支持 `s3.*`、`AWS_*`、OSS/COS/OBS 等 provider alias、role、provider chain、session token 等参数入口。
2. 普通 `CREATE RESOURCE type=s3` 由 `S3Resource` 承载，服务 storage policy、冷热分层、S3 TVF resource 复用等非 Cloud-only 场景。
3. Cloud storage vault 入口复用了 `S3Resource` 的属性处理，但最终通过 `StorageVaultMgr` 构造 `ObjectStoreInfoPB`。这条链路仍有不少代码逐个 key 取值。
4. Cloud MetaService、Recycler 和 BE 从 PB / thrift / map 读取 S3 配置时也存在字段能力不一致，例如 session token、provider chain、role 相关字段没有完全对齐。

这导致同一类参数在不同入口的用户体验不一致：某些入口支持 alias，某些入口不支持；某些入口会处理 role，某些入口只处理 AK/SK；Cloud 和内核对 session token、provider chain、path style 等能力的表现也不同。

本期目标不是继续补局部兼容逻辑，而是把 S3-compatible 用户配置行为的唯一维护点收敛到 `StorageProperties` / `AbstractS3CompatibleProperties` 体系：参数解析、provider 识别、alias 归一、默认值、校验、认证方式互斥、脱敏 key、以及生成 `ObjectStoreInfoPB` / `TS3StorageParam` / backend properties 的转换逻辑都从同一个 parsed storage properties 对象出发。Cloud 和内核在用户可见行为上应完全统一，差异只能是下游 wire 格式或组件部署能力，并且这些差异应通过补齐后端能力解决。

## 1.1 `CREATE RESOURCE` 当前使用面

`S3Resource` 不是 Cloud-only。它是普通 `CREATE RESOURCE type=s3` 的实现，当前至少被以下业务使用：

1. Storage policy / 冷热分层 / tiered storage。用户先 `CREATE RESOURCE type=s3`，再在 `CREATE STORAGE POLICY` 中通过 `storage_resource` 引用。FE 会把 `S3Resource` 转成 `TS3StorageParam` 推给 BE。
2. S3 TVF resource 复用。`S3(...)` TVF 可以通过 `"resource" = "<resource_name>"` 复用 `S3Resource` 中的连接和凭据参数。
3. Cloud storage vault 内部复用。用户入口是 `CREATE STORAGE VAULT`，但 `S3StorageVault` 复用了 `S3Resource` 的部分属性处理逻辑，最终再生成 Cloud `ObjectStoreInfoPB`。

因此实现时不再拆 `RESOURCE` 和 `STORAGE_VAULT` 两套 S3-compatible 行为。普通 `CREATE RESOURCE type=s3` 和 Cloud `CREATE STORAGE VAULT type=s3` 应共享同一套 `StorageProperties` provider 识别、`AbstractS3CompatibleProperties` 解析、归一、校验和凭据语义；差异只体现在输出目标不同，例如 `TS3StorageParam` 或 `ObjectStoreInfoPB`。

## 1.2 `S3Resource` 到实际访问的参数消费链路

当前 `S3Resource` 的下游消费大多已经会经过 `StorageProperties.createPrimary()` 并落到具体的 S3-compatible properties，例如 `S3Properties`、`OSSProperties`、`COSProperties`、`OBSProperties` 等，但 `S3Resource` 入口层本身还没有完全对象化为这套 properties 体系。

1. S3 TVF resource 复用路径：`ExternalFileTableValuedFunction.parseCommonProperties()` 先读取 `resource.getCopiedProperties()`，再用 TVF 显式参数覆盖 resource 参数；随后 `S3TableValuedFunction` 调用 `StorageProperties.createPrimary()`，根据 provider 识别实例化具体的 S3-compatible properties 并生成 backend connect properties。因此这条链路的最终访问参数已经走了统一 properties 体系。
2. Storage policy / 冷热分层路径：FE 不直接访问对象存储，而是在 `PushStoragePolicyTask` 中把 `S3Resource.getCopiedProperties()` 转成 `TS3StorageParam` 下发给 BE，真正访问发生在 BE。这条链路应改为先经 `StorageProperties.createPrimary()` 识别具体 provider，再由统一实例生成 `TS3StorageParam`。
3. Cloud storage vault 路径：`S3StorageVault` 复用 `S3Resource` 保存属性，`StorageVaultMgr` 通过 `S3Properties.getObjStoreInfoPB()` 生成 `ObjectStoreInfoPB`。这条链路目前已复用部分 static helper，但仍需要改为经 `StorageProperties.createPrimary()` / S3-compatible properties 实例补齐 provider 识别、统一校验、版本门禁和后端读取。
4. `S3Resource` 自身 create/alter/ping 仍是 `Map<String, String>` + static helper + 手工补齐 endpoint/region 的模式。第一阶段统一的重点不是否认下游已经复用 properties 体系，而是把入口解析、alias 合并、provider 识别、凭据模型、互斥清理都收敛到同一个 parsed storage properties 实例 API。

## 2. 目标

第一阶段目标是把 Cloud 和内核的 S3-compatible 参数入口、认证方式和访问能力统一收敛到 `StorageProperties` / `AbstractS3CompatibleProperties` 体系，并在同一期补齐 Cloud storage vault 真实访问链路：

1. 内核普通 `S3Resource` 与 Cloud `StorageVault` 用户输入统一先归一到 canonical `s3.*`。
2. AK/SK、AK/SK/session token、role ARN、external ID、provider chain、`use_path_style` 等凭据和连接参数统一由具体的 S3-compatible properties 实例解析、校验和互斥清理。
3. `ObjectStoreInfoPB`、`TS3StorageParam`、backend properties 都从统一 parsed storage properties 实例生成，调用方不再逐个 key 自行拼装 S3 参数。
4. Cloud storage vault 在本期对齐可持久化的 S3-compatible properties 能力，包括 AK/SK + session token、role ARN + external ID、provider 识别、以及可通过枚举稳定表达的 credentials provider type。
5. 扩展 `ObjectStoreInfoPB`、Cloud MetaService 加密/脱敏、Cloud Recycler 和 BE `ObjectStoreInfoPB` 读取逻辑，避免入口接受参数但后端静默丢弃。
6. 保持普通 `CREATE RESOURCE type=s3` 的业务使用面，同时确保它和 Cloud storage vault 共享同一套 S3-compatible 用户行为。

## 3. 非目标

第一阶段不做：

1. 不把 Cloud stage 的 `access_type`、`role_name` 纳入 S3-compatible properties。它们是 stage 语义，不是通用 S3 参数。
2. 不实现 session token 自动刷新、expires-at 调度或外部刷新协议。本期只支持用户显式传入的静态 session token，并保证不会被静默丢弃。
3. 不持久化本地环境变量、web identity token 文件内容、container metadata 返回值等运行时派生凭据。provider chain 只持久化 provider type，实际凭据仍由各进程运行环境提供。
4. 不把 `S3Resource` 视为 Cloud-only，不移除或弱化 storage policy、冷热分层、S3 TVF 等普通 resource 使用场景。

## 4. 核心设计

统一入口使用 `StorageProperties.createPrimary(...)` 识别具体 S3-compatible provider：

```java
StorageProperties storage = StorageProperties.createPrimary(properties);
if (!(storage instanceof AbstractS3CompatibleProperties)) {
    throw new DdlException("Only S3-compatible storage properties are supported");
}
AbstractS3CompatibleProperties objStorage = (AbstractS3CompatibleProperties) storage;
Cloud.ObjectStoreInfoPB.Builder obj = objStorage.toObjectStoreInfoPB();
TS3StorageParam param = objStorage.toS3TStorageParam();
Map<String, String> canonical = objStorage.getCanonicalProperties();
```

`StorageProperties.createPrimary(...)` 是 provider 识别入口，负责根据 `fs.xx.support`、`provider` / `s3.provider` hint、endpoint/uri guess 等规则选择 `S3Properties`、`OSSProperties`、`COSProperties`、`OBSProperties`、`MinioProperties` 等具体实现。对于 `CREATE RESOURCE type=s3` 和 `CREATE STORAGE VAULT type=s3` 这类用户语义上属于 S3-compatible 的入口，调用方应使用该入口识别具体 provider，再要求结果必须是 `AbstractS3CompatibleProperties`。这样 OSS/COS/OBS 等不会被错误当成 AWS S3，也不需要 Cloud 和内核分别维护 provider 判断。

如果 `StorageProperties.createPrimary(...)` 对 S3-compatible 入口过宽，可以新增受限工厂，例如 `StorageProperties.createS3Compatible(properties)`。这个工厂只在 S3-compatible provider registry 中选择，不允许返回 HDFS、BROKER、LOCAL 等非对象存储类型。无论采用现有 `createPrimary` 还是新增受限工厂，核心原则都是：provider 识别和参数绑定只维护在 `StorageProperties` / `AbstractS3CompatibleProperties` 体系内。

统一后只保留一套行为边界：

1. 输入语义一致：同一组 `s3.*` / `AWS_*` / OSS/COS/OBS 等 historical alias 在内核 `S3Resource`、Cloud storage vault、TVF resource 复用路径中的解析结果一致。
2. 凭据语义一致：AK/SK、AK/SK/token、role ARN + external ID、provider chain 的校验和互斥清理一致。
3. 输出适配不同：普通 resource 输出 `TS3StorageParam` 或 backend properties；Cloud vault 输出 `ObjectStoreInfoPB`。输出缺字段时应补齐 wire 能力，而不是在调用方拆出不同行为。

现有 static helper 保留为兼容 wrapper：

1. `convertToStdProperties`
2. `requiredS3Properties`
3. `requiredS3PingProperties`
4. `getObjStoreInfoPB`
5. `getS3TStorageParam`

新实现应让这些 helper 委托 `AbstractS3CompatibleProperties` 实例 API，避免 static 逻辑继续扩张。

## 5. 参数归一

所有入口先归一到 canonical key：

| 旧 key / alias | canonical key |
| --- | --- |
| `AWS_ENDPOINT`, `endpoint` | `s3.endpoint` |
| `AWS_REGION`, `region` | `s3.region` |
| `AWS_ACCESS_KEY`, `access_key` | `s3.access_key` |
| `AWS_SECRET_KEY`, `secret_key` | `s3.secret_key` |
| `AWS_TOKEN`, `session_token`, `s3.session-token` | `s3.session_token` |
| `AWS_BUCKET`, `bucket` | `s3.bucket` |
| `AWS_ROOT_PATH`, `root_path`, `prefix` | `s3.root.path` |
| `AWS_ROLE_ARN` | `s3.role_arn` |
| `AWS_EXTERNAL_ID` | `s3.external_id` |
| `provider` | `provider` |
| `use_path_style`, `s3.path-style-access` | `use_path_style` |

归一规则：

1. canonical key 优先。
2. 只有 alias 时写入 canonical key。
3. canonical key 和 alias 值相同则接受。
4. 多个 alias 值冲突且没有 canonical key 时，分析失败并报出冲突 key。
5. 不做全局 case-insensitive map，只枚举历史兼容 key，避免影响其他属性。

## 6. 凭据模型

`AbstractS3CompatibleProperties` 统一表达以下凭据形态：

1. AK/SK：`s3.access_key` + `s3.secret_key`。
2. AK/SK/token：`s3.access_key` + `s3.secret_key` + `s3.session_token`。
3. Role：`s3.role_arn` + optional `s3.external_id`。
4. Provider chain：`s3.credentials_provider_type`。

本期统一支持以下凭据形态：

1. 支持 AK/SK。
2. 支持 AK/SK + session token。token 只能和 AK/SK 同时使用，切换到 role 或 provider chain 时必须清理。
3. 支持 role ARN + optional external ID。role ARN 和 AK/SK 仍互斥。
4. 支持可持久表达的 provider chain：至少覆盖 `DEFAULT`、`SIMPLE`、`INSTANCE_PROFILE`，并同步扩展 `CredProviderTypePB` / `TCredProviderType` 到 `ENV`、`SYSTEM_PROPERTIES`、`WEB_IDENTITY`、`CONTAINER`、`ANONYMOUS`，使 Cloud vault、storage policy 和 backend map 路径语义一致。
5. provider chain 不携带具体临时凭据。使用 `ENV`、`WEB_IDENTITY`、`CONTAINER` 等模式时，FE、BE、Recycler、MetaService 所在运行环境必须具备对应配置；否则创建或访问时应返回明确错误。

## 7. Cloud 能力差异

| 能力 | 内核 S3-compatible properties 路径 | Cloud storage vault 当前状态 | 本期处理 |
| --- | --- | --- | --- |
| AK/SK | 支持 | 支持，MetaService 会加密保存 AK/SK | 对齐到 S3-compatible properties |
| AK/SK + session token | S3-compatible properties 和 `TS3StorageParam` 支持 | 不支持。`ObjectStoreInfoPB` 没有 token 字段，BE/Recycler 从 PB 构造 `S3Conf` 时 token 为空 | 本期补齐：PB 增加 token，MetaService 加密/脱敏，BE/Recycler 读取并创建 session credentials |
| token 过期时间 / refresh | 有部分属性雏形或其他设计路线 | 不支持。PB 无 expires 字段，也无刷新协议 | 本期不做自动刷新，只保证显式 token 可持久化、可脱敏、可访问 |
| role ARN + external ID | 支持 | 部分支持。Cloud 只允许 `provider=S3` 且 `cred_provider_type=INSTANCE_PROFILE` | 本期保持 role ARN 使用 `INSTANCE_PROFILE` base provider，FE 统一解析后按 Cloud 真实访问链路校验 |
| generic provider chain | S3-compatible properties 支持多种模式 | 不完整。PB 只有 `DEFAULT/SIMPLE/INSTANCE_PROFILE`，MetaService 创建 vault 实际要求 AK/SK 或 role ARN，Recycler 没有完整 provider factory | 本期补齐可持久化 provider type；FE/Thrift/PB/BE/Recycler 同步更新 |
| web identity / env / system properties | 内核路径可表达部分模式 | Cloud vault 目前不能完整表达 | 本期只持久化 provider type，不持久化派生凭据；要求运行环境对 FE/BE/Recycler 一致可用 |
| `use_path_style` | 支持 | PB 有字段，BE/Recycler 会读取 | 对齐 |
| `external_endpoint` | S3-compatible properties 有 `s3.external_endpoint` | PB 有字段，展示路径也在用 | 保留并对齐 |
| provider 大小写和枚举 | 可统一校验 | PB 枚举固定 | FE 归一后写 PB |
| 凭据加密 | FE 不负责持久加密 | Cloud 只加密 AK/SK；token 没有字段和加密链路 | 本期扩展为 credential secret 加密，AK/SK/token 都不能明文落盘或明文输出日志 |

关键结论：

1. 本期是 Cloud 和内核端到端能力补齐，不能只在入口侧接受参数。
2. `s3.session_token` / `AWS_TOKEN` 在普通 `TS3StorageParam` 路径和 Cloud storage vault 路径都应可用。
3. 入口侧只能在确认 MetaService / BE / Recycler 都支持对应字段后放开 Cloud vault token，否则必须通过版本门禁报错，不能静默降级。
4. `S3Resource` 和 Cloud storage vault 的 S3 参数行为应保持一致。Cloud 侧如果缺 wire 字段或读取逻辑，本期补齐后端能力，而不是在 FE 入口拆出不同校验。

## 8. 调用点迁移

### 8.1 `S3Resource`

`S3Resource.setProperties()`：

1. 调用 `StorageProperties.createPrimary(properties)` 或受限的 `StorageProperties.createS3Compatible(properties)`，并要求结果是 `AbstractS3CompatibleProperties`。
2. 使用 canonical map 执行 required 校验、endpoint scheme 补齐、region 推断和 ping。
3. 保留 `s3_validity_check` 行为。
4. 保留 role 和 AK/SK 的互斥清理语义。
5. 保留普通 resource 到 `TS3StorageParam` 的能力，包括 session token。

`S3Resource.modifyProperties()`：

1. 先 normalize 修改参数。
2. 对不可修改字段使用 canonical key 判断。
3. role 与 AK/SK 互斥时，同时清理 canonical key 和 legacy alias，避免旧值残留。

### 8.2 `S3StorageVault`

`S3StorageVault.checkCreationProperties()`：

1. 不再只检查 `s3.root.path`。
2. 调用 `StorageProperties.createPrimary(properties)` 或受限的 `StorageProperties.createS3Compatible(properties)`，复用和 `S3Resource` 一致的 provider 识别、解析、校验和凭据互斥规则。
3. `s3.root.path` 继续映射到 `ObjectStoreInfoPB.prefix`。

### 8.3 `StorageVaultMgr`

`buildAlterS3VaultRequest()`：

1. 不再逐个 key 读取 map。
2. 使用 S3-compatible properties 实例的 `toObjectStoreInfoPB()`。
3. 保留 `VAULT_NAME` rename 逻辑。

### 8.4 `ObjectStoreInfoPB` / MetaService / BE / Recycler

`cloud.proto`：

1. `ObjectStoreInfoPB` 增加 optional token 字段，用于保存 encrypted session token。
2. `CredProviderTypePB` 同步增加 `ENV`、`SYSTEM_PROPERTIES`、`WEB_IDENTITY`、`CONTAINER`、`ANONYMOUS`；`gensrc/thrift/AgentService.thrift` 的 `TCredProviderType` 同步扩展，避免 storage policy 和 Cloud vault 语义再次分叉。

Cloud MetaService：

1. create / alter vault 时接受 AK/SK + optional token。AK/SK/token 使用同一套 credential secret 加密策略；旧 metadata 只有 AK/SK 时继续按旧逻辑解密。
2. AK/SK/token、role ARN、provider chain 三类凭据保持互斥清理。切到 role/provider chain 时清理 AK/SK/token/encryption info；切到 AK/SK 时清理 role/provider chain。
3. 日志、HTTP debug、show / list vault 返回结果必须同时脱敏 `ak`、`sk`、`token`。

BE：

1. `S3Conf::get_s3_conf(ObjectStoreInfoPB)` 从 PB 读取 token 并写入 `S3ClientConf.token`。
2. `cred_provider_type_from_pb()` 与扩展后的 `CredProviderTypePB` 对齐。

Cloud Recycler：

1. `S3Conf` 增加 token 字段，`from_obj_store_info()` 解密并读取 token。
2. credentials provider 创建逻辑与 BE 对齐，至少保证 AK/SK/token 和 role ARN 行为一致。

## 9. 兼容性策略

### 9.1 输入兼容

继续接受历史 SQL 中的 `AWS_*`、canonical `s3.*`、以及 Cloud 常见短 key。新实现只改变冲突行为：历史上可能静默选择某个值的冲突输入，迁移后应失败。

普通 `CREATE RESOURCE type=s3` 和 `CREATE STORAGE VAULT type=s3` 共享同一套 S3 参数兼容规则。Cloud vault 的组件版本门禁属于发布和部署约束，不应变成另一套用户参数语义。

### 9.2 Metadata 兼容

PB 只做 optional 字段和枚举追加，不改变既有字段编号和语义。FE edit log/image 格式不因 normalize 单独变更；旧对象加载后只在 FE 使用时做内存 normalize。新建或 alter 后可以保存 canonical key，但不能破坏旧对象读取。

### 9.3 展示兼容

`SHOW CREATE STORAGE VAULT` 继续展示 canonical `s3.*`。`SHOW PROC`、错误日志和 `DatasourcePrintableMap` 必须覆盖 canonical key 和 legacy alias 的脱敏，包括 access key、secret key、session token、role/external id 相关敏感项。

### 9.4 Wire 兼容

`ObjectStoreInfoPB` 和 `TS3StorageParam` 保持既有字段语义不变，只追加缺失字段 / 枚举：

| canonical property | wire field |
| --- | --- |
| `s3.endpoint` | `endpoint` |
| `s3.region` | `region` |
| `s3.bucket` | `bucket` |
| `s3.root.path` | `prefix` / `rootPath` |
| `s3.access_key` | `ak` |
| `s3.secret_key` | `sk` |
| `s3.session_token` | `ObjectStoreInfoPB.token` / `TS3StorageParam.token` |
| `s3.role_arn` | `roleArn` / `role_arn` |
| `s3.external_id` | `externalId` / `external_id` |
| `s3.credentials_provider_type` | `credProviderType` / `cred_provider_type` |
| `use_path_style` | `usePathStyle` / `use_path_style` |
| `provider` | `provider` |

Cloud storage vault 本期会新增 `ObjectStoreInfoPB.token`，并保持 optional 字段兼容。旧 metadata 没有 token 时行为不变；新 metadata 带 token 时要求 MetaService、BE、Recycler 都已升级到可识别字段的版本。

## 10. 风险

1. Alias 冲突导致部分历史 SQL 失败。需要明确错误信息，说明 canonical key 和 alias 的冲突关系。
2. Role 与 AK/SK 清理不完整会导致 alter 后残留旧凭据。迁移时必须保持现有互斥语义。
3. `s3.root.path` 和 stage `prefix` 语义不同。本阶段不迁移 stage，避免混淆。
4. Provider chain 容易被误解为 Cloud vault 会持久化真实临时凭据。文档和校验都要明确只持久化 provider type，实际凭据由运行环境提供。
5. Session token 如果在混部版本中被旧 MetaService / BE / Recycler 忽略，会造成用户误以为短期凭据生效。需要版本门禁或明确的部署顺序。
6. Redaction 漏敏风险增加。新增 alias 或 canonical 输出后必须补齐脱敏测试。
7. Endpoint scheme 和 region 推断可能改变 ping 行为。应先用 UT 锁定旧行为，再迁移。
8. 如果把 Cloud vault 的部署门禁、PB 输出约束或 MetaService 约束错误应用到 `S3Resource`，会破坏 storage policy、冷热分层、S3 TVF resource 复用等普通 `CREATE RESOURCE` 场景。
9. token 加密复用 AK/SK encryption info 时要保证旧 metadata 兼容，不能让旧 AK/SK 解密路径失败。

## 11. 测试计划

本期需要覆盖 FE、Cloud MetaService、BE 和 Recycler：

1. `StorageProperties` / `AbstractS3CompatibleProperties` 单测
   - canonical key 和 `AWS_*` alias 归一。
   - alias 冲突报错。
   - `S3Resource` / `S3StorageVault` 使用相同输入得到相同 canonical properties。
   - AK/SK、role、session token、provider chain 的统一校验和互斥清理。
2. `S3ResourceTest`
   - `AWS_*` 和 `s3.*` 输入得到一致 canonical properties。
   - role 与 AK/SK alter 互斥清理语义保持不变。
   - session token 在普通 resource / `TS3StorageParam` 路径继续可用。
   - storage policy 所需的 `s3.root.path` / `s3.bucket` 校验保持不变。
   - S3 TVF 通过 `"resource" = "<resource_name>"` 复用 resource 参数的行为保持不变。
3. `S3StorageVaultTest` / `StorageVaultMgrTest`
   - old key 和 canonical key 生成相同 `ObjectStoreInfoPB`。
   - `s3.session_token` / `AWS_TOKEN` 在 storage vault 下写入 `ObjectStoreInfoPB.token`。
   - role ARN 仅在本期定义的 provider/cred provider 组合下通过。
4. `ShowCreateStorageVaultCommandTest`
   - canonical 输出稳定。
   - 敏感字段脱敏。
5. Cloud MetaService 单测
   - create / alter vault 时 AK/SK/token 加密保存，读出和下发时正确解密。
   - AK/SK/token、role、provider chain 互斥清理。
   - 日志和返回结果不暴露 token。
6. BE / Recycler 单测
   - `ObjectStoreInfoPB.token` 能进入 `S3Conf` / `S3ClientConf`。
   - 扩展后的 `CredProviderTypePB` 映射一致。

不优先增加 regression，除非实现过程中改变了 SQL 用户可见行为或错误信息。

## 12. 后续阶段

后续可以分三步继续推进：

1. 将 Cloud stage 的 object storage 参数接入 S3-compatible properties 体系，但保留 `access_type` / `role_name` 为 stage 自有语义。
2. 设计 session token expires-at、自动刷新和外部刷新协议。
3. 如果 provider chain 需要持久化更多运行时参数，例如 web identity token file path、profile name、container credentials URI，应单独设计安全边界和脱敏规则。
