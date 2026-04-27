# FE Cloud S3 Properties Unification 设计方案

日期：2026-04-27  
状态：方案草案  
范围：FE-only，先覆盖 `S3Resource` / `S3StorageVault` / `StorageVaultMgr`

## 1. 背景

FE 当前存在两套 S3-compatible object storage 参数处理方式：

1. 普通 FE 存储路径逐步收敛到 `S3Properties`，支持 `s3.*`、`AWS_*`、role、provider chain、session token 等参数入口。
2. Cloud 相关路径仍有不少代码逐个 key 取值，例如 `S3Resource`、`S3StorageVault`、`StorageVaultMgr` 构造 `ObjectStoreInfoPB` 时直接读取 map。

这导致同一类参数在不同入口的行为不一致：某些入口支持 alias，某些入口不支持；某些入口会处理 role，某些入口只处理 AK/SK；Cloud storage vault 的真实能力也容易和 `S3Properties` 的普通 FE 能力混在一起。

## 2. 目标

第一阶段目标是把 FE Cloud 参数入口收敛到 `S3Properties`：

1. `StorageVault` / `S3Resource` 用户输入统一先归一到 canonical `s3.*`。
2. AK/SK、role ARN、external ID、provider、`use_path_style` 等凭据和连接参数统一由 `S3Properties` 解析和校验。
3. `ObjectStoreInfoPB`、`TS3StorageParam`、backend properties 都从 `S3Properties` 实例生成。
4. 保留旧 SQL 和旧 metadata 兼容，不改 Cloud MetaService protobuf，不改 Cloud/BE C++ 读取逻辑。
5. 明确写清 Cloud storage vault 当前没有对齐的能力，避免 FE 统一后误认为 Cloud 已支持全部 `S3Properties` 能力。

## 3. 非目标

第一阶段不做：

1. 不修改 `cloud.proto` 的 `ObjectStoreInfoPB`。
2. 不修改 `cloud/src` / `be/src` 中 PB 到 `S3Conf` 的读取逻辑。
3. 不把 Cloud stage 的 `access_type`、`role_name` 纳入 `S3Properties`。这些是 stage 语义，不是通用 S3 参数。
4. 不承诺 Cloud storage vault 支持 session token、token refresh、web identity、environment/system property provider 等普通 FE 路径可表达但 Cloud 未完整支持的能力。

## 4. 核心设计

在 `S3Properties` 中新增用例化入口：

```java
S3Properties s3 = S3Properties.forUseCase(properties, S3PropertyUseCase.STORAGE_VAULT);
s3.validateFor(S3PropertyUseCase.STORAGE_VAULT);
Cloud.ObjectStoreInfoPB.Builder obj = s3.toObjectStoreInfoPB();
TS3StorageParam param = s3.toS3TStorageParam();
Map<String, String> canonical = s3.getCanonicalProperties();
```

新增 `S3PropertyUseCase`，第一阶段至少包含：

```java
RESOURCE
STORAGE_VAULT
```

现有 static helper 保留为兼容 wrapper：

1. `convertToStdProperties`
2. `requiredS3Properties`
3. `requiredS3PingProperties`
4. `getObjStoreInfoPB`
5. `getS3TStorageParam`

新实现应让这些 helper 委托实例 API，避免 static 逻辑继续扩张。

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

`S3Properties` 统一表达以下凭据形态：

1. AK/SK：`s3.access_key` + `s3.secret_key`。
2. AK/SK/token：`s3.access_key` + `s3.secret_key` + `s3.session_token`。
3. Role：`s3.role_arn` + optional `s3.external_id`。
4. Provider chain：`s3.credentials_provider_type`。

但是 `S3PropertyUseCase.STORAGE_VAULT` 必须按 Cloud 当前能力收紧校验：

1. 支持 AK/SK。
2. 支持 role ARN，但仅限 Cloud 已支持的 `provider=S3` + `cred_provider_type=INSTANCE_PROFILE` 路径。
3. 不支持 session token。若用户传入 `s3.session_token` / `AWS_TOKEN`，应直接报错，不允许静默忽略。
4. 不承诺 generic provider chain。Cloud MetaService 当前创建 vault 仍要求 AK/SK 或 role ARN，不能仅凭 `s3.credentials_provider_type` 创建 vault。

## 7. Cloud 能力差异

| 能力 | 普通 `S3Properties` / FE 路径 | Cloud storage vault 当前状态 | 第一阶段处理 |
| --- | --- | --- | --- |
| AK/SK | 支持 | 支持，MetaService 会加密保存 AK/SK | 对齐到 `S3Properties` |
| AK/SK + session token | `S3Properties` 和 `TS3StorageParam` 支持 | 不支持。`ObjectStoreInfoPB` 没有 token 字段，BE/Recycler 从 PB 构造 `S3Conf` 时 token 为空 | `STORAGE_VAULT` 下直接报错 |
| token 过期时间 / refresh | 有部分属性雏形或其他设计路线 | 不支持。PB 无 expires 字段，也无刷新协议 | 不纳入第一阶段 |
| role ARN + external ID | 支持 | 部分支持。Cloud 只允许 `provider=S3` 且 `cred_provider_type=INSTANCE_PROFILE` | 保持现状，FE 统一解析后按 Cloud 当前规则校验 |
| generic provider chain | `S3Properties` 支持多种模式 | 不完整。PB 只有 `DEFAULT/SIMPLE/INSTANCE_PROFILE`，MetaService 创建 vault 实际要求 AK/SK 或 role ARN | 第一阶段不承诺 Cloud vault 支持 |
| web identity / env / system properties | 普通 FE 路径可表达部分模式 | Cloud vault 不支持持久表达，也无法跨进程稳定使用本地环境 | 不纳入第一阶段 |
| `use_path_style` | 支持 | PB 有字段，BE/Recycler 会读取 | 对齐 |
| `external_endpoint` | `S3Properties` 有 `s3.external_endpoint` | PB 有字段，展示路径也在用 | 保留并对齐 |
| provider 大小写和枚举 | 可统一校验 | PB 枚举固定 | FE 归一后写 PB |
| 凭据加密 | FE 不负责持久加密 | Cloud 只加密 AK/SK；token 没有字段和加密链路 | 未来如支持 token，必须同步设计 PB、加密、脱敏和兼容 |

关键结论：

1. 第一阶段是 FE 参数统一，不扩大 Cloud storage vault 的实际能力。
2. `s3.session_token` / `AWS_TOKEN` 在普通 `TS3StorageParam` 路径可用，但在 Cloud storage vault 路径不可用。
3. FE 统一后应 fail fast，不能继续接受 token 后静默丢弃。

## 8. 调用点迁移

### 8.1 `S3Resource`

`S3Resource.setProperties()`：

1. 调用 `S3Properties.forUseCase(properties, RESOURCE)`。
2. 使用 canonical map 执行 required 校验、endpoint scheme 补齐、region 推断和 ping。
3. 保留 `s3_validity_check` 行为。
4. 保留 role 和 AK/SK 的互斥清理语义。

`S3Resource.modifyProperties()`：

1. 先 normalize 修改参数。
2. 对不可修改字段使用 canonical key 判断。
3. role 与 AK/SK 互斥时，同时清理 canonical key 和 legacy alias，避免旧值残留。

### 8.2 `S3StorageVault`

`S3StorageVault.checkCreationProperties()`：

1. 不再只检查 `s3.root.path`。
2. 调用 `S3Properties.forUseCase(properties, STORAGE_VAULT).validateFor(STORAGE_VAULT)`。
3. `s3.root.path` 继续映射到 `ObjectStoreInfoPB.prefix`。

### 8.3 `StorageVaultMgr`

`buildAlterS3VaultRequest()`：

1. 不再逐个 key 读取 map。
2. 使用 `S3Properties.forUseCase(properties, STORAGE_VAULT).toObjectStoreInfoPB()`。
3. 保留 `VAULT_NAME` rename 逻辑。

## 9. 兼容性策略

### 9.1 输入兼容

继续接受历史 SQL 中的 `AWS_*`、canonical `s3.*`、以及 Cloud 常见短 key。新实现只改变冲突行为：历史上可能静默选择某个值的冲突输入，迁移后应失败。

### 9.2 Metadata 兼容

不改 PB，不改 edit log/image 格式。旧对象加载后只在 FE 使用时做内存 normalize，不因为 normalize 单独写新 edit log。新建或 alter 后可以保存 canonical key，但不能破坏旧对象读取。

### 9.3 展示兼容

`SHOW CREATE STORAGE VAULT` 继续展示 canonical `s3.*`。`SHOW PROC`、错误日志和 `DatasourcePrintableMap` 必须覆盖 canonical key 和 legacy alias 的脱敏，包括 access key、secret key、session token、role/external id 相关敏感项。

### 9.4 Wire 兼容

`ObjectStoreInfoPB` 和 `TS3StorageParam` 保持字段不变：

| canonical property | wire field |
| --- | --- |
| `s3.endpoint` | `endpoint` |
| `s3.region` | `region` |
| `s3.bucket` | `bucket` |
| `s3.root.path` | `prefix` / `rootPath` |
| `s3.access_key` | `ak` |
| `s3.secret_key` | `sk` |
| `s3.session_token` | `TS3StorageParam.token` only |
| `s3.role_arn` | `roleArn` / `role_arn` |
| `s3.external_id` | `externalId` / `external_id` |
| `use_path_style` | `usePathStyle` / `use_path_style` |
| `provider` | `provider` |

Cloud storage vault 的 `ObjectStoreInfoPB` 没有 token 字段，所以 `STORAGE_VAULT` 用例不能生成 token。

## 10. 风险

1. Alias 冲突导致部分历史 SQL 失败。需要明确错误信息，说明 canonical key 和 alias 的冲突关系。
2. Role 与 AK/SK 清理不完整会导致 alter 后残留旧凭据。迁移时必须保持现有互斥语义。
3. `s3.root.path` 和 stage `prefix` 语义不同。本阶段不迁移 stage，避免混淆。
4. Provider chain 容易被误解为 Cloud vault 已支持。文档和校验都要明确 Cloud 只支持当前 PB/MetaService 真实能力。
5. Session token 如果继续被静默忽略，会造成用户误以为短期凭据生效。必须在 `STORAGE_VAULT` 下 fail fast。
6. Redaction 漏敏风险增加。新增 alias 或 canonical 输出后必须补齐脱敏测试。
7. Endpoint scheme 和 region 推断可能改变 ping 行为。应先用 UT 锁定旧行为，再迁移。

## 11. 测试计划

FE 单测优先：

1. `S3PropertiesTest`
   - canonical key 和 `AWS_*` alias 归一。
   - alias 冲突报错。
   - `RESOURCE` / `STORAGE_VAULT` required 校验。
   - AK/SK、role、session token、provider chain 的 use-case 差异。
2. `S3ResourceTest`
   - `AWS_*` 和 `s3.*` 输入得到一致 canonical properties。
   - role 与 AK/SK alter 互斥清理语义保持不变。
   - session token 在 resource 普通路径和 Cloud vault 路径的行为分开验证。
3. `S3StorageVaultTest` / `StorageVaultMgrTest`
   - old key 和 canonical key 生成相同 `ObjectStoreInfoPB`。
   - `s3.session_token` / `AWS_TOKEN` 在 storage vault 下报错。
   - role ARN 仅在 Cloud 当前支持的 provider/cred provider 组合下通过。
4. `ShowCreateStorageVaultCommandTest`
   - canonical 输出稳定。
   - 敏感字段脱敏。

不优先增加 regression，除非实现过程中改变了 SQL 用户可见行为或错误信息。

## 12. 后续阶段

后续可以分三步继续推进：

1. 将 Cloud stage 的 object storage 参数接入 `S3Properties`，但保留 `access_type` / `role_name` 为 stage 自有语义。
2. 评估是否扩展 `ObjectStoreInfoPB` 以支持 session token、expires-at 和刷新协议。
3. 在 Cloud/BE 支持完善后，再放开 `STORAGE_VAULT` 对 session token 或更多 provider chain 的校验限制。
