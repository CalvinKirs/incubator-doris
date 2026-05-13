# FE 对象存储 Native SDK 后续改造记录

本文记录 `StorageProperties` / `FileSystemProvider` 参数统一之后，对 OSS/COS/OBS 等对象存储改用厂商 Native SDK 的后续设计计划。

## 背景

当前 SPI 改造中，参数入口已经开始收敛到：

```text
StorageProperties
  -> FileSystemProvider
  -> FileSystemProperties
  -> FileSystem
```

但对象存储底层 I/O 仍然没有完全切到厂商 SDK。

以 OSS 为例，当前 provider 组装的是：

```java
new S3FileSystem(new OssObjStorage(properties.toFileSystemKv()))
```

其中 `OssObjStorage` 目前继承 `S3ObjStorage`，核心 I/O 仍复用 AWS S3 SDK 和 S3-compatible 协议，只在 `getPresignedUrl()`、`getStsToken()` 等云厂商扩展能力上使用 OSS SDK。

这和最终目标不一致。最终目标是：

```text
fs.provider=s3  -> S3FileSystem  -> S3ObjStorage  -> AWS S3 SDK
fs.provider=oss -> OssFileSystem -> OssObjStorage -> OSS SDK
fs.provider=cos -> CosFileSystem -> CosObjStorage -> COS SDK
fs.provider=obs -> ObsFileSystem -> ObsObjStorage -> OBS SDK
```

也就是说，统一的是 FE 参数入口、凭据构建、基础行为和 filesystem 接口；真正访问对象存储时，各 provider 应该使用自己的 SDK client。

## 当前问题

当前实现存在两个层面的耦合。

第一，文件系统层命名和组合关系不干净。

OSS/COS/OBS provider 返回 `S3FileSystem`，虽然底层传入的是对应厂商 `ObjStorage`，但对外看起来仍然是 S3 文件系统。这会让 provider、FileSystem、ObjStorage 三层关系不清晰。

第二，`S3FileSystem` 还没有完全抽象为通用对象存储文件系统。

`S3FileSystem` 中仍存在一些 S3 强绑定逻辑，例如：

- `S3Uri` 解析。
- `S3ObjStorage` 强转。
- `S3ObjStorage.listObjects(..., maxKeys)` 这种非 `ObjStorage` 接口方法。
- 注释和方法语义中直接绑定 S3。

因此不能简单把 provider 改成 `new OssFileSystem(...)` 就认为完成 native SDK 改造。真正关键是先拆清楚通用文件系统语义和具体对象存储 SDK。

## 分阶段计划

### 阶段一：抽出通用对象存储文件系统

先从 `S3FileSystem` 中抽出通用对象存储文件语义，形成类似：

```java
public abstract class ObjectFileSystem extends ObjFileSystem {
    protected ObjectFileSystem(String name, ObjStorage<?> objStorage) {
        super(name, objStorage);
    }
}
```

这一层只依赖 `ObjStorage` 接口，不依赖 `S3ObjStorage`。

需要同步处理：

- 把 `S3Uri` 抽象为通用 `ObjectStorageUri`，用于解析 `s3://bucket/key`、`oss://bucket/key`、`cos://bucket/key`、`obs://bucket/key` 等 URI。
- 把 `S3FileSystem` 中的 `S3ObjStorage` 强转移除，必要时把 `listObjects(..., maxKeys)` 这类能力沉到 `ObjStorage` 接口或用已有接口组合实现。
- 把目录 marker、递归删除、rename/copy、批量 delete、glob/list 等通用对象存储语义放在 `ObjectFileSystem`。
- 保留 S3 特有逻辑在 `S3FileSystem` / `S3ObjStorage`。

阶段一完成后，provider 可以返回明确的文件系统类型：

```java
return new S3FileSystem(properties);
return new OssFileSystem(properties);
return new CosFileSystem(properties);
return new ObsFileSystem(properties);
```

其中 `OssFileSystem`、`CosFileSystem`、`ObsFileSystem` 可以先是薄封装：

```java
public class OssFileSystem extends ObjectFileSystem {
    public OssFileSystem(FileSystemProperties properties) {
        super("OSS", new OssObjStorage(properties.toFileSystemKv()));
    }
}
```

这个阶段只修正 FileSystem/Provider/Properties 的边界，不急于替换所有底层 SDK。

### 阶段二：厂商 ObjStorage 改为 Native SDK 实现

在通用文件系统层稳定之后，再逐个替换厂商 `ObjStorage` 的继承关系。

目标结构：

```java
public class OssObjStorage implements ObjStorage<OSS> {
    // list/head/get/put/delete/copy/multipart/deleteObjectsByKeys all use OSS SDK
}

public class CosObjStorage implements ObjStorage<COSClient> {
    // list/head/get/put/delete/copy/multipart/deleteObjectsByKeys all use COS SDK
}

public class ObsObjStorage implements ObjStorage<ObsClient> {
    // list/head/get/put/delete/copy/multipart/deleteObjectsByKeys all use OBS SDK
}
```

这个阶段要把核心 I/O 全部切到厂商 SDK，包括：

- `getClient`
- `listObjects`
- `headObject`
- `putObject`
- `deleteObject`
- `copyObject`
- `initiateMultipartUpload`
- `uploadPart`
- `completeMultipartUpload`
- `abortMultipartUpload`
- `deleteObjectsByKeys`
- `listObjectsWithPrefix`
- `headObjectWithMeta`
- `getPresignedUrl`
- `getStsToken`

完成后，`OssObjStorage.toS3Props()`、`CosObjStorage.toS3Props()`、`ObsObjStorage.toS3Props()` 这类兼容转换逻辑应该删除。厂商 `ObjStorage` 只消费 provider bind 后的 `FileSystemProperties.toFileSystemKv()`。

### 阶段三：统一行为和回归测试

Native SDK 切换不能只靠单元测试，需要有一套共享 contract test 来保证 S3/OSS/COS/OBS 行为一致。

共享测试应覆盖：

- `exists`
- `list`
- `glob`
- `mkdirs`
- `create`
- `open`
- `delete`
- `deleteFiles`
- `rename`
- `copy`
- `multipart upload`
- `presigned url`
- `sts token`
- not found 错误映射
- auth failed 错误映射
- endpoint/region/path-style 行为

测试结构建议是：

```text
ObjectFileSystemContractTest
  -> S3
  -> OSS
  -> COS
  -> OBS
```

单元测试用 mock client 覆盖参数转换、请求构建和错误映射；环境测试继续使用现有 `*FileSystemEnvTest`，但对象存储环境测试必须明确标识当前 provider 是否走 native SDK。

## 与参数统一改造的边界

当前 `StorageProperties` / `FileSystemProvider` 参数统一改造先完成以下目标：

- 统一参数入口。
- 统一 provider 选择。
- 统一 `FileSystemProperties` bind / validate。
- 统一脱敏、基础参数检查、基础参数构建。
- 让 provider 能够返回自己的 `FileSystem`。

Native SDK 切换作为后续阶段单独推进。

不建议在同一次改动中同时完成：

- 参数 SPI 重构。
- `S3FileSystem` 通用化。
- OSS/COS/OBS native SDK 全量替换。

原因是这三件事分别影响参数兼容、文件系统语义和底层 SDK 行为。如果混在一个 PR 中，review 和回归风险都会很高。

## 结论

后续 native SDK 改造的核心不是简单把 provider 里 `new S3FileSystem(...)` 改名，而是：

```text
先抽通用 ObjectFileSystem，消除 S3FileSystem 对 S3ObjStorage 的强绑定；
再让 OSS/COS/OBS 拥有自己的 FileSystem；
最后把 OSS/COS/OBS ObjStorage 从继承 S3ObjStorage 改为直接使用厂商 Native SDK。
```

这条路径可以保证：

- 参数统一先稳定落地。
- FileSystem/ObjStorage 边界清晰。
- S3-compatible 路径和厂商 native SDK 路径可独立回归。
- 最终行为上做到同一套参数入口、同一套 filesystem 语义、不同 provider 使用各自 SDK。
