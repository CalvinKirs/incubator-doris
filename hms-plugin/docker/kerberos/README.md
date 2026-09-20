# Kerberos HMS 测试环境

独立 MIT KDC + Hive 1.1 SASL HMS，复用现有 Simple HDFS 和 4.1.3 BE。
不修改主机 `/etc/krb5.conf`。测试 principal/keytab 在启动时随机生成，保存在传入的运行目录中。
运行目录必须是新目录，建议放在已忽略的 `hms-plugin/target/` 下；保留容器和元数据方便排查。

先按上级 Dockerfile 构建 `gq-hms-auth:1.1.0`，并启动测试 HDFS。然后在 worktree 根目录执行：

```bash
bash hms-plugin/docker/kerberos/start.sh \
  "$PWD/hms-plugin/target/kerberos/run-$(date +%s)" hdfs://127.0.0.1:34097
source <启动脚本输出的运行目录>/fixture.env
```

使用 host network，KDC/HMS 分配随机空闲端口。KDC 镜像构建接受当前 shell 的代理配置。
HMS 启动日志应包含 `Starting DB backed MetaStore Server in Secure Mode`，随后运行测试验证 SASL。

Hive 1.1 Secure HMS 会代理客户端访问文件系统。仅在测试 HDFS 的 `core-site.xml` 加以下规则，
首次添加名称映射后重启该测试 HDFS；HDFS 认证仍为 Simple：

```xml
<property>
  <name>hadoop.security.auth_to_local</name>
  <value>RULE:[2:$1@$0](hive@HMS.AUTH.TEST)s/.*/hive/ RULE:[1:$1@$0](.*@HMS.AUTH.TEST)s/@.*// DEFAULT</value>
</property>
<property>
  <name>hadoop.proxyuser.hive.hosts</name><value>127.0.0.1</value>
</property>
<property>
  <name>hadoop.proxyuser.hive.users</name>
  <value>doris-hms,doris-renew</value>
</property>
```

这些规则只将测试 realm 的指定 principal 映射为本地名称。HMS 的 `ipc.client.fallback-to-simple-auth-allowed=true`
也是为了连接这个 Simple HDFS；不代表生产环境应打开该选项。

Java 测试通过仓库入口运行，必须显式指定类，防止 Maven 匹配不到测试：

```bash
MAVEN_OPTS='-Xms512m -Xmx5g -XX:MaxMetaspaceSize=1g' \
MAVEN_ARGS='-Dskip.clean=true -Dmaven.main.skip=true' \
CUSTOM_MVN="$PWD/hms-plugin/maven.sh" EXTRA_FE_MODULES='hms=../hms-plugin' \
./run-fe-ut.sh --run org.apache.doris.plugin.hms.HmsKerberosIntegrationTest
```

上面的 `maven.main.skip` 仅适用于生产代码已经通过 `build.sh --fe` 构建的 worktree。
用例会停止并重启自己的 HMS 容器，请勿与使用该 HMS 的 SQL 用例同时运行。
短票据用例持续约一分钟，检查同一 UGI 中 TGT 的实际开始/结束时间变化。
测试类设置 JVM 的 `java.security.krb5.conf`，应在独立的这次测试调用中运行。

SQL 用例是独立文件 `regression/suites/hms/hms_kerberos_authorization.groovy`。
测试 FE 的 `JAVA_OPTS_FOR_JDK_17` 加上 `-Djava.security.krb5.conf=<运行目录>/krb5.conf` 后重启。
FE 必须能读取该运行目录内的 keytab；BE 保持原配置。回归配置增加：

```groovy
hmsKrbUri = "thrift://127.0.0.1:<HMS端口>"
hmsKrbDir = "<运行目录绝对路径>"
// 继续使用已有 hmsAuthPassword、hmsAuthHdfsUri 和当前测试 FE JDBC 配置。
```

```bash
JAVA_HOME=/mnt/disk1/gq/jdk17 \
JAVA_OPTS="-Xmx2g --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djava.security.krb5.conf=$HMS_KRB_TEST_DIR/krb5.conf" \
./run-regression-test.sh --conf "$PWD/hms-plugin/target/cluster/regression-conf.groovy" \
  --run -d hms -s hms_kerberos_authorization -parallel 1
```

`.out` 只能用同一入口加 `-forceGenOut` 生成，然后去掉该参数执行比较。
SQL 用例验证真实 HDFS 读写、用户大小写、角色、撤权、服务用户不能替代查询用户权限、
四类错误认证配置拒绝和短票据到期后继续查询。错误 Catalog 保持元数据凭据正确，仅改变插件凭据。
Java 用例进一步检查票据时间及 SASL 连接池重连。
