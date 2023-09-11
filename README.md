## 介绍
![logo](dbsyncer-web/src/main/resources/static/img/logo.png)

DBSyncer是一款开源的数据同步中间件，提供MySQL、Oracle、SqlServer、PostgreSQL、Elasticsearch(ES)、Kafka、File、SQL等同步场景。支持上传插件自定义同步转换业务，提供监控全量和增量数据统计图、应用性能预警等。

> 特点
* 组合驱动，自定义库同步到库组合，关系型数据库与非关系型之间组合，任意搭配表同步映射关系
* 实时监控，驱动全量或增量实时同步运行状态、结果、同步日志和系统日志
* 开发插件，自定义转化同步逻辑

> 项目地址

* [Gitee](https://gitee.com/ghi/dbsyncer "https://gitee.com/ghi/dbsyncer")  
* [GitHub](https://github.com/86dbs/dbsyncer "https://github.com/86dbs/dbsyncer")

[![star](https://gitee.com/ghi/dbsyncer/badge/star.svg?theme=dark)](https://gitee.com/ghi/dbsyncer/stargazers)
[![fork](https://gitee.com/ghi/dbsyncer/badge/fork.svg?theme=dark)](https://gitee.com/ghi/dbsyncer/members)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://gitee.com/ghi/dbsyncer/blob/master/LICENSE)

## 🌈应用场景
| 连接器 | 数据源 | 目标源 | 支持版本(包含以下) |
|---|---|---|---|
| MySQL | ✔ |  ✔ | 5.7.19以上 |
| Oracle | ✔ |  ✔ | 10gR2 -11g |
| SqlServer | ✔ |  ✔ | 2008以上 |
| PostgreSQL | ✔ |  ✔ | 9.5.25以上 |
| ES | ✔ |  ✔ | 6.0以上 |
| Kafka | 开发中 |  ✔ | 2.10-0.9.0.0以上 |
| File | ✔ |  ✔ | *.txt, *.unl |
| SQL | ✔ |  | 支持以上关系型数据库 |
| 后期计划 | Redis | | |

## 📦安装配置
#### 步骤
1. 安装[JDK 1.8](https://www.oracle.com/java/technologies/jdk8-downloads.html)（省略详细）
2. 下载安装包[DBSyncer-1.x.x.zip](https://gitee.com/ghi/dbsyncer/releases)（也可手动编译）
3. 解压安装包，Window执行bin/startup.bat，Linux执行bin/startup.sh
4. 打开浏览器访问：http://127.0.0.1:18686
5. 账号和密码：admin/admin

#### 增量同步配置（源库）

##### MySQL
* Dump Binlog二进制日志。Master同步Slave, 创建IO线程读取数据，写入relaylog，基于消息订阅捕获增量数据。
> 修改my.ini文件，重启服务
```bash
#服务唯一ID
server_id=1
log-bin=mysql_bin
binlog-format=ROW
max_binlog_cache_size = 256M
max_binlog_size = 512M
#监听同步的库, 多个库使用英文逗号“,”拼接
replicate-do-db=test
```
> 准备账号用于数据同步（这里我为test数据库创建了ae86账号，并授权）
``` sql
CREATE USER 'ae86'@'%' IDENTIFIED WITH mysql_native_password BY '123';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ae86'@'%';
GRANT SELECT ON test.* TO 'ae86'@'%';
flush privileges;
```

##### Oracle
* CDN注册订阅。监听增删改事件，得到rowid，根据rowid执行SQL查询，得到变化数据。
> 1、授予账号监听权限, 同时要求目标源表必须定义一个长度为18的varchar字段，通过接收rowid值实现增删改操作。
```roomsql
grant change notification to 你的账号
```
> 2、账号必须是监听表的OWNER
```roomsql
SELECT OBJECT_ID, OBJECT_NAME, OWNER FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'TABLE' AND OWNER='你的账号';
```
![DCN账号](https://images.gitee.com/uploads/images/2022/0717/001127_fb4049b6_376718.png "DCN账号.png")

##### SqlServer
* SQL Server 2008提供了内建的方法变更数据捕获（Change Data Capture 即CDC）以实现异步跟踪用户表的数据修改。
> 要求2008版本以上, 启动代理服务（Agent服务）, 连接账号具有 sysadmin 固定服务器角色或 db_owner 固定数据库角色的成员身份。对于所有其他用户，具有源表SELECT 权限；如果已定义捕获实例的访问控制角色，则还要求具有该数据库角色的成员身份。

> 1. 启动 **代理** 和 **数据库** 服务

![输入图片说明](https://foruda.gitee.com/images/1669649054209443088/5ae57c11_376718.png "屏幕截图")

> 2. 准备测试账号，test

![输入图片说明](https://foruda.gitee.com/images/1669648409722723985/2c9cc49b_376718.png "屏幕截图")

> 3. 分配sysadmin角色

![输入图片说明](https://foruda.gitee.com/images/1669648470726217924/0ea30c3c_376718.png "屏幕截图")

> 4. 分配指定数据库权限

![输入图片说明](https://foruda.gitee.com/images/1669648797643372138/018a1692_376718.png "屏幕截图")

##### PostgreSQL
* 通过复制流技术监听增量事件，基于内置插件pgoutput、test_decoding实现解析wal日志
> 1、修改postgresql.conf文件，重启服务
``` shell
wal_level=logical
```
> 2、授予账号权限LOGIN和REPLICATION

##### File
* 监听文件修改时间得到变化文件，通过文件偏移量读取最新数据
> [监听文件实现方案](https://gitee.com/ghi/dbsyncer/issues/I55EP5)

##### ES
* 定时获取增量数据。
> 账号具有访问权限。

##### 日志
> 建议MySQL、SqlServer、PostgreSQL都使用日志

![日志](https://images.gitee.com/uploads/images/2021/0906/181036_1f9a9e78_376718.png "日志.png")

##### 定时
> 假设源表数据格式

![表数据格式](https://images.gitee.com/uploads/images/2021/0903/004406_68ef9bb4_376718.png "表数据格式.png")
![定时和过滤条件](https://images.gitee.com/uploads/images/2021/0903/004807_07cdf2b7_376718.png "定时和过滤条件.png")

## ✨预览
### 驱动管理
![连接器和驱动](https://images.gitee.com/uploads/images/2021/0903/003755_01016fc1_376718.png "驱动管理.png")

### 驱动详情
![驱动详情](https://images.gitee.com/uploads/images/2021/0903/004031_a571f6b5_376718.png "驱动详情.png")

### 驱动表字段关系配置
![驱动表字段关系配置](https://images.gitee.com/uploads/images/2021/0903/004106_26399534_376718.png "驱动表字段关系配置.png")

### 监控
![监控](https://images.gitee.com/uploads/images/2021/0728/000645_35a544b3_376718.png "监控.png")

### 上传插件
![上传插件](https://images.gitee.com/uploads/images/2021/0806/232643_9b1f3f64_376718.png "上传插件.png")

## 🎨设计
#### 架构图
<img src="http://assets.processon.com/chart_image/5d63b0bce4b0ac2b61877037.png" />

## 🔗开发依赖
* [JDK - 1.8.0_202](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html)
* [Maven - 3.3.9](https://dlcdn.apache.org/maven/maven-3/)（推荐版本以上）

## ⚙️手动编译
> 先确保环境已安装JDK和Maven
```bash
$ git clone https://gitee.com/ghi/dbsyncer.git
$ cd dbsyncer
$ chmod u+x build.sh
$ ./build.sh
```

## 🐞常见问题
* MySQL无法连接。默认使用的驱动版本为8.0.21，如果为mysql5.x需要手动替换驱动 [mysql-connector-java-5.1.40.jar](https://gitee.com/ghi/dbsyncer/attach_files) 
* SQLServer无法连接。案例：[驱动程序无法通过使用安全套接字层(SSL)加密与 SQL Server 建立安全连接。错误:“The server selected protocol version TLS10 is not accepted by client preferences [TLS12]”](https://gitee.com/ghi/dbsyncer/issues/I4PL46?from=project-issue) 
* 同步数据乱码。案例：[mysql8表导入sqlserver2008R2后，sqlserver表nvarchar字段内容为乱码](https://gitee.com/ghi/dbsyncer/issues/I4JXY0) 
* [如何开启远程debug模式？](https://gitee.com/ghi/dbsyncer/issues/I63F6R)  

## 🏆性能测试
|  类型 | 耗时 | 数据量 |  机器配置 |
|---|---|---|---|
|  MySQL全量同步 | 202s  |  1000w |  MacBook Pro 2.4 GHz 四核Intel Core i5 16 GB 2133 MHz LPDDR3 |

<img src="https://foruda.gitee.com/images/1660034515191434708/屏幕截图.png" width="200" height="200" />

## 💕了解更多
* [博客地址](https://my.oschina.net/dbsyncer "https://my.oschina.net/dbsyncer")（小提示：现在需要先登录，才能查看完整的教程信息，包含截图等😂）
* [使用文档](https://gitee.com/ghi/dbsyncer/wikis "https://gitee.com/ghi/dbsyncer/wikis")（正在完善中..）
* QQ群: 875519623或点击右侧按钮<a target="_blank" href="//shang.qq.com/wpa/qunwpa?idkey=fce8d51b264130bac5890674e7db99f82f7f8af3f790d49fcf21eaafc8775f2a"><img border="0" src="//pub.idqqimg.com/wpa/images/group.png" alt="数据同步dbsyncer" title="数据同步dbsyncer" /></a>

## 🤝贡献支持
* 如有比较着急的需求或建议（想支持某版本的中间件，最好能描述清楚你的原始需求，作者会帮你提供一些建议方案），欢迎大家[【新建issuses】](https://gitee.com/ghi/dbsyncer/issues/new?issue%5Bassignee_id%5D=0&issue%5Bmilestone_id%5D=0)!
* DBSyncer研发团队目标：**取之社区，用于社区**。为了能让项目得到可持续发展，我们期望获得更多的支持者! 
1.  **内推项目** 如您觉得项目不错，可推荐到公司，建立长期稳定的商业合作，提供更专业的技术服务。（入群联系群主）
2.  **参与开发** 项目成员来自于不同公司，汇聚了不同专业的大佬，相信一定能找到您比较感兴趣的方向，欢迎加入团队！（入群联系群主）
3.  **扫赞赏码**[【捐赠者名单】](https://gitee.com/ghi/dbsyncer/issues/I4HL3C) 款项主要用于项目研发和推广，会定期通过线上活动，与大家一起讨论问题，随机回馈粉丝们一些礼物。
<img src="https://images.gitee.com/uploads/images/2021/1110/001937_717dfb9d_376718.png" title="DBSyncer款项用于研发推广" width="400" height="400" />