## 介绍
![logo](https://gitee.com/ghi/dbsyncer/raw/master/dbsyncer-web/src/main/resources/static/img/logo.png)

[DBSyncer](https://gitee.com/ghi/dbsyncer)（英[dbsɪŋkɜː(r)]，美[dbsɪŋkɜː(r) 简称dbs）是一款开源的数据同步中间件，提供MySQL、Oracle、SqlServer、PostgreSQL、Elasticsearch(ES)、Kafka、File、SQL等同步场景。支持上传插件自定义同步转换业务，提供监控全量和增量数据统计图、应用性能预警等。

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
| 连接器        | 数据源 | 目标源 | 支持版本(包含以下)            |
|------------|---|---|-----------------------|
| MySQL      | ✔ |  ✔ | 5.7.19以上              |
| Oracle     | ✔ |  ✔ | 11g-19c |
| SqlServer  | ✔ |  ✔ | 2008以上                |
| PostgreSQL | ✔ |  ✔ | 9.5.25以上              |
| ES         | ✔ |  ✔ | 6.0以上                 |
| Kafka      | 开发中 |  ✔ | 2.10-0.9.0.0以上        |
| File       | ✔ |  ✔ | *.txt, *.unl          |
| SQL        | ✔ |  | 支持以上关系型数据库            |
| Sqlite     | ✔ |   |                       |
| 后期计划       | Redis | |                       |

## 📚使用手册
### [Wiki](https://gitee.com/ghi/dbsyncer/wikis "https://gitee.com/ghi/dbsyncer/wikis")

## ✨预览
### 驱动管理
![连接器和驱动](https://images.gitee.com/uploads/images/2021/0903/003755_01016fc1_376718.png "驱动管理.png")

### 监控
![监控](https://foruda.gitee.com/images/1694424923138969858/111e55e0_376718.png "监控")

## 📦安装配置
#### 步骤
1. 安装[JDK 1.8](https://www.oracle.com/java/technologies/jdk8-downloads.html)（省略详细）
2. 下载安装包[DBSyncer-1.0.0-RC.zip](https://gitee.com/ghi/dbsyncer/releases)（也可手动编译）
3. 解压安装包，Window执行bin/startup.bat，Linux执行bin/startup.sh
4. 打开浏览器访问：http://127.0.0.1:18686
5. 账号和密码：admin/admin

## ⚙️手动编译
> 先确保环境已安装JDK和Maven
```bash
$ git clone https://gitee.com/ghi/dbsyncer.git
$ cd dbsyncer
$ chmod u+x build.sh
$ ./build.sh
```
## 🏆性能测试
|  类型 | 耗时 | 数据量 |  机器配置 |
|---|---|---|---|
|  MySQL全量同步 | 202s  |  1000w |  MacBook Pro 2.4 GHz 四核Intel Core i5 16 GB 2133 MHz LPDDR3 |

<img src="https://foruda.gitee.com/images/1660034515191434708/屏幕截图.png" width="200" height="200" />

## docker支持
docker pull crazylife/dbsyncer-web:2.0.1

## 🐞常见问题
* MySQL无法连接。默认使用的驱动版本为8.0.21，如果为mysql5.x需要手动替换驱动 [mysql-connector-java-5.1.40.jar](https://gitee.com/ghi/dbsyncer/attach_files) 
* SQLServer无法连接。案例：[驱动程序无法通过使用安全套接字层(SSL)加密与 SQL Server 建立安全连接。错误:“The server selected protocol version TLS10 is not accepted by client preferences [TLS12]”](https://gitee.com/ghi/dbsyncer/issues/I4PL46?from=project-issue) 
* 同步数据乱码。案例：[mysql8表导入sqlserver2008R2后，sqlserver表nvarchar字段内容为乱码](https://gitee.com/ghi/dbsyncer/issues/I4JXY0) 
* [如何开启远程debug模式？](https://gitee.com/ghi/dbsyncer/issues/I63F6R)  

## 🤝贡献支持
*  **DBS团队目标**：开源共享，稳定盈利，推进项目可持续。（开源不易，非常感谢一直支持我们的用户朋友们！！）
* 需要专业技术指导，请扫码加[**会员粉丝服务群**](https://gitee.com/ghi/dbsyncer/wikis/%E4%BC%9A%E5%91%98%E7%B2%89%E4%B8%9D%E6%9C%8D%E5%8A%A1%E7%BE%A4?sort_id=9604090)★★★★★（福利有限，加满即止）

<img src="https://foruda.gitee.com/images/1710433533728877903/c79fd442_376718.png" title="DBSyncer款项用于研发推广" height="400" />

* QQ群讨论: **[875519623](//shang.qq.com/wpa/qunwpa?idkey=fce8d51b264130bac5890674e7db99f82f7f8af3f790d49fcf21eaafc8775f2a)**
* 欢迎大家提需求和建议[【新建issuses】](https://gitee.com/ghi/dbsyncer/issues/new?issue%5Bassignee_id%5D=0&issue%5Bmilestone_id%5D=0)!（详细描述你的原始需求，我们会帮你提供一些方案，节约大家的成本）
1.  **内推项目** 如您觉得项目不错，可推荐到公司，建立长期稳定的商业合作，提供更专业的技术服务。（入群联系群主）
2.  **参与开发** 项目成员有不同专业的大佬，相信一定能找到您比较感兴趣的方向，欢迎加入团队！（入群联系群主）
3.  **扫赞赏码**[【捐赠者名单】](https://gitee.com/ghi/dbsyncer/issues/I4HL3C) 

<img src="https://foruda.gitee.com/images/1710433659737550167/452d76c9_376718.png" title="DBSyncer款项用于研发推广" height="400" />