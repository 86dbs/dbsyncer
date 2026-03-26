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
| 连接器        | 数据源 | 目标源 | 支持版本(包含以下)    |
|------------|---|---|---------------|
| MySQL      | ✔ |  ✔ | 5.7.19以上      |
| Oracle     | ✔ |  ✔ | 10g-19c       |
| SqlServer  | ✔ |  ✔ | 2008以上        |
| PostgreSQL | ✔ |  ✔ | 9.5.25以上      |
| Sqlite     | ✔ |  ✔ | 2以上           |
| ES         | ✔ |  ✔ | 6.0.0-8.15.3  |
| Kafka      | ✔ |  ✔ | 2.10-0.9.0.0以上 |
| File       | ✔ |  ✔ | *.txt, *.unl  |
| Http       | ✔ |  ✔ |   |
| SQL        | ✔ |  | 支持以上关系型数据库    |
| 后期计划       | Redis | |               |

## ✨预览
![仪表盘](https://foruda.gitee.com/images/1769953561917318680/775e6928_376718.png "仪表盘")

![监控](https://foruda.gitee.com/images/1769953915949399025/fc16d35b_376718.png "监控")

## 📚[使用手册 Wiki](https://gitee.com/ghi/dbsyncer/wikis "https://gitee.com/ghi/dbsyncer/wikis")

## 📦安装配置
- dbsyncer-x.x.x.zip [社区版安装教程](https://gitee.com/ghi/dbsyncer/wikis/%E6%93%8D%E4%BD%9C%E6%89%8B%E5%86%8C/%E7%A4%BE%E5%8C%BA%E7%89%88%E5%AE%89%E8%A3%85)
- dbsyncer-enterprise-x.x.x.zip [专业版安装教程](https://gitee.com/ghi/dbsyncer/wikis/%E6%93%8D%E4%BD%9C%E6%89%8B%E5%86%8C/%E4%B8%93%E4%B8%9A%E7%89%88%E5%AE%89%E8%A3%85)

#### 方式一 下载安装包
1. 安装[JDK 1.8](https://www.oracle.com/java/technologies/jdk8-downloads.html)（省略详细）
2. 下载安装包[dbsyncer-x.x.x.zip](https://gitee.com/ghi/dbsyncer/releases)（也可手动编译）
3. 解压安装包，Window执行bin/startup.bat，Linux执行bin/startup.sh
4. 打开浏览器访问：http://127.0.0.1:18686
5. 账号和密码：admin/admin

#### 方式二 🐳 docker
* 阿里云镜像
```shell
# 社区版
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer:latest
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer:2.0.8
# 专业版
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer-enterprise:latest
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer-enterprise:2.0.8
```

* 运行命令
```shell
docker run -d \
  --name=dbsyncer \
  --restart=unless-stopped \
  -p 18686:18686 \
  -e TZ="Asia/Shanghai" \
  -m 5g \
  --memory-swap=5g \
  -v /opt/dbsyncer/data:/app/dbsyncer/data \
  -v /opt/dbsyncer/logs:/app/dbsyncer/logs \
  -v /opt/dbsyncer/plugins:/app/dbsyncer/plugins \
  --log-driver json-file \
  --log-opt max-size=100m \
  --log-opt max-file=7 \
  scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer:latest

# 本地日志
ls -la /opt/dbsyncer/logs

# 容器日志
docker logs --tail 20 dbsyncer
# 容器实时日志（Ctrl+C退出）
docker logs -f dbsyncer
# 进入容器内部
docker exec -it dbsyncer /bin/bash
# 查看容器日志
ls -la /app/dbsyncer/logs

# 停止容器
docker stop dbsyncer
# 启动容器
docker start dbsyncer
# 重启容器
docker restart dbsyncer
# 删除容器（需先停止）
docker rm dbsyncer
```

## ⚙️手动编译
> 先确保环境已安装JDK和Maven
```bash
$ git clone https://gitee.com/ghi/dbsyncer.git
$ cd dbsyncer
$ chmod u+x build.sh
$ ./build.sh
```
## 🏆[性能测试](https://gitee.com/ghi/dbsyncer/wikis/%E5%BF%AB%E9%80%9F%E4%BA%86%E8%A7%A3/%E6%80%A7%E8%83%BD%E6%B5%8B%E8%AF%95)
#### 全量同步

| 系统 | 机器配置 |  数据量 |  耗时 |
|---|---|---|---|
| Mac | Apple M3 Pro 12核心 内存18GB | 1亿条 | 31分50秒 |
| Linux | Intel(R) Xeon(R) CPU E5-2696 v3B 8核心 内存48GB | 1亿条 | 37分52秒 |
| Windows | AMD Ryzen 7 5800x 8核心 12GB | 1亿条 | 57分43秒 |

#### 增量同步
| 系统 | 机器配置 |  分配内存 |  TPS | 峰值 |
|---|---|---|---|---|
| Mac | Apple M3 Pro 12核心 内存18GB | 4GB | 8112/秒 | 11000/秒 |
| Linux | Intel(R) Xeon(R) CPU E5-2696 v3B 8核心 内存48GB | 4GB | 8000/秒 | 10000/秒 |
| Windows | AMD Ryzen 7 5800x 8核心 12GB | 4GB | 7553/秒 | 9000/秒 |

<img src="https://foruda.gitee.com/images/1722860668272963387/7110f00f_376718.png" />

<img src="https://foruda.gitee.com/images/1732952268144233045/b607609e_376718.png" />

[专业版介绍](https://gitee.com/ghi/dbsyncer/wikis/DBSyncer%E4%B8%93%E4%B8%9A%E7%89%88)

## 🐞[常见问题](https://gitee.com/ghi/dbsyncer/wikis/%E5%92%A8%E8%AF%A2%E9%97%AE%E9%A2%98/%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98) 
* MySQL无法连接。默认使用的驱动版本为8.0.21，如果为mysql5.x需要手动替换驱动 [mysql-connector-java-5.1.40.jar](https://gitee.com/ghi/dbsyncer/attach_files) 
* SQLServer无法连接。案例：[驱动程序无法通过使用安全套接字层(SSL)加密与 SQL Server 建立安全连接。错误:“The server selected protocol version TLS10 is not accepted by client preferences [TLS12]”](https://gitee.com/ghi/dbsyncer/issues/I4PL46?from=project-issue) 
* 同步数据乱码。案例：[mysql8表导入sqlserver2008R2后，sqlserver表nvarchar字段内容为乱码](https://gitee.com/ghi/dbsyncer/issues/I4JXY0) 
* [如何开启远程debug模式？](https://gitee.com/ghi/dbsyncer/issues/I63F6R)  

## 🤝贡献支持
* DBS团队目标：**坚持开源，让每一个用户都能轻松完成数据同步！** 
* QQ群讨论: **875519623** 
* 欢迎大家提需求和建议[【新建issuses】](https://gitee.com/ghi/dbsyncer/issues/new?issue%5Bassignee_id%5D=0&issue%5Bmilestone_id%5D=0)!（详细描述你的原始需求，我们会帮你提供一些方案，节约大家的成本）
1) **内推项目** 如您觉得项目不错，可推荐到公司，建立长期稳定的商业合作，提供更专业的技术服务。（入群联系群主）
2) **参与开发** 项目成员有不同专业的大佬，相信一定能找到您比较感兴趣的方向，欢迎加入团队！（入群联系群主）
3) 开源不易，感谢粉丝朋友们的支持！[【捐赠者名单】](https://gitee.com/ghi/dbsyncer/issues/I4HL3C) 

<p>
<img src="https://foruda.gitee.com/images/1736348493470674811/5761f9e8_13999669.png "二维码_01.png"" title="微信扫码" width="258px" height="283.8px" /><img src="https://foruda.gitee.com/images/1736348568894927308/12fe6e2d_13999669.png "二维码_02.png"" title="微信扫码" width="258px" height="283.8px" /><img src="https://foruda.gitee.com/images/1738991478241390049/8f566d69_13999669.png "哔站二维码.png"" title="微信扫码" width="258px" height="283.8px" /><img src="https://foruda.gitee.com/images/1710433659737550167/452d76c9_376718.png" title="DBSyncer款项用于研发推广" width="223.2px" height="286.4px" />
<p>
