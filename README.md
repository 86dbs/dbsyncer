<div>
    <h3>介绍</h3>
    <p>DBSyncer是一款开源的数据同步中间件，提供Mysql、Oracle、SqlServer、Elasticsearch(ES)、SQL等同步场景。支持上传插件自定义同步转换业务，提供监控全量和增量数据统计图、应用性能预警等。</p>
    <p>特点</p>
    <ol>
        <li>组合驱动，自定义库同步到库组合，关系型数据库与非关系型之间组合，任意搭配表同步映射关系</li>
        <li>实时监控，驱动全量或增量实时同步运行状态、结果、同步日志和系统日志</li>
        <li>开发插件，自定义转化同步逻辑</li>
    </ol>
</div>

<div>
    <h3>应用场景</h3>
    <table>
        <tbody>
            <tr>
                <td colspan="2" rowspan="2">Every point is a DataBase</td>
                <td colspan="6" align="center">目标源</td>
            </tr>
            <tr>
                <td>Mysql</td>
                <td>Oracle</td>
                <td>SqlServer</td>
                <td>ES</td>
                <td>SQL（Mysql/Oracle/SqlServer）</td>
            </tr>
            <tr>
                <td rowspan="5">数据源</td>
                <td>Mysql</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>Oracle</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>SqlServer</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>ES</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>SQL（Mysql/Oracle/SqlServer）</td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
                <td></td>
            </tr>
            <tr>
                <td rowspan="4">版本支持</td>
                <td>Mysql</td>
                <td colspan="7">5.7.19以上</td>
            </tr>
            <tr>
                <td>Oracle</td>
                <td colspan="7">10g以上（Oracle-9i未测试）</td>
            </tr>
            <tr>
                <td>SqlServer</td>
                <td colspan="7">2008以上</td>
            </tr>
            <tr>
                <td>ES</td>
                <td colspan="7">6.X以上</td>
            </tr>
            <tr>
                <td>最近计划</td>
                <td colspan="7">kafka、postgrep</td>
            </tr>
        </tbody>
    </table>
    <h3>安装说明</h3>
    <p>准备</p>
    <ol>
        <li><a target="_blank" href="https://gitee.com/ghi/dbsyncer/releases">DBSyncer-1.0.0-Alpha.zip</a>（安装包）</li>
        <li><a target="_blank" href="https://www.oracle.com/java/technologies/jdk8-downloads.html">JRE 1.8 +</a></li>
    </ol>
    <p>步骤</p>
    <ol>
        <li>安装JRE1.8版本以上（省略详细）</li>
        <li>下载安装包DBSyncer-X.X.X-RELEASE.zip</li>
        <li>解压，启动脚本bin/startup.bat（Windows）/ bin/startup.sh（Linux）</li>
        <li>打开浏览器，输入访问地址：http://127.0.0.1:18686</li>
        <li>默认账号和密码：admin/admin</li>
    </ol>
    <h3>增量同步配置（源库）</h3>
    <table>
        <tbody>
            <tr>
                <td><b>类型</b></td>
                <td><b>配置</b></td>
                <td><b>原理</b></td>
            </tr>
            <tr>
                <td>
                    <h5>Mysql</h5>
                </td>
                <td>
                    <p>修改my.ini文件配置:</p>
                    <p># 服务唯一ID</p>
                    <p>server_id=1</p>
                    <p>log-bin=mysql_bin</p>
                    <p>binlog-format=ROW</p>
                    <p>max_binlog_cache_size = 256M</p>
                    <p>max_binlog_size = 512M</p>
                    <p>expire_logs_days = 7</p>
                    <p># 监听同步的库, 多个库使用英文逗号“,”拼接</p>
                    <p>replicate-do-db=test</p>
                </td>
                <td>Dump Binlog二进制日志。Master同步Slave, 创建IO线程读取数据，写入relaylog，基于消息订阅捕获增量数据。</td>
            </tr>
            <tr>
                <td>
                    <h5>Oracle</h5>
                </td>
                <td>
                    <p>授予账号监听权限:</p>
                    <p>grant change notification to AE86</p>
                    <p>要求目标源表必须定义一个长度为18的varchar字段，用于接收rowid值，来实现增删改操作</p>
                </td>
                <td>CDN注册订阅。监听增删改事件，得到rowid，根据rowid执行SQL查询，得到变化数据</td>
            </tr>
            <tr>
                <td>
                    <h5>SqlServer</h5>
                </td>
                <td>
                    <p>开启CDC:</p>
                    <p>要求2008版本以上, 启动代理服务（Agent服务）, 连接账号具有 sysadmin 固定服务器角色或 db_owner 固定数据库角色的成员身份。对于所有其他用户，具有源表SELECT 权限；如果已定义捕获实例的访问控制角色，则还要求具有该数据库角色的成员身份。</p>
                </td>
                <td>SQL Server 2008提供了内建的方法变更数据捕获（Change Data Capture 即CDC）以实现异步跟踪用户表的数据修改</td>
            </tr>
            <tr>
                <td>
                    <h5>ES</h5>
                </td>
                <td>
                    <p>账号具有访问权限</p>
                </td>
                <td>增量同步只支持定时读取</td>
            </tr>
        </tbody>
    </table>
</div>

<div>
    <h3>预览</h3>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0903/003755_01016fc1_376718.png" />
    </p>
    <p>驱动表关系配置</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0903/004031_a571f6b5_376718.png" />
    </p>
    <p>驱动表字段关系配置</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0903/004106_26399534_376718.png" />
    </p>
    <p>定时配置</p>
    <p align="center">
        <p>假设源表数据格式</p>
        <img src="https://images.gitee.com/uploads/images/2021/0903/004406_68ef9bb4_376718.png" />
        <img src="https://images.gitee.com/uploads/images/2021/0903/004807_07cdf2b7_376718.png" />
    </p>
    <p>监控</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0728/000645_35a544b3_376718.png" />
    </p>
    <p>上传插件</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0806/232643_9b1f3f64_376718.png" />
    </p>
</div>

<div>
<h3>使用说明</h3>
    <ol>
        <li>创建一个连接器。选择数据源类型，比如：Mysql，填写配置，保存</li>
        <li>添加驱动。配置数据源和目标源（数据源：数据的发送端，目标源：数据接收端），保存</li>
        <li>模式支持全量同步（默认：全量复制）和增量同步（监听变化的数据）</li>
        <li>基本配置里面，添加映射关系。添加数据源表同步到目标源表关系</li>
        <li>单击映射关系，进入表字段详细页面，默认匹配相识字段，识别主键（主键用于增量同步，更新和删除使用），保存</li>
        <li>高级配置省略</li>
        <li>点击驱动右上角齿轮按钮，启动或删除</li>
        <li>驱动面板下方显示同步的详细，如果有异常日志，点击日志可跳转至监控菜单查看详细</li>
    </ol>
    <h3>开发框架版本</h3>
    <ol>
        <li>JDK - 1.8.0_40 (推荐版本及以上)</li>
        <li>Maven - 3.3.9</li>
        <li><a target="_blank" href="https://docs.spring.io/spring-boot/docs/2.2.0.RELEASE/reference/html">Spring Boot - 2.2.0.RELEASE</a></li>
        <li><a target="_blank" href="http://getbootstrap.com">Bootstrap - 3.3.4</a></li>
    </ol>
</div>

<div>
    <h3>了解更多</h3>
    博客地址：<a target="_blank" href="https://my.oschina.net/dbsyncer">https://my.oschina.net/dbsyncer</a><br />
    QQ群: 875519623或点击右侧按钮<a target="_blank" href="//shang.qq.com/wpa/qunwpa?idkey=fce8d51b264130bac5890674e7db99f82f7f8af3f790d49fcf21eaafc8775f2a"><img border="0" src="//pub.idqqimg.com/wpa/images/group.png" alt="数据同步dbsyncer" title="数据同步dbsyncer" />
</div>