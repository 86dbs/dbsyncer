<div>
    <h3>介绍</h3>
    <p>DBSyncer是一款开源的数据同步软件，提供Mysql、Oracle、SqlServer、SQL结果集等场景，支持自定义同步转换业务。</p>
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
                <td colspan="2" rowspan="3">Every point is a DataBase</td>
                <td colspan="6" align="center">目标源</td>
            </tr>
            <tr>
                <td colspan="3" align="center">全量</td>
                <td colspan="3" align="center">增量</td>
            </tr>
            <tr>
                <td>Mysql</td>
                <td>Oracle</td>
                <td>SQLServer</td>
                <td>Mysql</td>
                <td>Oracle</td>
                <td>SQLServer</td>
            </tr>
            <tr>
                <td rowspan="6">数据源</td>
                <td>Mysql</td>
                <td>√</td>
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
                <td>√</td>
            </tr>
            <tr>
                <td>SQLServer</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>DQLMysql</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>DQLOracle</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td>DQLSQLServer</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
                <td>√</td>
            </tr>
            <tr>
                <td rowspan="3">版本支持</td>
                <td>Mysql</td>
                <td colspan="7">5.7.19以上</td>
            </tr>
            <tr>
                <td>Oracle</td>
                <td colspan="7">10g以上（Oracle-9i未测试）</td>
            </tr>
            <tr>
                <td>SQLServer</td>
                <td colspan="7">2008以上</td>
            </tr>
            <tr>
                <td>最近计划</td>
                <td colspan="7">kafka、es、postgrep</td>
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
        <li>解压，进入目录bin，启动脚本startup.bat（Windows）/startup.sh（Linux）</li>
        <li>打开浏览器，输入访问地址：http://127.0.0.1:18686</li>
        <li>默认账号和密码：admin/admin</li>
    </ol>
    <h3>增量同步配置</h3>
    <table>
        <tbody>
            <tr>
                <td><b>类型</b></td>
                <td><b>配置</b></td>
                <td><b>原理</b></td>
            </tr>
            <tr>
                <td>
                    <p>Mysql</p>
                    <p>开启Binlog功能，my.ini配置:</p>
                </td>
                <td>
                    <p># 服务唯一ID</p>
                    <p>server_id=1</p>
                    <p>log-bin=mysql_bin</p>
                    <p>binlog-format=ROW</p>
                    <p>max_binlog_cache_size = 256M</p>
                    <p>max_binlog_size = 512M</p>
                    <p>expire_logs_days = 7</p>
                    <p># 多个库使用英文逗号“,”拼接</p>
                    <p>replicate-do-db=test</p>
                </td>
                <td>Dump Binlog二进制日志。Master同步Slave, 创建IO线程读取数据，写入relaylog，基于消息订阅捕获增量数据。</td>
            </tr>
            <tr>
                <td>
                    <p>Oracle</p>
                    <p>授予账号监听权限:</p>
                </td>
                <td>
                    <p>grant change notification to AE86</p>
                    <p>要求目标源表必须定义一个长度为18的varchar字段，用于接收rowid值，来实现增删改操作</p>
                </td>
                <td>CDN注册订阅。监听增删改事件，得到rowid，根据rowid执行SQL查询，得到变化数据</td>
            </tr>
            <tr>
                <td>
                    <p>SQLServer</p>
                    <p>开启CDC:</p>
                </td>
                <td>
                    <p>要求2008版本以上, 启动代理服务（Agent服务）, 连接账号具有 sysadmin 固定服务器角色或 db_owner 固定数据库角色的成员身份。对于所有其他用户，具有源表SELECT 权限；如果已定义捕获实例的访问控制角色，则还要求具有该数据库角色的成员身份。</p>
                </td>
                <td>SQL Server 2008提供了内建的方法变更数据捕获（Change Data Capture 即CDC）以实现异步跟踪用户表的数据修改</td>
            </tr>
        </tbody>
    </table>
    <p>Mysql增量示例</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0518/004448_26286acc_376718.png" />
    </p>
    <p>Oracle增量示例</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0518/004239_d79cf045_376718.png" />
    </p>
    <p>定时示例</p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2020/1023/160953_d34d6d11_376718.png" />
    </p>
</div>

<div>
    <h3>预览</h3>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2020/0519/000443_b52b4a8c_376718.png" />
    </p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2020/0602/221008_64dbb479_376718.png" />
    </p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2020/0602/221018_20d0ef67_376718.png" />
    </p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2020/0602/221029_c4f5d804_376718.png" />
    </p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0518/004836_3b4b9e49_376718.png" />
    </p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0518/004947_6883e6c8_376718.png" />
    </p>
    <p align="center">
        <img src="https://images.gitee.com/uploads/images/2021/0518/005017_31e6697b_376718.png" />
    </p>
    <h3>流程图</h3>
    <p align="center">
        <img src="http://assets.processon.com/chart_image/5d53b405e4b09965fac2ae27.png" />
    </p>
</div>

<div>
<h3>使用说明</h3>
    <p>驱动管理<p/>
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
    <p>监控<p/>
    <ol>
        <li>查看驱动同步数据</li>
        <li>查看系统性能指标</li>
        <li>查看系统操作日志</li>
    </ol>
    <p>其他<p/>
    <ol>
        <li>参数>修改系统参数</li>
        <li>参数>修改管理员密码</li>
        <li>注销</li>
    </ol>
    <h3>开发框架版本</h3>
    <ol>
        <li>JDK - 1.8.0_40 (推荐版本及以上)</li>
        <li>Maven - 3.3.9</li>
        <li><a target="_blank" href="https://docs.spring.io/spring-boot/docs/2.1.8.RELEASE/reference/html/">Spring Boot - 2.1.8.RELEASE</a></li>
        <li><a target="_blank" href="http://getbootstrap.com">Bootstrap - 3.3.4</a></li>
    </ol>
</div>

<div>
    <h3>欢迎加群</h3>
    QQ群: 875519623 或点击右侧按钮 <a target="_blank" href="//shang.qq.com/wpa/qunwpa?idkey=fce8d51b264130bac5890674e7db99f82f7f8af3f790d49fcf21eaafc8775f2a"><img border="0" src="//pub.idqqimg.com/wpa/images/group.png" alt="数据同步dbsyncer" title="数据同步dbsyncer" />
</div>