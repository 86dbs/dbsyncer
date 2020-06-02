<div>
	<p>DBSyncer是一款开源的数据同步软件，提供Mysql、Oracle、SqlServer、Redis、SQL结果集等场景，支持自定义同步转换业务。</p>
</div>

<div>
    <h3>特点</h3>
    <ol>
        <li>组合驱动，自定义库同步到库组合，关系型数据库与非关系型之间组合，任意搭配表同步映射关系</li>
        <li>实时监控，驱动全量或增量实时同步运行状态、结果、同步日志和系统日志</li>
        <li>开发插件，自定义同步转化逻辑</li>
        <li>单机/HA高可用部署</li>
    </ol>
</div>

<div>
    <p>运行环境</p>
    <ol>
        <li><a target="_blank" href="https://www.oracle.com/java/technologies/jdk8-downloads.html">JRE 1.8 +</a></li>
        <li><a target="_blank" href="http://apache.fayea.com/zookeeper/stable/">Zookeeper - 3.5.5</a> （高可用模式下需要）</li>
    </ol>
    <h3>安装说明</h3>
    <ol>
        <li>安装JRE1.8版本以上（省略详细）</li>
        <li>下载安装包<a target="_blank" href="#">dbsyncer-1.0.0-Alpha.zip</a></li>
        <li>解压，进入目录bin</li>
        <li>Windows平台：startup.bat启动脚本</li>
        <li>Linux平台：startup.sh启动脚本</li>
        <li>打开浏览器，输入访问地址：http://127.0.0.1:18686</li>
        <li>默认账号和密码：admin/admin</li>
    </ol>
    <h3>使用说明</h3>
    <p>驱动管理<p/>
    <ol>
        <li>首先，创建一个连接器。选择数据源类型，比如：Mysql，填写配置，保存</li>
        <li>添加驱动。配置数据源和目标源（数据源：数据的发送端，目标源：数据接受端），保存</li>
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
        <li>参数>>修改系统参数</li>
        <li>参数>>修改管理员密码</li>
        <li>注销</li>
    </ol>
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
        <img src="https://images.gitee.com/uploads/images/2020/0602/220345_291a73af_376718.png" />
    </p>
    <h3>流程图</h3>
    <p align="center">
        <img src="http://assets.processon.com/chart_image/5d53b405e4b09965fac2ae27.png" />
    </p>
    <h3>部署图</h3>
    <p align="center">
        <img src="http://assets.processon.com/chart_image/5d63b0bce4b0ac2b61877037.png" />
    </p>
</div>

<div>
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