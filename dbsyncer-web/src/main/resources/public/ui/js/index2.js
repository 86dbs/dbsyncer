new Vue({
    el: '#app',
    data() {
        return {
            // 状态管理
            activeMenu: 'dashboard',
            collapsed: false,
            isMobile: window.innerWidth < 768,
            showConnectionForm: false,
            showTaskForm: false,
            showNotifications: false,
            activeSettingTab: 'basic',
            activeTaskTab: 'basic',

            // 分页配置
            currentPage: 1,
            pageSize: 10,

            // 仪表盘数据
            timeRange: 'today',
            timeRangeOptions: [
                {label: '今日', value: 'today'},
                {label: '昨日', value: 'yesterday'},
                {label: '本周', value: 'week'},
                {label: '本月', value: 'month'},
                {label: '自定义', value: 'custom'}
            ],

            chartType: 'throughput',
            chartTypeOptions: [
                {label: '吞吐量', value: 'throughput'},
                {label: '响应时间', value: 'responseTime'},
                {label: '成功率', value: 'successRate'}
            ],

            // 最近活动数据
            recentActivityData: [
                {
                    id: 1,
                    activity: '任务 "订单数据同步" 执行成功',
                    time: '10分钟前',
                    icon: 'fa fa-check-circle',
                    color: 'success'
                },
                {
                    id: 2,
                    activity: '新增数据库连接 "MySQL从库"',
                    time: '30分钟前',
                    icon: 'fa fa-plug',
                    color: 'primary'
                },
                {
                    id: 3,
                    activity: '任务 "用户数据同步" 执行失败',
                    time: '1小时前',
                    icon: 'fa fa-exclamation-circle',
                    color: 'danger'
                },
                {
                    id: 4,
                    activity: '修改任务 "商品数据同步" 配置',
                    time: '2小时前',
                    icon: 'fa fa-edit',
                    color: 'primary'
                },
                {
                    id: 5,
                    activity: '系统自动备份完成',
                    time: '昨天',
                    icon: 'fa fa-database',
                    color: 'warning'
                }
            ],

            // 连接管理数据
            dbTypeOptions: [
                {label: 'MySQL', value: 'mysql'},
                {label: 'PostgreSQL', value: 'postgresql'},
                {label: 'Oracle', value: 'oracle'},
                {label: 'SQL Server', value: 'sqlserver'},
                {label: 'MongoDB', value: 'mongodb'},
                {label: 'Redis', value: 'redis'}
            ],

            dbTypeMap: {
                'mysql': 'MySQL',
                'postgresql': 'PostgreSQL',
                'oracle': 'Oracle',
                'sqlserver': 'SQL Server',
                'mongodb': 'MongoDB',
                'redis': 'Redis'
            },

            connectionStatusOptions: [
                {label: '正常', value: 'connected'},
                {label: '断开', value: 'disconnected'},
                {label: '异常', value: 'error'}
            ],

            statusMap: {
                'connected': {label: '正常', color: 'success'},
                'disconnected': {label: '断开', color: 'danger'},
                'error': {label: '异常', color: 'warning'}
            },

            dbTypeFilter: '',
            connectionStatusFilter: '',
            connectionSearch: '',

            connectionsData: [
                {
                    id: 1,
                    name: '',
                    type: 'mysql',
                    host: '192.168.1.100',
                    port: 3306,
                    database: 'main_db',
                    username: 'admin',
                    status: 'connected',
                    lastTest: '2023-06-15 09:30:00'
                },
                {
                    id: 2,
                    name: '从数据库',
                    type: 'mysql',
                    host: '192.168.1.101',
                    port: 3306,
                    database: 'slave_db',
                    username: 'admin',
                    status: 'connected',
                    lastTest: '2023-06-15 09:30:00'
                },
                {
                    id: 3,
                    name: '报表数据库',
                    type: 'postgresql',
                    host: '192.168.1.102',
                    port: 5432,
                    database: 'report_db',
                    username: 'report_user',
                    status: 'error',
                    lastTest: '2023-06-15 10:15:00'
                },
                {
                    id: 4,
                    name: '历史归档库',
                    type: 'oracle',
                    host: '192.168.1.103',
                    port: 1521,
                    database: 'history_db',
                    username: 'archive_user',
                    status: 'disconnected',
                    lastTest: '2023-06-14 16:45:00'
                },
                {
                    id: 5,
                    name: '缓存数据库',
                    type: 'redis',
                    host: '192.168.1.104',
                    port: 6379,
                    database: '0',
                    username: '',
                    status: 'connected',
                    lastTest: '2023-06-15 11:20:00'
                }
            ],

            // 新增连接表单数据
            newConnection: {
                id: null,
                name: '',
                type: [{
                    value: '选项1',
                    label: '黄金糕'
                }, {
                    value: '选项2',
                    label: '双皮奶'
                }, {
                    value: '选项3',
                    label: '蚵仔煎'
                }, {
                    value: '选项4',
                    label: '龙须面'
                }, {
                    value: '选项5',
                    label: '北京烤鸭'
                }],
                host: 'localhost',
                port: 3306,
                database: '',
                username: '',
                password: '',
                charset: 'utf8mb4',
                timeout: 30,
                poolSize: 10,
                extraParams: ''
            },

            connectionRules: {
                name: [{required: true, message: '请输入连接名称', trigger: 'blur'}],
                type: [{required: true, message: '请选择数据库类型', trigger: 'change'}],
                host: [{required: true, message: '请输入主机地址', trigger: 'blur'}],
                port: [{required: true, message: '请输入端口号', trigger: 'blur'}],
                database: [{required: true, message: '请输入数据库名称', trigger: 'blur'}],
                username: [{required: true, message: '请输入用户名', trigger: 'blur'}]
            },

            charsetOptions: [
                {label: 'utf8', value: 'utf8'},
                {label: 'utf8mb4', value: 'utf8mb4'},
                {label: 'gbk', value: 'gbk'},
                {label: 'gb2312', value: 'gb2312'}
            ],

            // 同步任务数据
            taskStatusOptions: [
                {label: '运行中', value: 'running'},
                {label: '已停止', value: 'stopped'},
                {label: '已暂停', value: 'paused'},
                {label: '已失败', value: 'failed'}
            ],

            taskStatusMap: {
                'running': {label: '运行中', color: 'success'},
                'stopped': {label: '已停止', color: 'default'},
                'paused': {label: '已暂停', color: 'warning'},
                'failed': {label: '已失败', color: 'danger'}
            },

            syncTypeOptions: [
                {label: '全量同步', value: 'full'},
                {label: '增量同步', value: 'incremental'},
                {label: '实时同步', value: 'realtime'},
                {label: '定时同步', value: 'scheduled'}
            ],

            syncTypeMap: {
                'full': '全量同步',
                'incremental': '增量同步',
                'realtime': '实时同步',
                'scheduled': '定时同步'
            },

            taskStatusFilter: '',
            syncTypeFilter: '',
            taskSearch: '',

            tasksData: [
                {
                    id: 1,
                    name: '订单数据同步',
                    source: '主数据库',
                    target: '从数据库',
                    syncType: 'incremental',
                    status: 'running',
                    lastRun: '2023-06-15 10:30:00',
                    nextRun: '2023-06-15 11:30:00',
                    successRate: '99.8%',
                    avgTime: '2m 30s'
                },
                {
                    id: 2,
                    name: '用户数据同步',
                    source: '主数据库',
                    target: '报表数据库',
                    syncType: 'scheduled',
                    status: 'failed',
                    lastRun: '2023-06-15 09:15:00',
                    nextRun: '2023-06-15 11:15:00',
                    successRate: '65.2%',
                    avgTime: '5m 12s'
                },
                {
                    id: 3,
                    name: '商品数据同步',
                    source: '主数据库',
                    target: '从数据库',
                    syncType: 'realtime',
                    status: 'running',
                    lastRun: '刚刚',
                    nextRun: '持续运行',
                    successRate: '100%',
                    avgTime: '0.5s'
                },
                {
                    id: 4,
                    name: '销售报表同步',
                    source: '从数据库',
                    target: '报表数据库',
                    syncType: 'scheduled',
                    status: 'stopped',
                    lastRun: '2023-06-14 23:00:00',
                    nextRun: '已停止',
                    successRate: '98.5%',
                    avgTime: '15m 40s'
                },
                {
                    id: 5,
                    name: '历史数据归档',
                    source: '主数据库',
                    target: '历史归档库',
                    syncType: 'full',
                    status: 'paused',
                    lastRun: '2023-06-15 02:30:00',
                    nextRun: '手动恢复',
                    successRate: '75.3%',
                    avgTime: '45m 18s'
                }
            ],

            // 创建任务表单数据
            newTask: {
                id: null,
                name: '',
                description: '',
                sourceConnectionId: '',
                targetConnectionId: '',
                syncType: 'incremental',
                syncStrategy: 'insert_update',
                status: 'enabled',
                executionMode: 'scheduled',
                cronExpression: '0 0/30 * * * ?',
                timeout: 30,
                retryCount: 3,
                parallelism: 3,
                batchSize: 1000,
                conflictStrategy: 'overwrite',
                filterCondition: '',
                advancedOptions: ['use_transaction'],
                tableMappings: []
            },

            taskRules: {
                name: [{required: true, message: '请输入任务名称', trigger: 'blur'}],
                sourceConnectionId: [{required: true, message: '请选择源数据库', trigger: 'change'}],
                targetConnectionId: [{required: true, message: '请选择目标数据库', trigger: 'change'}],
                syncType: [{required: true, message: '请选择同步类型', trigger: 'change'}],
                syncStrategy: [{required: true, message: '请选择同步策略', trigger: 'change'}],
                executionMode: [{required: true, message: '请选择执行方式', trigger: 'change'}],
                cronExpression: [
                    {
                        required: function () {
                            return this.newTask.executionMode === 'scheduled'
                        },
                        message: '请输入调度表达式',
                        trigger: 'blur'
                    }
                ]
            },

            // 连接选项（用于任务创建）
            connectionOptions() {
                return this.connectionsData.map(conn => ({
                    label: conn.name,
                    value: conn.name
                }));
            },

            // 同步策略选项
            syncStrategyOptions: [
                {label: '插入更新', value: 'insert_update'},
                {label: '只插入', value: 'insert_only'},
                {label: '只更新', value: 'update_only'},
                {label: '删除后插入', value: 'delete_insert'}
            ],

            // 执行方式选项
            executionModeOptions: [
                {label: '立即执行', value: 'immediate'},
                {label: '定时执行', value: 'scheduled'},
                {label: '实时同步', value: 'realtime'},
                {label: '手动执行', value: 'manual'}
            ],

            // 冲突解决策略选项
            conflictStrategyOptions: [
                {label: '覆盖', value: 'overwrite'},
                {label: '忽略', value: 'ignore'},
                {label: '抛异常', value: 'abort'},
                {label: '合并', value: 'merge'}
            ],

            mappingTypeMap: {
                'full': '全量映射',
                'partial': '部分映射',
                'custom': '自定义SQL'
            },

            // 同步日志数据
            logLevelOptions: [
                {label: 'INFO', value: 'info'},
                {label: 'WARN', value: 'warn'},
                {label: 'ERROR', value: 'error'},
                {label: 'DEBUG', value: 'debug'}
            ],

            logLevelMap: {
                'info': {label: 'INFO', color: 'info'},
                'warn': {label: 'WARN', color: 'warning'},
                'error': {label: 'ERROR', color: 'danger'},
                'debug': {label: 'DEBUG', color: 'default'}
            },

            logTaskOptions() {
                return [
                    {label: '所有任务', value: ''},
                    ...this.tasksData.map(task => ({
                        label: task.name,
                        value: task.id.toString()
                    }))
                ];
            },

            logLevelFilter: '',
            logTaskFilter: '',
            logSearch: '',
            logDateRange: null,

            logsData: [
                {
                    id: 1,
                    taskId: 1,
                    taskName: '订单数据同步',
                    level: 'info',
                    message: '任务开始执行',
                    timestamp: '2023-06-15 10:30:00'
                },
                {
                    id: 2,
                    taskId: 1,
                    taskName: '订单数据同步',
                    level: 'info',
                    message: '开始读取源数据，预计1254条记录',
                    timestamp: '2023-06-15 10:30:02'
                },
                {
                    id: 3,
                    taskId: 1,
                    taskName: '订单数据同步',
                    level: 'info',
                    message: '数据同步完成，成功1254条，失败0条',
                    timestamp: '2023-06-15 10:32:30'
                },
                {
                    id: 4,
                    taskId: 2,
                    taskName: '用户数据同步',
                    level: 'info',
                    message: '任务开始执行',
                    timestamp: '2023-06-15 09:15:00'
                },
                {
                    id: 5,
                    taskId: 2,
                    taskName: '用户数据同步',
                    level: 'error',
                    message: '连接报表数据库失败：超时',
                    timestamp: '2023-06-15 09:16:30'
                },
                {
                    id: 6,
                    taskId: 2,
                    taskName: '用户数据同步',
                    level: 'warn',
                    message: '尝试重试连接...',
                    timestamp: '2023-06-15 09:16:35'
                },
                {
                    id: 7,
                    taskId: 2,
                    taskName: '用户数据同步',
                    level: 'error',
                    message: '任务执行失败，达到最大重试次数',
                    timestamp: '2023-06-15 09:18:45'
                },
                {
                    id: 8,
                    taskId: 3,
                    taskName: '商品数据同步',
                    level: 'debug',
                    message: '处理实时变更事件：新增商品 ID=12345',
                    timestamp: '2023-06-15 11:05:23'
                }
            ],

            // 系统设置数据
            appName: 'DBsyncer',
            refreshInterval: '30s',
            refreshIntervalOptions: [
                {label: '10秒', value: '10s'},
                {label: '30秒', value: '30s'},
                {label: '1分钟', value: '1m'},
                {label: '5分钟', value: '5m'},
                {label: '手动刷新', value: 'manual'}
            ],

            themeMode: 'light',
            language: 'zh-CN',
            languageOptions: [
                {label: '简体中文', value: 'zh-CN'},
                {label: 'English', value: 'en-US'},
                {label: '日本語', value: 'ja-JP'}
            ],

            logRetentionDays: 30,
            notificationSettings: ['task_failure', 'system_updates'],

            newPassword: '',
            confirmPassword: '',
            apiAccessControl: true,
            apiKey: 'sk_dbsyncer_8f7e6d5c4b3a210',

            // 用户选项
            userOptions: [
                {
                    label: '个人信息',
                    key: 'profile'
                },
                {
                    label: '修改密码',
                    key: 'password'
                },
                {
                    type: 'divider'
                },
                {
                    label: '退出登录',
                    key: 'logout',
                    color: 'danger'
                }
            ]
        };
    },

    computed: {
        // 过滤连接数据
        filteredConnections() {
            return this.connectionsData.filter(conn => {
                const matchesType = !this.dbTypeFilter || conn.type === this.dbTypeFilter;
                const matchesStatus = !this.connectionStatusFilter || conn.status === this.connectionStatusFilter;
                const matchesSearch = !this.connectionSearch ||
                    conn.name.includes(this.connectionSearch) ||
                    conn.host.includes(this.connectionSearch) ||
                    conn.database.includes(this.connectionSearch);

                return matchesType && matchesStatus && matchesSearch;
            });
        },

        // 过滤任务数据
        filteredTasks() {
            return this.tasksData.filter(task => {
                const matchesStatus = !this.taskStatusFilter || task.status === this.taskStatusFilter;
                const matchesType = !this.syncTypeFilter || task.syncType === this.syncTypeFilter;
                const matchesSearch = !this.taskSearch ||
                    task.name.includes(this.taskSearch) ||
                    task.source.includes(this.taskSearch) ||
                    task.target.includes(this.taskSearch);

                return matchesStatus && matchesType && matchesSearch;
            });
        },

        // 过滤日志数据
        filteredLogs() {
            return this.logsData.filter(log => {
                const matchesLevel = !this.logLevelFilter || log.level === this.logLevelFilter;
                const matchesTask = !this.logTaskFilter || log.taskId.toString() === this.logTaskFilter;
                const matchesSearch = !this.logSearch ||
                    log.message.includes(this.logSearch) ||
                    log.taskName.includes(this.logSearch);

                return matchesLevel && matchesTask && matchesSearch;
            });
        }
    },

    mounted() {
        // 监听窗口大小变化
        const handleResize = () => {
            this.isMobile = window.innerWidth < 768;
            if (window.innerWidth >= 768) {
                this.collapsed = false;
            }
        };

        window.addEventListener('resize', handleResize);
        this.$once('hook:beforeDestroy', () => {
            window.removeEventListener('resize', handleResize);
        });

        // 初始化图表
        this.initCharts();

        this.loadDateConnections();


    },

    methods: {


        // 初始化图表
        initCharts() {
            // 性能趋势图
            const performanceCtx = document.getElementById('performanceChart').getContext('2d');
            this.performanceChart = new Chart(performanceCtx, {
                type: 'line',
                data: {
                    labels: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00', '现在'],
                    datasets: [{
                        label: '同步记录数',
                        data: [1200, 1900, 3000, 5000, 4000, 6000, 7500],
                        borderColor: '#409EFF',
                        backgroundColor: 'rgba(64, 158, 255, 0.1)',
                        tension: 0.3,
                        fill: true
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'top',
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });

            // 状态分布图
            const statusCtx = document.getElementById('statusChart').getContext('2d');
            this.statusChart = new Chart(statusCtx, {
                type: 'doughnut',
                data: {
                    labels: ['运行中', '已停止', '失败', '已完成'],
                    datasets: [{
                        data: [18, 4, 2, 10],
                        backgroundColor: [
                            '#67C23A',
                            '#909399',
                            '#F56C6C',
                            '#409EFF'
                        ],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                        }
                    },
                    cutout: '70%'
                }
            });
        },

        loadDateConnections() {
            axios.get('/index/getConnectList')
                .then(response => {
                    console.log(response.data);

                    var connectors = response.data.connectors;

                    this.connectionsData = connectors.map(item => ({
                        id: item.id,
                        name: item.name,
                        type: item.config.connectorType,
                        host: item.config.url,
                        username: item.config.username,
                        status: 'disconnected', // 默认状态，可以根据实际情况调整
                        lastTest: ''
                    }));

                })
                .catch(error => {
                    console.error('获取连接列表失败:', error);
                    this.$message.error('获取连接列表失败');
                });
        },

        // 处理菜单切换
        handleMenuChange(key) {
            this.activeMenu = key;
            if (this.isMobile) {
                this.collapsed = true;
            }
        },

        // 切换侧边栏（移动端）
        toggleSidebar() {
            this.collapsed = !this.collapsed;
        },

        // 处理用户操作
        handleUserAction(key) {
            if (key === 'logout') {
                this.$message.success('已退出登录');
            } else {
                this.$message.info(`用户操作: ${key}`);
            }
        },

        // 分页方法
        handleSizeChange(val) {
            this.pageSize = val;
            this.currentPage = 1;
        },

        handleCurrentChange(val) {
            this.currentPage = val;
        },

        // 处理连接操作
        handleConnectionTest(id) {
            this.$message.success(`正在测试连接 #${id}...`);
        },

        handleConnectionEdit(id) {
            const conn = this.connectionsData.find(c => c.id === id);
            if (conn) {
                this.newConnection = {...conn};
                this.showConnectionForm = true;
            }
        },

        handleConnectionDelete(id) {
            this.$confirm(`确定要删除连接 #${id} 吗?`, '提示', {
                confirmButtonText: '确定',
                cancelButtonText: '取消',
                type: 'warning'
            }).then(() => {
                const index = this.connectionsData.findIndex(c => c.id === id);
                if (index !== -1) {
                    this.connectionsData.splice(index, 1);
                }
                this.$message.success('删除成功');
            }).catch(() => {
                this.$message.info('已取消删除');
            });
        },

        // 测试连接
        testConnection() {
            this.$refs.connectionForm.validate(valid => {
                if (valid) {
                    this.$message({
                        message: '正在测试连接...',
                        type: 'info',
                        duration: 1000
                    });
                    setTimeout(() => {
                        this.$message.success('连接测试成功！');
                    }, 1000);
                }
            });
        },

        // 保存连接
        saveConnection() {
            this.$refs.connectionForm.validate(valid => {
                if (valid) {
                    if (this.newConnection.id) {
                        // 更新现有连接
                        const index = this.connectionsData.findIndex(c => c.id === this.newConnection.id);
                        if (index !== -1) {
                            this.connectionsData[index] = {...this.newConnection};
                        }
                        this.$message.success('连接更新成功！');
                    } else {
                        // 添加新连接
                        const newId = Math.max(...this.connectionsData.map(c => c.id), 0) + 1;
                        const conn = {...this.newConnection, id: newId, status: 'disconnected', lastTest: ''};
                        this.connectionsData.push(conn);
                        this.$message.success('新连接创建成功！');
                    }
                    this.showConnectionForm = false;
                    // 重置表单
                    this.resetConnectionForm();
                }
            });
        },

        // 重置连接表单
        resetConnectionForm() {
            this.newConnection = {
                id: null,
                name: '',
                type: '',
                host: 'localhost',
                port: 3306,
                database: '',
                username: '',
                password: '',
                charset: 'utf8mb4',
                timeout: 30,
                poolSize: 10,
                extraParams: ''
            };
            this.$refs.connectionForm.resetFields();
        },

        // 处理任务操作
        handleTaskStartStop(id, status) {
            const action = status === 'running' ? '停止' : '启动';
            this.$message.success(`${action}任务 #${id} 成功`);
        },

        handleTaskEdit(id) {
            const task = this.tasksData.find(t => t.id === id);
            if (task) {
                // 转换任务数据到表单格式
                this.newTask = {
                    id,
                    name: task.name,
                    description: '',
                    sourceConnectionId: task.source,
                    targetConnectionId: task.target,
                    syncType: task.syncType,
                    syncStrategy: 'insert_update',
                    status: task.status === 'running' ? 'enabled' : 'disabled',
                    executionMode: task.syncType === 'realtime' ? 'realtime' : 'scheduled',
                    cronExpression: '0 0/30 * * * ?',
                    timeout: 30,
                    retryCount: 3,
                    parallelism: 3,
                    batchSize: 1000,
                    conflictStrategy: 'overwrite',
                    filterCondition: '',
                    advancedOptions: ['use_transaction'],
                    tableMappings: [
                        {
                            id: 1,
                            sourceTable: 'orders',
                            targetTable: 'orders_copy',
                            mappingType: 'full'
                        }
                    ]
                };
                this.showTaskForm = true;
            }
        },

        handleTaskLogs(id) {
            this.activeMenu = 'logs';
            this.logTaskFilter = id.toString();
        },

        // 表映射操作
        addTableMapping() {
            const newId = this.newTask.tableMappings.length + 1;
            this.newTask.tableMappings.push({
                id: newId,
                sourceTable: '',
                targetTable: '',
                mappingType: 'full'
            });
        },

        handleEditTableMapping(index) {
            this.$message.info(`编辑表映射 #${index + 1}`);
        },

        handleDeleteTableMapping(index) {
            this.newTask.tableMappings.splice(index, 1);
        },

        // 保存任务
        saveTask() {
            this.$refs.taskForm.validate(valid => {
                if (valid) {
                    if (this.newTask.id) {
                        // 更新现有任务
                        const index = this.tasksData.findIndex(t => t.id === this.newTask.id);
                        if (index !== -1) {
                            this.tasksData[index] = {
                                ...this.tasksData[index],
                                name: this.newTask.name,
                                source: this.newTask.sourceConnectionId,
                                target: this.newTask.targetConnectionId,
                                syncType: this.newTask.syncType,
                                status: this.newTask.status === 'enabled' ? 'running' : 'stopped'
                            };
                        }
                        this.$message.success('任务更新成功！');
                    } else {
                        // 添加新任务
                        const newId = Math.max(...this.tasksData.map(t => t.id), 0) + 1;
                        const task = {
                            id: newId,
                            name: this.newTask.name,
                            source: this.newTask.sourceConnectionId,
                            target: this.newTask.targetConnectionId,
                            syncType: this.newTask.syncType,
                            status: this.newTask.status === 'enabled' ? 'running' : 'stopped',
                            lastRun: '从未运行',
                            nextRun: this.newTask.executionMode === 'scheduled' ? '即将运行' : '立即执行',
                            successRate: '0%',
                            avgTime: '0s'
                        };
                        this.tasksData.push(task);
                        this.$message.success('新任务创建成功！');
                    }
                    this.showTaskForm = false;
                    // 重置表单
                    this.resetTaskForm();
                }
            });
        },

        // 重置任务表单
        resetTaskForm() {
            this.newTask = {
                id: null,
                name: '',
                description: '',
                sourceConnectionId: '',
                targetConnectionId: '',
                syncType: 'incremental',
                syncStrategy: 'insert_update',
                status: 'enabled',
                executionMode: 'scheduled',
                cronExpression: '0 0/30 * * * ?',
                timeout: 30,
                retryCount: 3,
                parallelism: 3,
                batchSize: 1000,
                conflictStrategy: 'overwrite',
                filterCondition: '',
                advancedOptions: ['use_transaction'],
                tableMappings: []
            };
            this.$refs.taskForm.resetFields();
        }
    }
});