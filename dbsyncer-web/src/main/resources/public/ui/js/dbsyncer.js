// public/ui/dbsyncer.js
new Vue({
    el: '#app',
    data() {
        return {
            // 侧边栏状态
            sidebarCollapsed: false,
            // 当前页面
            currentPage: 'dashboard',
            currentPageTitle: '数据同步仪表盘',
            // 用户信息
            userName: '管理员',
            showUserMenu: false,
            
            // 统计数据
            stats: {
                runningTasks: 8,
                stoppedTasks: 3,
                errorTasks: 1,
                dataSources: 12
            },

            // 图表相关
            timeRange: 'day',
            syncVolumeChart: null,
            taskStatusChart: null,
            
            // 最近活动
            recentActivities: [
                {
                    taskName: '用户数据同步',
                    source: 'MySQL - 主数据库',
                    target: 'PostgreSQL - 分析库',
                    status: 'running',
                    records: 1254,
                    time: '2023-07-15 09:30:22'
                },
                {
                    taskName: '订单数据同步',
                    source: 'MySQL - 主数据库',
                    target: 'MongoDB - 备份库',
                    status: 'running',
                    records: 326,
                    time: '2023-07-15 08:15:47'
                },
                {
                    taskName: '产品信息同步',
                    source: 'MySQL - 主数据库',
                    target: 'Elasticsearch - 搜索库',
                    status: 'error',
                    records: 0,
                    time: '2023-07-15 07:00:12'
                },
                {
                    taskName: '日志数据同步',
                    source: 'MySQL - 日志库',
                    target: 'ClickHouse - 数据仓库',
                    status: 'running',
                    records: 5892,
                    time: '2023-07-15 06:45:33'
                }
            ],
            
            // 任务数据
            tasks: [
                {
                    id: 1,
                    name: '用户数据同步',
                    sourceId: 1,
                    sourceName: 'MySQL - 主数据库',
                    targetId: 2,
                    targetName: 'PostgreSQL - 分析库',
                    syncType: 'incremental',
                    interval: 30,
                    status: 'running',
                    lastSyncTime: '2023-07-15 09:30:22'
                },
                {
                    id: 2,
                    name: '订单数据同步',
                    sourceId: 1,
                    sourceName: 'MySQL - 主数据库',
                    targetId: 3,
                    targetName: 'MongoDB - 备份库',
                    syncType: 'incremental',
                    interval: 60,
                    status: 'running',
                    lastSyncTime: '2023-07-15 08:15:47'
                },
                {
                    id: 3,
                    name: '产品信息同步',
                    sourceId: 1,
                    sourceName: 'MySQL - 主数据库',
                    targetId: 4,
                    targetName: 'Elasticsearch - 搜索库',
                    syncType: 'full',
                    interval: 1440,
                    status: 'error',
                    lastSyncTime: '2023-07-15 07:00:12'
                },
                {
                    id: 4,
                    name: '日志数据同步',
                    sourceId: 5,
                    sourceName: 'MySQL - 日志库',
                    targetId: 6,
                    targetName: 'ClickHouse - 数据仓库',
                    syncType: 'incremental',
                    interval: 15,
                    status: 'running',
                    lastSyncTime: '2023-07-15 06:45:33'
                },
                {
                    id: 5,
                    name: '库存数据同步',
                    sourceId: 1,
                    sourceName: 'MySQL - 主数据库',
                    targetId: 7,
                    targetName: 'Redis - 缓存库',
                    syncType: 'incremental',
                    interval: 5,
                    status: 'stopped',
                    lastSyncTime: '2023-07-14 23:10:05'
                }
            ],
            
            // 筛选后的任务
            filteredTasks: [],
            // 任务筛选条件
            taskFilter: {
                name: '',
                status: '',
                source: '',
                target: ''
            },
            // 选中的任务
            selectedTasks: [],
            // 任务加载状态
            tasksLoading: false,
            // 分页信息
            currentPageNum: 1,
            pageSize: 10,
            
            // 数据源
            dataSources: [
                { id: 1, name: 'MySQL - 主数据库', type: 'MySQL', host: '192.168.1.100', port: 3306, database: 'main_db', username: 'admin', status: 'connected' },
                { id: 2, name: 'PostgreSQL - 分析库', type: 'PostgreSQL', host: '192.168.1.101', port: 5432, database: 'analytics_db', username: 'analyst', status: 'connected' },
                { id: 3, name: 'MongoDB - 备份库', type: 'MongoDB', host: '192.168.1.102', port: 27017, database: 'backup_db', username: 'backup', status: 'connected' },
                { id: 4, name: 'Elasticsearch - 搜索库', type: 'Elasticsearch', host: '192.168.1.103', port: 9200, database: 'search_db', username: '', status: 'connected' },
                { id: 5, name: 'MySQL - 日志库', type: 'MySQL', host: '192.168.1.104', port: 3306, database: 'log_db', username: 'logger', status: 'connected' },
                { id: 6, name: 'ClickHouse - 数据仓库', type: 'ClickHouse', host: '192.168.1.105', port: 8123, database: 'data_warehouse', username: 'dw_user', status: 'connected' },
                { id: 7, name: 'Redis - 缓存库', type: 'Redis', host: '192.168.1.106', port: 6379, database: '0', username: '', status: 'connected' }
            ],
            
            // 同步日志
            syncLogs: [
                { id: 1, taskName: '用户数据同步', status: 'running', startTime: '2023-07-15 09:30:22', endTime: '2023-07-15 09:32:15', duration: 113, records: 1254 },
                { id: 2, taskName: '订单数据同步', status: 'running', startTime: '2023-07-15 08:15:47', endTime: '2023-07-15 08:16:32', duration: 45, records: 326 },
                { id: 3, taskName: '产品信息同步', status: 'error', startTime: '2023-07-15 07:00:12', endTime: '2023-07-15 07:01:45', duration: 93, records: 0 },
                { id: 4, taskName: '日志数据同步', status: 'running', startTime: '2023-07-15 06:45:33', endTime: '2023-07-15 06:50:12', duration: 279, records: 5892 },
                { id: 5, taskName: '库存数据同步', status: 'stopped', startTime: '2023-07-14 23:10:05', endTime: '2023-07-14 23:10:22', duration: 17, records: 45 }
            ],
            
            // 系统设置
            systemSettings: {
                threadCount: 5,
                logRetentionDays: 30,
                retryCount: 3,
                enableEmailNotify: true,
                notifyEmail: 'admin@example.com',
                timeout: 30
            },
            
            // 任务对话框
            taskDialogVisible: false,
            currentTask: {
                id: null,
                name: '',
                sourceId: '',
                sourceTable: '',
                targetId: '',
                targetTable: '',
                syncType: 'incremental',
                interval: 30,
                condition: '',
                fieldMappings: []
            }
        };
    },
    mounted() {
        // 初始化图表
        this.initCharts();
        // 初始化筛选任务
        this.filteredTasks = [...this.tasks];
        // 添加默认字段映射
        this.addFieldMapping();
    },
    methods: {
        // 切换侧边栏
        toggleUserMenu() {
            this.showUserMenu = !this.showUserMenu;
            console.log('showUserMenu:', this.showUserMenu); // 调试用
        },
        // 处理菜单选择
        handleMenuSelect(index) {
            this.currentPage = index;
            
            // 更新页面标题
            const pageTitles = {
                'dashboard': '数据同步仪表盘',
                'tasks': '同步任务管理',
                'datasources': '数据源管理',
                'logs': '同步日志',
                'settings': '系统设置',
                'docs': '文档中心',
                'about': '关于我们'
            };
            
            this.currentPageTitle = pageTitles[index] || 'DBsyncer';
            
            // 在移动设备上选择菜单后收起侧边栏
            if (window.innerWidth <= 768) {
                this.sidebarCollapsed = false;
            }
        },
        
        // 格式化状态显示
        formatStatus(status) {
            const statusMap = {
                'running': '运行中',
                'stopped': '已停止',
                'error': '异常',
                'connected': '已连接',
                'disconnected': '未连接'
            };
            return statusMap[status] || status;
        },
        
        // 初始化图表
        initCharts() {
            // 同步数据量趋势图
            const syncVolumeCtx = document.getElementById('syncVolumeChart').getContext('2d');
            this.syncVolumeChart = new Chart(syncVolumeCtx, {
                type: 'line',
                data: {
                    labels: ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
                    datasets: [{
                        label: '同步记录数',
                        data: [1200, 800, 500, 1500, 2800, 3200, 2500, 1800],
                        borderColor: '#3498db',
                        backgroundColor: 'rgba(52, 152, 219, 0.1)',
                        tension: 0.4,
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
                    },
                    animation: {
                        duration: 1000,
                        easing: 'easeOutQuart'
                    }
                }
            });
            
            // 任务状态分布图
            const taskStatusCtx = document.getElementById('taskStatusChart').getContext('2d');
            this.taskStatusChart = new Chart(taskStatusCtx, {
                type: 'doughnut',
                data: {
                    labels: ['运行中', '已停止', '异常'],
                    datasets: [{
                        data: [this.stats.runningTasks, this.stats.stoppedTasks, this.stats.errorTasks],
                        backgroundColor: [
                            '#2ecc71',
                            '#95a5a6',
                            '#e74c3c'
                        ],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom'
                        }
                    },
                    cutout: '70%',
                    animation: {
                        animateRotate: true,
                        animateScale: true
                    }
                }
            });
        },
        
        // 更新图表数据
        updateCharts() {
            // 根据时间范围更新图表数据
            if (this.timeRange === 'day') {
                this.syncVolumeChart.data.labels = ['00:00', '03:00', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00'];
                this.syncVolumeChart.data.datasets[0].data = [1200, 800, 500, 1500, 2800, 3200, 2500, 1800];
            } else if (this.timeRange === 'week') {
                this.syncVolumeChart.data.labels = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'];
                this.syncVolumeChart.data.datasets[0].data = [12000, 15000, 13500, 18000, 21000, 9500, 8000];
            } else if (this.timeRange === 'month') {
                this.syncVolumeChart.data.labels = ['第1周', '第2周', '第3周', '第4周'];
                this.syncVolumeChart.data.datasets[0].data = [65000, 78000, 82000, 95000];
            }
            this.syncVolumeChart.update();
        },
        
        // 刷新仪表盘数据
        refreshDashboard() {
            // 模拟数据刷新
            this.stats.runningTasks += Math.floor(Math.random() * 3) - 1;
            this.stats.stoppedTasks += Math.floor(Math.random() * 3) - 1;
            this.stats.errorTasks += Math.floor(Math.random() * 2) - 0;
            
            // 确保数值不为负
            this.stats.runningTasks = Math.max(0, this.stats.runningTasks);
            this.stats.stoppedTasks = Math.max(0, this.stats.stoppedTasks);
            this.stats.errorTasks = Math.max(0, this.stats.errorTasks);
            
            // 更新图表
            this.taskStatusChart.data.datasets[0].data = [this.stats.runningTasks, this.stats.stoppedTasks, this.stats.errorTasks];
            this.taskStatusChart.update();
            
            this.$message.success('数据已刷新');
        },
        
        // 查看所有日志
        viewAllLogs() {
            this.handleMenuSelect('logs');
        },
        
        // 查看详情
        viewDetails(row) {
            this.$message.info(`查看 ${row.taskName} 的详情`);
        },
        
        // 筛选任务
        filterTasks() {
            this.filteredTasks = this.tasks.filter(task => {
                const nameMatch = task.name.toLowerCase().includes(this.taskFilter.name.toLowerCase());
                const statusMatch = !this.taskFilter.status || task.status === this.taskFilter.status;
                const sourceMatch = !this.taskFilter.source || task.sourceId === this.taskFilter.source;
                const targetMatch = !this.taskFilter.target || task.targetId === this.taskFilter.target;
                return nameMatch && statusMatch && sourceMatch && targetMatch;
            });
            this.currentPageNum = 1; // 重置到第一页
        },
        
        // 重置任务筛选
        resetTaskFilter() {
            this.taskFilter = {
                name: '',
                status: '',
                source: '',
                target: ''
            };
            this.filteredTasks = [...this.tasks];
            this.currentPageNum = 1;
        },
        
        // 处理任务选择变化
        handleTaskSelectionChange(selection) {
            this.selectedTasks = selection;
        },
        
        // 批量启动任务
        batchStartTasks() {
            if (this.selectedTasks.length === 0) {
                this.$message.warning('请先选择要启动的任务');
                return;
            }
            
            this.selectedTasks.forEach(task => {
                const index = this.tasks.findIndex(t => t.id === task.id);
                if (index !== -1) {
                    this.tasks[index].status = 'running';
                }
            });
            
            this.filterTasks(); // 刷新筛选结果
            this.$message.success(`已启动 ${this.selectedTasks.length} 个任务`);
            this.selectedTasks = []; // 清空选择
        },
        
        // 批量停止任务
        batchStopTasks() {
            if (this.selectedTasks.length === 0) {
                this.$message.warning('请先选择要停止的任务');
                return;
            }
            
            this.selectedTasks.forEach(task => {
                const index = this.tasks.findIndex(t => t.id === task.id);
                if (index !== -1) {
                    this.tasks[index].status = 'stopped';
                }
            });
            
            this.filterTasks(); // 刷新筛选结果
            this.$message.success(`已停止 ${this.selectedTasks.length} 个任务`);
            this.selectedTasks = []; // 清空选择
        },
        
        // 启动任务
        startTask(task) {
            const index = this.tasks.findIndex(t => t.id === task.id);
            if (index !== -1) {
                this.tasks[index].status = 'running';
                this.filterTasks();
                this.$message.success(`已启动任务: ${task.name}`);
            }
        },
        // 停止任务
        stopTask(task) {
            const index = this.tasks.findIndex(t => t.id === task.id);
            if (index !== -1) {
                this.tasks[index].status = 'stopped';
                this.filterTasks();
                this.$message.success(`已停止任务: ${task.name}`);
            }
        },
        
        // 打开任务对话框
        openTaskDialog() {
            // 重置当前任务
            // this.currentTask = {
            //     id: null,
            //     name: '',
            //     sourceId: '',
            //     sourceTable: '',
            //     targetId: '',
            //     targetTable: '',
            //     syncType: 'incremental',
            //     interval: 30,
            //     condition: '',
            //     fieldMappings: []
            // };
            console.log('openTaskDialog');

            // this.addFieldMapping(); // 添加一个默认字段映射
            this.taskDialogVisible = true;
        },
        
        // 编辑任务
        editTask(task) {
            // 复制任务数据
            this.currentTask = { ...task };
            // 添加字段映射（实际应用中应该从后端获取）
            this.currentTask.fieldMappings = [
                { sourceField: 'id', targetField: 'id' },
                { sourceField: 'name', targetField: 'name' }
            ];
            this.taskDialogVisible = true;
        },
        
        // 添加字段映射
        addFieldMapping() {
            this.currentTask.fieldMappings.push({
                sourceField: '',
                targetField: ''
            });
        },
        
        // 移除字段映射
        removeFieldMapping(index) {
            if (this.currentTask.fieldMappings.length > 1) {
                this.currentTask.fieldMappings.splice(index, 1);
            } else {
                this.$message.warning('至少保留一个字段映射');
            }
        },

        // 添加这个方法
        say(message) {
            console.log(message);
            // 或者显示一个提示消息
            this.$message.info(message);
        },
        // 保存任务
        saveTask() {
            if (!this.currentTask.name) {
                this.$message.error('请输入任务名称');
                return;
            }
            
            if (!this.currentTask.sourceId || !this.currentTask.targetId) {
                this.$message.error('请选择源数据库和目标数据库');
                return;
            }
            
            if (!this.currentTask.sourceTable || !this.currentTask.targetTable) {
                this.$message.error('请输入源表和目标表');
                return;
            }
            
            // 检查是否有有效的字段映射
            const hasValidMapping = this.currentTask.fieldMappings.some(
                mapping => mapping.sourceField && mapping.targetField
            );
            
            if (!hasValidMapping) {
                this.$message.error('请至少设置一个有效的字段映射');
                return;
            }
            
            if (this.currentTask.id) {
                // 更新现有任务
                const index = this.tasks.findIndex(t => t.id === this.currentTask.id);
                if (index !== -1) {
                    this.tasks[index] = { ...this.currentTask };
                    this.$message.success('任务已更新');
                }
            } else {
                // 创建新任务
                const newId = Math.max(...this.tasks.map(t => t.id), 0) + 1;
                const sourceName = this.dataSources.find(db => db.id === this.currentTask.sourceId)?.name || '';
                const targetName = this.dataSources.find(db => db.id === this.currentTask.targetId)?.name || '';
                
                this.tasks.push({
                    ...this.currentTask,
                    id: newId,
                    sourceName,
                    targetName,
                    status: 'stopped',
                    lastSyncTime: '未同步'
                });
                
                this.$message.success('任务已创建');
            }
            
            this.filterTasks(); // 刷新筛选结果
            this.taskDialogVisible = false;
        },
        
        // 删除任务
        deleteTask(task) {
            this.$confirm(`确定要删除任务"${task.name}"吗?`, '确认删除', {
                confirmButtonText: '确定',
                cancelButtonText: '取消',
                type: 'warning'
            }).then(() => {
                const index = this.tasks.findIndex(t => t.id === task.id);
                if (index !== -1) {
                    this.tasks.splice(index, 1);
                    this.filterTasks();
                    this.$message.success('任务已删除');
                }
            }).catch(() => {
                // 取消删除
            });
        },
        
        // 分页相关方法
        handleSizeChange(val) {
            this.pageSize = val;
            this.currentPageNum = 1;
        },
        handleCurrentChange(val) {
            this.currentPageNum = val;
        },
        
        // 数据源相关方法
        openDataSourceDialog() {
            this.$message.info('打开添加数据源对话框');
        },

        testConnection(dataSource) {
            this.$message.success(`正在测试 ${dataSource.name} 的连接...`);
            // 模拟连接测试
            setTimeout(() => {
                this.$message.success(`${dataSource.name} 连接成功`);
            }, 1000);
        },

        editDataSource(dataSource) {
            this.$message.info(`编辑数据源: ${dataSource.name}`);
        },

        deleteDataSource(dataSource) {
            this.$confirm(`确定要删除数据源"${dataSource.name}"吗?`, '确认删除', {
                confirmButtonText: '确定',
                cancelButtonText: '取消',
                type: 'warning'
            }).then(() => {
                const index = this.dataSources.findIndex(db => db.id === dataSource.id);
                if (index !== -1) {
                    this.dataSources.splice(index, 1);
                    this.$message.success('数据源已删除');
                }
            }).catch(() => {
                // 取消删除
            });
        },
        
        // 日志相关方法
        viewLogDetails(log) {
            this.$message.info(`查看日志详情: ${log.taskName}`);
        },
        exportLogs() {
            this.$message.success('正在导出日志...');
            // 模拟导出
            setTimeout(() => {
                this.$message.success('日志导出成功');
            }, 1500);
        },
        
        // 系统设置相关方法
        saveSettings() {
            this.$message.success('系统设置已保存');
        },
        resetSettings() {
            this.systemSettings = {
                threadCount: 5,
                logRetentionDays: 30,
                retryCount: 3,
                enableEmailNotify: true,
                notifyEmail: 'admin@example.com',
                timeout: 30
            };
            this.$message.info('设置已重置为默认值');
        }
    }
});
