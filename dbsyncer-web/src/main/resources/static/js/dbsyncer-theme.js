/**
 * DBsyncer 主题交互脚本
 * 处理新UI的交互逻辑
 */

(function($) {
    'use strict';

    // 初始化主题
    function initTheme() {
        initSidebar();
        initNavigation();
        initCharts();
        initTimeFilters();
        initResponsive();
    }

    // 初始化侧边栏
    function initSidebar() {
        // 侧边栏折叠/展开
        $('.dbsyncer-sidebar-toggle').on('click', function() {
            $('.dbsyncer-sidebar').toggleClass('collapsed');
            $('.dbsyncer-main').toggleClass('sidebar-collapsed');
        });

        // 移动端侧边栏
        $('.dbsyncer-sidebar-overlay').on('click', function() {
            $('.dbsyncer-sidebar').removeClass('mobile-open');
            $('.dbsyncer-sidebar-overlay').removeClass('show');
        });

        // 导航链接点击
        $('.dbsyncer-nav-link').on('click', function(e) {
            e.preventDefault();
            
            // 移除所有活动状态
            $('.dbsyncer-nav-link').removeClass('active');
            
            // 添加当前活动状态
            $(this).addClass('active');
            
            // 移动端自动关闭侧边栏
            if ($(window).width() <= 768) {
                $('.dbsyncer-sidebar').removeClass('mobile-open');
                $('.dbsyncer-sidebar-overlay').removeClass('show');
            }
        });
    }

    // 初始化导航
    function initNavigation() {
        // 搜索框功能
        $('.dbsyncer-search-input').on('input', function() {
            var query = $(this).val();
            if (query.length > 2) {
                // 这里可以添加搜索逻辑
                console.log('搜索:', query);
            }
        });

        // 用户下拉菜单
        $('.dbsyncer-user-info').on('click', function(e) {
            e.preventDefault();
            $(this).next('.dropdown-menu').toggle();
        });

        // 点击外部关闭下拉菜单
        $(document).on('click', function(e) {

            if (!$(e.target).closest('.dbsyncer-user-info').length) {
                $('.dropdown-menu').hide();
            }
        });

        // 新导航链接点击事件
        $('.dbsyncer-nav-link[url]').on('click', function(e) {
            e.preventDefault();
            var url = $(this).attr('url');
            var route = $(this).data('route');
            
            // 清理定时器 - 只有在离开index页面时才清理
            if (url !== '/index' && url !== '/') {
                if (typeof timer !== 'undefined' && timer != null) {
                    clearInterval(timer);
                    timer = null;
                }
                if (typeof timer2 !== 'undefined' && timer2 != null) {
                    clearInterval(timer2);
                    timer2 = null;
                }
            }
            
            // 更新活动状态
            $('.dbsyncer-nav-link').removeClass('active');
            $(this).addClass('active');
            
            // 加载页面
            loadPage(url, route);
        });
    }

    // 加载页面
    function loadPage(url, route) {
        // 使用统一的内容区域
        var contentDiv = $('#mainContent');
        if (contentDiv.length) {
            // 显示加载状态
            showLoading(contentDiv);
            
            // 加载页面内容
            if (typeof doLoader === 'function') {
                // 使用doLoader函数加载内容
                contentDiv.load($basePath + url, function(response, status, xhr) {
                    if (status === 'success') {
                        hideLoading(contentDiv);
                        // 执行水印等初始化操作
                        if (typeof watermark === 'function') {
                            watermark();
                        }
                    } else {
                        hideLoading(contentDiv);
                        if (typeof bootGrowl === 'function') {
                            bootGrowl('页面加载失败', 'danger');
                        } else {
                            alert('页面加载失败，请稍后重试');
                        }
                    }
                });
            } else {
                // 如果没有doLoader函数，直接跳转
                window.location.href = url;
            }
        }
    }

    // 初始化图表
    function initCharts() {
        // 同步趋势图表
        if (typeof echarts !== 'undefined' && $('#trendChart').length) {
            var trendChart = echarts.init(document.getElementById('trendChart'));
            var trendOption = {
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'cross'
                    }
                },
                legend: {
                    data: ['同步数据量 (MB)', '同步次数']
                },
                xAxis: {
                    type: 'category',
                    data: ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
                },
                yAxis: [
                    {
                        type: 'value',
                        name: '数据量 (MB)',
                        position: 'left'
                    },
                    {
                        type: 'value',
                        name: '同步次数',
                        position: 'right'
                    }
                ],
                series: [
                    {
                        name: '同步数据量 (MB)',
                        type: 'line',
                        data: [1200, 1800, 2200, 2100, 1900, 1600, 1400],
                        smooth: true,
                        itemStyle: {
                            color: '#1890ff'
                        }
                    },
                    {
                        name: '同步次数',
                        type: 'line',
                        yAxisIndex: 1,
                        data: [45, 52, 48, 55, 50, 42, 38],
                        smooth: true,
                        itemStyle: {
                            color: '#52c41a'
                        }
                    }
                ]
            };
            trendChart.setOption(trendOption);

            // 响应式调整
            $(window).on('resize', function() {
                trendChart.resize();
            });
        }

        // 任务状态分布图表
        if (typeof echarts !== 'undefined' && $('#statusChart').length) {
            var statusChart = echarts.init(document.getElementById('statusChart'));
            var statusOption = {
                tooltip: {
                    trigger: 'item',
                    formatter: '{a} <br/>{b}: {c} ({d}%)'
                },
                series: [
                    {
                        name: '任务状态',
                        type: 'pie',
                        radius: ['40%', '70%'],
                        center: ['50%', '50%'],
                        data: [
                            { value: 18, name: '运行中', itemStyle: { color: '#52c41a' } },
                            { value: 4, name: '暂停中', itemStyle: { color: '#fa8c16' } },
                            { value: 2, name: '失败', itemStyle: { color: '#f5222d' } }
                        ],
                        emphasis: {
                            itemStyle: {
                                shadowBlur: 10,
                                shadowOffsetX: 0,
                                shadowColor: 'rgba(0, 0, 0, 0.5)'
                            }
                        }
                    }
                ]
            };
            statusChart.setOption(statusOption);

            // 响应式调整
            $(window).on('resize', function() {
                statusChart.resize();
            });
        }

        // 系统资源图表
        if (typeof echarts !== 'undefined' && $('#resourceChart').length) {
            var resourceChart = echarts.init(document.getElementById('resourceChart'));
            var resourceOption = {
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'cross'
                    }
                },
                legend: {
                    data: ['CPU使用率', '内存使用率', '磁盘使用率']
                },
                xAxis: {
                    type: 'category',
                    data: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00', '24:00']
                },
                yAxis: {
                    type: 'value',
                    name: '使用率 (%)',
                    min: 0,
                    max: 100
                },
                series: [
                    {
                        name: 'CPU使用率',
                        type: 'line',
                        data: [45, 52, 68, 75, 68, 55, 48],
                        smooth: true,
                        itemStyle: { color: '#fa8c16' }
                    },
                    {
                        name: '内存使用率',
                        type: 'line',
                        data: [60, 65, 72, 78, 72, 68, 62],
                        smooth: true,
                        itemStyle: { color: '#fa8c16' }
                    },
                    {
                        name: '磁盘使用率',
                        type: 'line',
                        data: [30, 32, 35, 35, 35, 34, 33],
                        smooth: true,
                        itemStyle: { color: '#52c41a' }
                    }
                ]
            };
            resourceChart.setOption(resourceOption);

            // 响应式调整
            $(window).on('resize', function() {
                resourceChart.resize();
            });
        }
    }

    // 初始化时间筛选器
    function initTimeFilters() {
        $('.dbsyncer-time-filter-btn').on('click', function() {
            $('.dbsyncer-time-filter-btn').removeClass('active');
            $(this).addClass('active');
            
            var period = $(this).text();
            console.log('选择时间周期:', period);
            
            // 这里可以添加时间筛选逻辑
            // 例如：重新加载图表数据
        });

        $('.dbsyncer-trend-filter-btn').on('click', function() {
            $('.dbsyncer-trend-filter-btn').removeClass('active');
            $(this).addClass('active');
            
            var period = $(this).text();
            console.log('选择趋势周期:', period);
            
            // 这里可以添加趋势图表数据更新逻辑
        });
    }

    // 初始化响应式
    function initResponsive() {
        function handleResize() {
            var windowWidth = $(window).width();
            
            if (windowWidth <= 768) {
                // 移动端处理
                $('.dbsyncer-sidebar').removeClass('collapsed');
            } else {
                // 桌面端处理
                $('.dbsyncer-sidebar-overlay').removeClass('show');
            }
        }

        // 初始检查
        handleResize();
        
        // 窗口大小改变时检查
        $(window).on('resize', handleResize);
    }

    // 工具函数
    function showLoading(element) {
        $(element).html('<div class="dbsyncer-loading"><div class="dbsyncer-loading-spinner"></div>加载中...</div>');
    }

    function hideLoading(element) {
        $(element).find('.dbsyncer-loading').remove();
    }

    function showEmpty(element, message) {
        $(element).html('<div class="dbsyncer-empty"><div class="dbsyncer-empty-icon"><i class="fa fa-inbox"></i></div><div class="dbsyncer-empty-text">' + (message || '暂无数据') + '</div></div>');
    }

    // 导出到全局
    window.DBSyncerTheme = {
        init: initTheme,
        showLoading: showLoading,
        hideLoading: hideLoading,
        showEmpty: showEmpty
    };

    // 页面加载完成后初始化
    $(document).ready(function() {
        initTheme();
    });

})(jQuery);
