
// 初始化图表
// 同步趋势图表
const trendCtx = document.getElementById('syncTrendChart').getContext('2d');
const trendChart = new Chart(trendCtx, {
    type: 'line',
    data: {
        labels: ['周一', '周二', '周三', '周四', '周五', '周六', '周日'],
        datasets: [
            {
                label: '同步数据量 (MB)',
                data: [1250, 1900, 1750, 2400, 2100, 1800, 2300],
                borderColor: '#165DFF',
                backgroundColor: 'rgba(22, 93, 255, 0.1)',
                tension: 0.3,
                fill: true
            },
            {
                label: '同步次数',
                data: [32, 45, 38, 52, 48, 36, 42],
                borderColor: '#36CFC9',
                backgroundColor: 'transparent',
                tension: 0.3,
                yAxisID: 'y1'
            }
        ]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            mode: 'index',
            intersect: false,
        },
        scales: {
            y: {
                beginAtZero: true,
                title: {
                    display: true,
                    text: '数据量 (MB)'
                }
            },
            y1: {
                beginAtZero: true,
                position: 'right',
                title: {
                    display: true,
                    text: '同步次数'
                },
                grid: {
                    drawOnChartArea: false
                }
            }
        }
    }
});

// 任务状态图表
const statusCtx = document.getElementById('taskStatusChart').getContext('2d');
const statusChart = new Chart(statusCtx, {
    type: 'doughnut',
    data: {
        labels: ['运行中', '暂停中', '已失败'],
        datasets: [{
            data: [18, 4, 2],
            backgroundColor: [
                '#52C41A',
                '#FAAD14',
                '#FF4D4F'
            ],
            borderWidth: 0,
            hoverOffset: 4
        }]
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '70%',
        plugins: {
            legend: {
                display: false
            }
        }
    }
});