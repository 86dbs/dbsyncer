
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

// 搜索功能
(function() {
    const searchInput = document.getElementById('searchInput');
    const searchClear = document.getElementById('searchClear');
    
    if (!searchInput) return;
    
    // 防抖函数
    function debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
    
    // 搜索过滤函数
    function performSearch(keyword) {
        const keywordLower = keyword.toLowerCase().trim();
        
        // 过滤连接列表
        const connectionCards = document.querySelectorAll('.form-container.lg\\:col-span-1 .card.flex.items-center');
        connectionCards.forEach(card => {
            const cardText = card.textContent.toLowerCase();
            if (keywordLower === '' || cardText.includes(keywordLower)) {
                card.style.display = 'flex';
            } else {
                card.style.display = 'none';
            }
        });
        
        // 过滤驱动表格
        const tableRows = document.querySelectorAll('.table tbody tr');
        tableRows.forEach(row => {
            const rowText = row.textContent.toLowerCase();
            if (keywordLower === '' || rowText.includes(keywordLower)) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
            }
        });
        
        // 更新显示状态
        updateSearchState(keyword);
    }
    
    // 更新搜索状态（显示/隐藏清除按钮）
    function updateSearchState(keyword) {
        if (keyword && keyword.trim() !== '') {
            searchClear.classList.add('active');
        } else {
            searchClear.classList.remove('active');
        }
    }
    
    // 清除搜索
    function clearSearch() {
        searchInput.value = '';
        performSearch('');
        searchInput.focus();
    }
    
    // 防抖搜索函数（300ms延迟）
    const debouncedSearch = debounce(performSearch, 300);
    
    // 绑定事件
    searchInput.addEventListener('input', function(e) {
        const value = e.target.value;
        // 立即更新清除按钮显示状态
        updateSearchState(value);
        // 防抖执行搜索过滤
        debouncedSearch(value);
    });
    
    searchClear.addEventListener('click', clearSearch);
    
    // 回车键搜索
    searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            performSearch(e.target.value);
        }
    });
    
    // 初始化状态
    updateSearchState('');
})();