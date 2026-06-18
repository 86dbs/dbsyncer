/**
 * 订正校验结果详情页
 */
(function (window) {
    'use strict';

    function initValidateSyncDetailPage(config) {
        config = config || {};
        var currentTaskId = config.taskId || '';
        var currentDetailId = '';
        var manualReviseSubmitting = false;
        var diffModalInstance = null;

        window.backIndexPage = function () {
            doLoader('/validate-sync/list');
        };

        $('#detailTaskSelect').dbSelect({
            type: 'single',
            onSelect: function (selected) {
                var selectedId = selected.value || selected;
                if (selectedId && selectedId !== currentTaskId) {
                    currentTaskId = selectedId;
                    doLoader('/validate-sync/page/detail?id=' + currentTaskId);
                }
            }
        });

        function getFilterValue(selector) {
            var api = $(selector).data('dbSelect');
            if (api && typeof api.getValues === 'function' && api.getValues().length > 0) {
                return api.getValues()[0] || '';
            }
            return $(selector).val() || '';
        }

        function buildSearchParams() {
            return {
                taskId: currentTaskId,
                detailStatus: getFilterValue('#detailStatusFilter')
            };
        }

        function triggerSearch() {
            resultPagination.doSearch(buildSearchParams(), 1);
        }

        function initFilterSelect(selector) {
            $(selector).dbSelect({
                type: 'single',
                onSelect: function () {
                    triggerSearch();
                }
            });
        }

        initFilterSelect('#detailStatusFilter');

        var typeTextMap = {
            rowData: '行数据',
            tableSchema: '表结构',
            index: '索引'
        };
        var typeBadgeMap = {
            rowData: 'badge-primary',
            tableSchema: 'badge-info',
            index: 'badge-warning'
        };

        function parseContent(contentStr) {
            try {
                return typeof contentStr === 'string' ? JSON.parse(contentStr) : contentStr;
            } catch (e) {
                return null;
            }
        }

        function getDiffTotal(row) {
            if (row.diffTotal !== undefined && row.diffTotal !== null && row.diffTotal !== '') {
                var n = Number(row.diffTotal);
                if (n > 0) {
                    return '<span class="badge badge-error">' + n + '</span>';
                }
                return '<span class="badge badge-success">0</span>';
            }
            var content = parseContent(row.content);
            if (!content) {
                return '-';
            }
            var total = content.diffTotal || 0;
            if (total > 0) {
                return '<span class="badge badge-error">' + total + '</span>';
            }
            return '<span class="badge badge-success">0</span>';
        }

        function getCorrectedTotal(row) {
            var total = row.fixedTotal;
            if (total === undefined || total === null) {
                var content = parseContent(row.content);
                total = content ? content.correctedTotal : 0;
            }
            total = Number(total) || 0;
            if (total > 0) {
                return '<span class="badge badge-success">' + total + '</span>';
            }
            return '<span class="text-gray-400">0</span>';
        }

        function getSourceTargetCounts(row) {
            if (row.type !== 'rowData') {
                return '<span class="text-gray-400">-</span>';
            }
            var sourceTotal = row.sourceTotal;
            var targetTotal = row.targetTotal;
            if (sourceTotal === undefined && targetTotal === undefined) {
                var content = parseContent(row.content);
                if (content) {
                    sourceTotal = content.sourceTotal;
                    targetTotal = content.targetTotal;
                }
            }
            sourceTotal = (sourceTotal === undefined || sourceTotal === null) ? '-' : sourceTotal;
            targetTotal = (targetTotal === undefined || targetTotal === null) ? '-' : targetTotal;
            return '<span>源：' + sourceTotal + '</span><br><span>目：' + targetTotal + '</span>';
        }

        function parseDiffDetails(details) {
            if (details == null || details === '') {
                return [];
            }
            try {
                var parsed = typeof details === 'string' ? JSON.parse(details) : details;
                return Array.isArray(parsed) ? parsed : [];
            } catch (e) {
                return [];
            }
        }

        function formatDiffValue(value) {
            if (value === null || value === undefined || value === '') {
                return 'NULL';
            }
            return String(value);
        }

        function buildCompareDiffTable(rows, options) {
            options = options || {};
            var firstHeader = options.firstHeader || '字段';
            var firstKey = options.firstKey || 'field';
            var html = '<div class="table-inset"><table class="table table-wrap-cells">'
                + '<thead><tr>'
                + '<th class="width38p">' + escapeHtml(firstHeader) + '</th>'
                + '<th class="width31p">源</th>'
                + '<th class="width31p">目标</th>'
                + '</tr></thead><tbody>';
            rows.forEach(function (row) {
                var firstVal = row[firstKey];
                if (firstVal === undefined || firstVal === null || firstVal === '') {
                    firstVal = row.field || '-';
                }
                html += '<tr>'
                    + '<td class="font-medium">' + escapeHtml(firstVal) + '</td>'
                    + '<td class="text-primary">' + escapeHtml(formatDiffValue(row.source)) + '</td>'
                    + '<td class="text-error font-medium">' + escapeHtml(formatDiffValue(row.target)) + '</td>'
                    + '</tr>';
            });
            html += '</tbody></table></div>';
            return html;
        }

        function buildFieldDiffTable(details) {
            return buildCompareDiffTable(details, { firstHeader: '字段', firstKey: 'field' });
        }

        function parseSchemaDiffFromMsg(msg) {
            if (!msg || typeof msg !== 'string') {
                return null;
            }
            var text = msg.trim();
            var patterns = [
                { re: /^类型不一致[，,]\s*source[：:]\s*(.+?)[，,]\s*target[：:]\s*(.+)$/i, label: '类型' },
                { re: /^长度不一致[，,]\s*source[：:]\s*(.+?)[，,]\s*target[：:]\s*(.+)$/i, label: '长度' },
                { re: /^精度不一致[，,]\s*source[：:]\s*(.+?)[，,]\s*target[：:]\s*(.+)$/i, label: '精度' },
                { re: /^主键标记不一致[，,]\s*source[：:]\s*(.+?)[，,]\s*target[：:]\s*(.+)$/i, label: '主键' }
            ];
            var i;
            for (i = 0; i < patterns.length; i++) {
                var m = text.match(patterns[i].re);
                if (m) {
                    return { label: patterns[i].label, source: m[1], target: m[2] };
                }
            }
            return null;
        }

        function isSchemaDiffItem(item) {
            var type = (item && (item.type || item.objType)) || '';
            return type === '表结构' || type === 'tableSchema';
        }

        function buildDiffMsgCell(item) {
            var details = parseDiffDetails(item.details);
            var html = '<div class="table-cell-scroll">';
            if (details.length > 0) {
                html += buildFieldDiffTable(details);
            } else if (isSchemaDiffItem(item)) {
                var schemaRow = parseSchemaDiffFromMsg(item.msg);
                if (schemaRow) {
                    html += buildCompareDiffTable([schemaRow], { firstHeader: '差异项', firstKey: 'label' });
                } else {
                    html += '<div class="text-secondary">' + escapeHtml(item.msg || '-') + '</div>';
                }
            } else {
                html += '<div class="text-secondary">' + escapeHtml(item.msg || '-') + '</div>';
            }
            html += '</div>';
            return html;
        }

        function resetDiffModalActions() {
            currentDetailId = '';
        }

        function closeDiffDetailModal() {
            if (diffModalInstance) {
                diffModalInstance.close();
                diffModalInstance = null;
            }
            resetDiffModalActions();
        }

        function getDiffDetailConfirmBtn() {
            if (!diffModalInstance) {
                return null;
            }
            var dialog = diffModalInstance.getDialog();
            return dialog ? dialog.querySelector('.confirm-btn-confirm') : null;
        }

        function resolveDiffFixedCounts(row, content) {
            var diff = row.diffTotal;
            if (diff === undefined || diff === null || diff === '') {
                diff = content && content.diffTotal != null ? content.diffTotal : 0;
            }
            var fixed = row.fixedTotal;
            if (fixed === undefined || fixed === null || fixed === '') {
                fixed = content && content.correctedTotal != null ? content.correctedTotal : 0;
            }
            return {
                diff: Number(diff) || 0,
                fixed: Number(fixed) || 0
            };
        }

        function buildDiffSummaryTitleExtra(src, tgt, counts) {
            function summaryItem(label, badgeClass, value) {
                return '<div class="flex items-center gap-2">'
                    + '<label class="form-label mb-0 text-secondary">' + label + '</label>'
                    + '<span class="badge ' + badgeClass + '">' + escapeHtml(String(value)) + '</span>'
                    + '</div>';
            }
            return '<div class="grid grid-cols-4 gap-2">'
                + summaryItem('源表', 'badge-info', src)
                + summaryItem('目标', 'badge-info', tgt)
                + summaryItem('差异', 'badge-error', counts.diff)
                + summaryItem('已订正', 'badge-success', counts.fixed)
                + '</div>';
        }

        function buildDiffDetailBodyHtml(data, tableNameOuter) {
            var html = '<div class="white-space-none">';
            var headHtml = '<tr>'
                + '<th class="width18p">源表名称</th>'
                + '<th class="width10p">类型</th>'
                + '<th class="width12p">主键/标识</th>'
                + '<th class="width40p">异常</th></tr>';
            var bodyHtml = '';
            data.forEach(function (d) {
                var msgHtml = buildDiffMsgCell(d);
                var typeText = d.type || d.objType || '-';
                bodyHtml += '<tr>'
                    + '<td>' + escapeHtml(tableNameOuter) + '</td>'
                    + '<td>' + escapeHtml(typeText) + '</td>'
                    + '<td>' + escapeHtml(d.key || '-') + '</td>'
                    + '<td class="p-3">' + msgHtml + '</td>'
                    + '</tr>';
            });
            if (data.length === 0) {
                bodyHtml = '<tr><td colspan="4" class="text-center text-gray-400">无差异数据</td></tr>';
            }
            html += '<table class="table table-wrap-cells"><thead>' + headHtml + '</thead><tbody>' + bodyHtml + '</tbody></table>';
            html += '</div>';
            return html;
        }

        function openManualReviseConfirm() {
            if (!currentDetailId || manualReviseSubmitting) {
                return;
            }
            showConfirm({
                title: '确定要手动订正当前明细中尚未成功的差异数据？',
                icon: 'warning',
                size: 'large',
                position: 'center',
                confirmType: 'primary',
                onConfirm: function () {
                    manualReviseSubmitting = true;
                    var confirmBtn = getDiffDetailConfirmBtn();
                    if (confirmBtn) {
                        confirmBtn.disabled = true;
                    }
                    doPoster('/validate-sync/manualRevise', { id: currentDetailId }, function (res) {
                        manualReviseSubmitting = false;
                        if (confirmBtn) {
                            confirmBtn.disabled = false;
                        }
                        if (!res.success) {
                            bootGrowl(res.message || '手动订正失败', 'danger');
                            return;
                        }
                        bootGrowl('手动订正完成', 'success');
                        renderDiffDetailModal(res.data || {});
                        resultPagination.doSearch(buildSearchParams());
                    });
                }
            });
        }

        function renderDiffDetailModal(row) {
            var content = parseContent(row.content);
            if (!content) {
                bootGrowl('无差异数据！', 'warning');
                return;
            }
            var type = row.type || '';
            var data = content.data || [];
            var tableNameOuter = row.sourceTableName || (content.tableName || '-');
            var src = row.sourceTotal != null ? row.sourceTotal : (content.sourceTotal != null ? content.sourceTotal : 0);
            var tgt = row.targetTotal != null ? row.targetTotal : (content.targetTotal != null ? content.targetTotal : 0);
            var counts = resolveDiffFixedCounts(row, content);
            var pending = counts.diff > counts.fixed;

            closeDiffDetailModal();
            currentDetailId = row.id || '';

            diffModalInstance = showConfirm({
                title: '差异详情',
                titleExtra: type === 'rowData' ? buildDiffSummaryTitleExtra(src, tgt, counts) : '',
                icon: 'info',
                size: 'max',
                position: 'center',
                body: buildDiffDetailBodyHtml(data, tableNameOuter),
                confirmText: pending ? '手动订正(最多100条)' : '关闭',
                showCancel: pending,
                cancelText: '关闭',
                confirmType: 'primary',
                closeOnConfirm: !pending,
                onConfirm: function () {
                    if (pending) {
                        openManualReviseConfirm();
                        return;
                    }
                    diffModalInstance = null;
                    resetDiffModalActions();
                },
                onCancel: function () {
                    diffModalInstance = null;
                    resetDiffModalActions();
                }
            });
        }

        window.showDiffDetail = function (detailId) {
            if (!detailId) {
                return;
            }
            doPoster('/validate-sync/getResultDetail', { id: detailId }, function (res) {
                if (!res.success) {
                    bootGrowl(res.message || '加载失败', 'danger');
                    return;
                }
                renderDiffDetailModal(res.data || {});
            });
        };

        $(document).on('click', '.diff-detail-btn', function () {
            var id = $(this).data('detail-id');
            if (id) {
                window.showDiffDetail(String(id));
            }
        });

        var resultPagination = new PaginationManager({
            requestUrl: '/validate-sync/searchResult',
            tableBodySelector: '#detail-result-table',
            params: buildSearchParams(),
            renderRow: function (row, index) {
                var typeCode = row.type || '';
                var badgeClass = typeBadgeMap[typeCode] || 'badge-info';
                var typeText = typeTextMap[typeCode] || typeCode;
                var detailId = row.id != null ? row.id : '';
                return '<tr>'
                    + '<td>' + index + '</td>'
                    + '<td><span class="badge ' + badgeClass + '">' + escapeHtml(typeText) + '</span></td>'
                    + '<td>' + escapeHtml(row.sourceTableName || '-') + '</td>'
                    + '<td>' + escapeHtml(row.targetTableName || '-') + '</td>'
                    + '<td>' + getSourceTargetCounts(row) + '</td>'
                    + '<td>' + getDiffTotal(row) + '</td>'
                    + '<td>' + getCorrectedTotal(row) + '</td>'
                    + '<td>' + formatDate(row.updateTime || '') + '</td>'
                    + '<td><button type="button" class="table-action-btn view diff-detail-btn" title="查看详情" data-detail-id="'
                    + String(detailId).replace(/"/g, '') + '"><i class="fa fa-eye"></i></button></td>'
                    + '</tr>';
            },
            emptyHtml: '<td colspan="9" class="text-center"><i class="fa fa-file-text-o empty-icon"></i>'
                + '<p class="empty-text">暂无校验结果</p><p class="empty-description">请先执行校验任务</p></td>',
            customPageSize: true
        });
    }

    window.initValidateSyncDetailPage = initValidateSyncDetailPage;
})(window);
