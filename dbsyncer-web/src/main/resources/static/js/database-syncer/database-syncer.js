/**
 * 整库迁移 - 新增/编辑页
 */
(function () {
    const page = window.DATABASE_SYNC_PAGE || { mode: 'add', readOnly: false, task: null };
    const TABLE_PAGE_SIZE = 100;
    const TABLE_DISPLAY_MAX = 2000;
    /** 右侧表映射详情每页条数 */
    const DETAIL_PAGE_SIZE = 10;

    const state = {
        source: {
            connectorId: '',
            database: '',
            schema: '',
            tables: [],
            checked: {},
            tableMeta: {
                total: 0,
                cappedTotal: 0,
                truncated: false,
                hasMore: false,
                loading: false,
                typeCounts: {}
            },
            tableSearchKey: ''
        },
        target: { connectorId: '' },
        mappings: [],
        activeMappingIndex: -1,
        detailPageNum: 1
    };

    let sourceDbSelect = null;
    let sourceSchemaSelect = null;
    let sourceConnectorSelect = null;
    let targetConnectorSelect = null;
    let tableTreeScrollBound = false;
    let initializing = false;
    let lastSourceConnectorId = '';

    function isReadOnly() {
        return !!page.readOnly;
    }

    function isEditMode() {
        return page.mode === 'edit';
    }

    function onDBChange(connectorId, schemaSelect, dbName) {
        if (!connectorId) {
            schemaSelect.setData([]);
            return;
        }
        doGetter('/connector/getSchema', { id: connectorId, database: dbName }, function (response) {
            if (response.success && response.data) {
                const array = (response.data || []).map(function (schema) {
                    return { label: schema, value: schema, disabled: false };
                });
                schemaSelect.setData(array);
            } else {
                bootGrowl('获取 Schema 失败: ' + (response.message || ''), 'danger');
            }
        });
    }

    function onSourceConnectorChange(connectorId) {
        if (!connectorId) {
            sourceDbSelect.setData([]);
            sourceSchemaSelect.setData([]);
            state.source.connectorId = '';
            state.source.database = '';
            state.source.schema = '';
            state.source.checked = {};
            resetSourceTableList();
            renderSourceTableTreeEmpty('暂无表，请选择连接器、库与 Schema');
            clearMappings();
            return;
        }
        if (lastSourceConnectorId && lastSourceConnectorId !== connectorId && !initializing) {
            clearMappings();
        }
        lastSourceConnectorId = connectorId;
        state.source.connectorId = connectorId;
        doGetter('/connector/getDatabase', { id: connectorId }, function (response) {
            if (response.success && response.data) {
                const db = (response.data || []).map(function (dbName) {
                    return { label: dbName, value: dbName, disabled: false };
                });
                sourceDbSelect.setData(db);
            } else {
                bootGrowl('获取数据库失败: ' + (response.message || ''), 'danger');
            }
        });
    }

    function resetSourceTableList() {
        const s = state.source;
        s.tables = [];
        s.tableMeta = {
            total: 0,
            cappedTotal: 0,
            truncated: false,
            hasMore: false,
            loading: false,
            typeCounts: {}
        };
    }

    function loadSourceTables(searchKey, reset) {
        const s = state.source;
        if (!s.connectorId) {
            resetSourceTableList();
            renderSourceTableTreeEmpty('暂无表，请选择连接器、库与 Schema');
            return;
        }
        const key = searchKey != null ? searchKey : s.tableSearchKey;
        if (reset || key !== s.tableSearchKey) {
            s.tableSearchKey = key;
            resetSourceTableList();
        }
        if (s.tableMeta.loading) {
            return;
        }
        if (!reset && (!s.tableMeta.hasMore || s.tables.length >= TABLE_DISPLAY_MAX)) {
            return;
        }
        fetchSourceTablePage(reset);
    }

    function refreshTableListDisplayMeta(s, serverHasMore) {
        s.tableMeta.cappedTotal = Math.min(s.tableMeta.total, TABLE_DISPLAY_MAX);
        s.tableMeta.truncated = s.tableMeta.total > TABLE_DISPLAY_MAX;
        s.tableMeta.hasMore = !!serverHasMore && s.tables.length < s.tableMeta.cappedTotal;
    }

    function fetchSourceTablePage(reset, onComplete) {
        const s = state.source;
        const offset = reset ? 0 : s.tables.length;
        if (offset >= TABLE_DISPLAY_MAX) {
            refreshTableListDisplayMeta(s, false);
            updateTableTreeFooter(false);
            if (typeof onComplete === 'function') {
                onComplete();
            }
            return;
        }
        const limit = Math.min(TABLE_PAGE_SIZE, TABLE_DISPLAY_MAX - offset);
        s.tableMeta.loading = true;
        updateTableTreeFooter(true);
        doPoster('/database-syncer/previewTables', {
            connectorId: s.connectorId,
            database: s.database || '',
            schema: s.schema || '',
            searchKey: s.tableSearchKey || '',
            offset: offset,
            limit: limit
        }, function (res) {
            s.tableMeta.loading = false;
            if (!res.success) {
                updateTableTreeFooter(false);
                bootGrowl(res.message || '加载表失败', 'danger');
                if (typeof onComplete === 'function') {
                    onComplete();
                }
                return;
            }
            const payload = res.data || {};
            const pageItems = payload.data || [];
            s.tableMeta.total = payload.total || 0;
            s.tableMeta.typeCounts = payload.typeCounts || {};
            if (!s.tableMeta.total) {
                refreshTableListDisplayMeta(s, false);
                renderSourceTableTreeEmpty('暂无表，请检查库/Schema 或调整搜索条件');
            } else if (reset) {
                s.tables = pageItems.slice();
                refreshTableListDisplayMeta(s, payload.hasMore);
                renderSourceTableTreeShell();
                appendSourceTableRows(pageItems);
                syncFolderCheckboxState($('#sourceTableTree'));
            } else {
                s.tables = s.tables.concat(pageItems);
                refreshTableListDisplayMeta(s, payload.hasMore);
                appendSourceTableRows(pageItems);
                syncFolderCheckboxState($('#sourceTableTree'));
            }
            updateTableTreeFooter(false);
            if (typeof onComplete === 'function') {
                onComplete();
            }
        });
    }

    function ensureAllSourceTablesLoaded(callback) {
        const s = state.source;
        if (!s.tableMeta.hasMore) {
            callback();
            return;
        }
        if (s.tableMeta.loading) {
            bootGrowl('正在加载表列表，请稍候', 'warning');
            return;
        }
        fetchSourceTablePage(false, function () {
            ensureAllSourceTablesLoaded(callback);
        });
    }

    function getLoadedTableNames() {
        return (state.source.tables || []).map(function (t) {
            return t.name || '';
        }).filter(Boolean);
    }

    function setSourceTableChecked(names, checked) {
        names.forEach(function (name) {
            state.source.checked[name] = checked;
        });
    }

    function selectAllSourceTables() {
        ensureAllSourceTablesLoaded(function () {
            const names = getLoadedTableNames();
            if (!names.length) {
                bootGrowl('当前没有可选择的表', 'warning');
                return;
            }
            setSourceTableChecked(names, true);
            $('#sourceTableTree .db-sync-table-cb').prop('checked', true);
            syncFolderCheckboxState($('#sourceTableTree'));
            if (state.source.tableMeta.truncated) {
                bootGrowl('已全选展示范围内的 ' + names.length + ' 张表（最多 ' + TABLE_DISPLAY_MAX + ' 张），请用搜索缩小范围', 'info');
            }
        });
    }

    function clearAllSourceTables() {
        state.source.checked = {};
        $('#sourceTableTree .db-sync-table-cb').prop('checked', false);
    }

    function renderSourceTableTreeEmpty(message) {
        $('#sourceTableTree').html('<div class="text-xs text-tertiary p-2">' + escapeHtml(message) + '</div>');
    }

    function getTableFolderDefs() {
        return [
            { key: 'TABLE', label: 'Tables', icon: 'fa-table' },
            { key: 'VIEW', label: 'Views', icon: 'fa-eye' },
            { key: 'OTHER', label: 'Other', icon: 'fa-folder-o' }
        ];
    }

    function normalizeTableType(type) {
        const upper = (type || 'TABLE').toUpperCase();
        if (upper === 'VIEW') {
            return 'VIEW';
        }
        if (upper === 'TABLE') {
            return 'TABLE';
        }
        return 'OTHER';
    }

    function getFolderCount(folderKey) {
        const counts = state.source.tableMeta.typeCounts || {};
        if (folderKey === 'TABLE') {
            return counts.TABLE || counts.table || 0;
        }
        if (folderKey === 'VIEW') {
            return counts.VIEW || counts.view || 0;
        }
        return counts.OTHER || counts.other || 0;
    }

    function renderSourceTableTreeShell() {
        const $box = $('#sourceTableTree');
        const meta = state.source.tableMeta;
        if (!meta.total) {
            renderSourceTableTreeEmpty('暂无表，请检查库/Schema 或调整搜索条件');
            return;
        }
        const folders = getTableFolderDefs();
        let html = '<div class="picker-tree-body">';
        folders.forEach(function (folder) {
            const count = getFolderCount(folder.key);
            if (!count) {
                return;
            }
            const loadedInFolder = (state.source.tables || []).filter(function (t) {
                return normalizeTableType(t.type) === folder.key;
            });
            const folderChecked = loadedInFolder.length > 0 && loadedInFolder.every(function (t) {
                return !!state.source.checked[t.name];
            });
            const folderIndeterminate = !folderChecked && loadedInFolder.some(function (t) {
                return !!state.source.checked[t.name];
            });
            html += '<div class="picker-tree-node" data-folder-key="' + folder.key + '">'
                + '<div class="picker-tree-folder" data-folder="TABLE">'
                + '<label class="picker-tree-item picker-tree-folder-label" onclick="event.stopPropagation()">'
                + '<input type="checkbox" class="db-sync-folder-cb" data-folder-key="' + folder.key + '"'
                + (folderChecked ? ' checked' : '')
                + (folderIndeterminate ? ' data-indeterminate="1"' : '')
                + '>'
                + '<i class="fa fa-caret-down"></i><i class="fa ' + folder.icon + '"></i> '
                + folder.label + ' (' + count + ')</label></div>'
                + '<div class="picker-tree-children" data-folder-key="' + folder.key + '"></div>'
                + '</div>';
        });
        html += '</div><div class="picker-scroll-footer" id="sourceTableTreeFooter"></div>';
        $box.html(html);

        $box.find('.db-sync-folder-cb[data-indeterminate="1"]').each(function () {
            this.indeterminate = true;
        });

        bindTableTreeScroll();
        updateTableTreeFooter(false);
    }

    function buildTableRowHtml(t) {
        const name = t.name || '';
        const ck = state.source.checked[name] ? ' checked' : '';
        const safeName = String(name).replace(/"/g, '&quot;');
        return '<label class="picker-tree-item">'
            + '<input type="checkbox" class="db-sync-table-cb" data-name="' + safeName + '"' + ck + '>'
            + '<span>' + escapeHtml(name) + '</span></label>';
    }

    function appendSourceTableRows(items) {
        if (!items || !items.length) {
            return;
        }
        const grouped = { TABLE: [], VIEW: [], OTHER: [] };
        items.forEach(function (t) {
            grouped[normalizeTableType(t.type)].push(t);
        });
        Object.keys(grouped).forEach(function (folderKey) {
            const list = grouped[folderKey];
            if (!list.length) {
                return;
            }
            const $children = $('#sourceTableTree .picker-tree-children[data-folder-key="' + folderKey + '"]');
            if (!$children.length) {
                return;
            }
            let html = '';
            list.forEach(function (t) {
                html += buildTableRowHtml(t);
            });
            $children.append(html);
        });
    }

    function updateTableTreeFooter(loading) {
        const $footer = $('#sourceTableTreeFooter');
        if (!$footer.length) {
            return;
        }
        const meta = state.source.tableMeta;
        if (loading) {
            $footer.addClass('is-loading').html('<i class="fa fa-spinner fa-spin"></i> 加载中...');
            return;
        }
        $footer.removeClass('is-loading');
        if (!meta.total) {
            $footer.html('');
            return;
        }
        const loaded = state.source.tables.length;
        let text = '已加载 ' + loaded + ' / ' + (meta.cappedTotal || meta.total) + ' 张表';
        if (meta.hasMore) {
            text += '，向下滚动加载更多';
        }
        if (meta.truncated) {
            text += '（共 ' + meta.total + ' 张，最多展示 ' + TABLE_DISPLAY_MAX + ' 张，请用搜索缩小范围）';
        }
        $footer.html(text);
    }

    function bindTableTreeScroll() {
        if (tableTreeScrollBound) {
            return;
        }
        tableTreeScrollBound = true;
        $('#sourceTableTree').on('scroll.dbSyncTables', function () {
            const el = this;
            const nearBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 24;
            if (nearBottom) {
                loadSourceTables(null, false);
            }
        });
    }

    function bindTableTreeEvents() {
        const $box = $('#sourceTableTree');

        $box.off('click.pickerFolder').on('click.pickerFolder', '.picker-tree-folder', function (e) {
            if ($(e.target).closest('.picker-tree-folder-label').length) {
                return;
            }
            const $icon = $(this).find('.fa-caret-down, .fa-caret-right').first();
            $(this).next('.picker-tree-children').toggle();
            $icon.toggleClass('fa-caret-down fa-caret-right');
        });

        $box.off('change.tableCb').on('change.tableCb', '.db-sync-table-cb', function () {
            const name = $(this).data('name');
            state.source.checked[name] = this.checked;
            syncFolderCheckboxState($box);
        });

        $box.off('change.folderCb').on('change.folderCb', '.db-sync-folder-cb', function () {
            const folderKey = $(this).data('folder-key');
            const checked = this.checked;
            const list = (state.source.tables || []).filter(function (t) {
                return normalizeTableType(t.type) === folderKey;
            });
            list.forEach(function (t) {
                const name = t.name || '';
                state.source.checked[name] = checked;
            });
            const $node = $(this).closest('.picker-tree-node');
            $node.find('.db-sync-table-cb').prop('checked', checked);
            $(this).prop('indeterminate', false).removeAttr('data-indeterminate');
        });
    }

    function syncFolderCheckboxState($box) {
        $box.find('.picker-tree-node').each(function () {
            const $folderCb = $(this).find('.db-sync-folder-cb').first();
            const $tableCbs = $(this).find('.db-sync-table-cb');
            if (!$folderCb.length || !$tableCbs.length) {
                return;
            }
            const total = $tableCbs.length;
            const checkedCount = $tableCbs.filter(':checked').length;
            $folderCb.prop('checked', checkedCount === total);
            $folderCb.prop('indeterminate', checkedCount > 0 && checkedCount < total);
        });
    }

    function getCheckedSourceTableNames() {
        const names = [];
        Object.keys(state.source.checked).forEach(function (name) {
            if (state.source.checked[name]) {
                names.push(name);
            }
        });
        return names;
    }

    /**
     * 按当前列表顺序为库映射、表映射生成连续序号（从 1 开始），保存时与后端一致。
     */
    function reindexMappings() {
        state.mappings.forEach(function (block, dbIdx) {
            block.index = dbIdx + 1;
            (block.tableMappings || []).forEach(function (row, tblIdx) {
                row.index = tblIdx + 1;
            });
        });
    }

    function syncMappingsJson() {
        reindexMappings();
        $('#databaseMappingsJson').val(JSON.stringify(state.mappings));
    }

    function findMappingIndex(sourceDatabase, sourceSchema) {
        const schema = sourceSchema || '';
        return state.mappings.findIndex(function (m) {
            return m.sourceDatabase === sourceDatabase && (m.sourceSchema || '') === schema;
        });
    }

    function openTablePickerModal() {
        if (isReadOnly()) {
            return;
        }
        if (!state.source.connectorId || !state.target.connectorId) {
            bootGrowl('请先选择源端、目标端连接器', 'warning');
            return;
        }
        $('#tablePickerModal').removeClass('hidden');
    }

    function closeTablePickerModal() {
        $('#tablePickerModal').addClass('hidden');
    }

    /**
     * 无库映射时清空右侧表映射展示区
     */
    function resetMappingDetailView() {
        state.activeMappingIndex = -1;
        state.detailPageNum = 1;
        $('#mappingDetailPanel').addClass('hidden');
        $('#mappingDetailTableCount').text('0');
        $('#mappingDetailTableBody').empty();
        $('#mappingDetailPagination').addClass('hidden');
    }

    function renderMappingSidebar() {
        const $list = $('#mappingCardList');
        const $empty = $('#mappingSidebarEmpty');
        if (!state.mappings.length) {
            $list.empty();
            $empty.removeClass('hidden');
            resetMappingDetailView();
            syncMappingsJson();
            return;
        }
        $empty.addClass('hidden');
        if (state.activeMappingIndex < 0 || state.activeMappingIndex >= state.mappings.length) {
            state.activeMappingIndex = 0;
        }
        let html = '';
        state.mappings.forEach(function (block, idx) {
            const count = (block.tableMappings || []).length;
            const active = idx === state.activeMappingIndex ? ' is-active' : '';
            html += '<div class="mapping-card' + active + '" data-index="' + idx + '">';
            if (!isReadOnly()) {
                html += '<button type="button" class="mapping-card-remove" data-index="' + idx + '" title="删除">'
                    + '<i class="fa fa-times"></i></button>';
            }
            html += '<div class="mapping-card-row">'
                + '<span class="mapping-card-label">源库</span>'
                + '<span class="mapping-card-value" title="' + escapeHtml(block.sourceDatabase || '') + '">'
                + escapeHtml(block.sourceDatabase || '') + '</span></div>'
                + '<div class="mapping-card-row">'
                + '<span class="mapping-card-label">目标</span>'
                + '<input type="text" class="form-control form-control-sm mapping-card-target-input mapping-card-tgt-db"'
                + ' data-index="' + idx + '" value="' + escapeHtml(block.targetDatabase || '') + '"'
                + (isReadOnly() ? ' readonly' : '') + '/>'
                + '</div>'
                + '<div class="mapping-card-footer">#' + (block.index || (idx + 1)) + ' · 共 ' + count + ' 个对象</div>'
                + '</div>';
        });
        $list.html(html);
        bindMappingSidebarEvents();
        syncMappingsJson();
    }

    function bindMappingSidebarEvents() {
        $('#mappingCardList .mapping-card').off('click.selectCard').on('click.selectCard', function (e) {
            if ($(e.target).closest('.mapping-card-remove, .mapping-card-tgt-db').length) {
                return;
            }
            const idx = Number($(this).data('index'));
            state.activeMappingIndex = idx;
            renderMappingSidebar();
            renderMappingDetail(true);
        });

        $('.mapping-card-remove').off('click.removeCard').on('click.removeCard', function (e) {
            e.stopPropagation();
            const idx = Number($(this).data('index'));
            state.mappings.splice(idx, 1);
            if (!state.mappings.length) {
                renderMappingSidebar();
                return;
            }
            if (state.activeMappingIndex === idx) {
                state.activeMappingIndex = Math.min(idx, state.mappings.length - 1);
            } else if (state.activeMappingIndex > idx) {
                state.activeMappingIndex--;
            }
            renderMappingSidebar();
            renderMappingDetail(true);
        });

        $('.mapping-card-tgt-db').off('change.cardTgtDb').on('change.cardTgtDb', function () {
            const idx = Number($(this).data('index'));
            if (!state.mappings[idx]) {
                return;
            }
            state.mappings[idx].targetDatabase = $(this).val();
            syncMappingsJson();
        });
    }

    function buildDetailTableRowHtml(block, row, rowIndex, blockIndex) {
        let html = '<tr data-r="' + rowIndex + '">'
            + '<td class="text-center text-tertiary">' + (row.index || (rowIndex + 1)) + '</td>'
            + '<td class="text-primary">' + escapeHtml(row.sourceTable || '') + '</td>';
        if (isReadOnly()) {
            html += '<td>' + escapeHtml(row.targetTable || '') + '</td>';
        } else {
            html += '<td><input type="text" class="form-control form-control-sm db-tgt-tbl"'
                + ' data-b="' + blockIndex + '" data-r="' + rowIndex + '" value="' + escapeHtml(row.targetTable || '') + '"/></td>'
                + '<td><button type="button" class="table-action-btn delete btn-rm-detail-row"'
                + ' data-b="' + blockIndex + '" data-r="' + rowIndex + '" title="删除">'
                + '<i class="fa fa-times"></i></button></td>';
        }
        return html + '</tr>';
    }

    function getDetailDisplayRows(block) {
        const rows = block.tableMappings || [];
        return rows.slice(0, TABLE_DISPLAY_MAX);
    }

    function renderDetailPagination(currentPage, totalPages, total) {
        const $pagination = $('#mappingDetailPagination');
        $pagination.find('.mappingDetailTotalCount').text(total);
        $pagination.find('.mappingDetailCurrentPage').text(currentPage);
        $pagination.find('.mappingDetailTotalPages').text(totalPages);
        const $bar = $pagination.find('.pagination-bar');
        $bar.empty();
        if (total <= 0) {
            $pagination.addClass('hidden');
            return;
        }
        $pagination.removeClass('hidden');

        const prevBtn = $('<button type="button" class="pagination-btn"' + (currentPage === 1 ? ' disabled' : '') + '>'
            + '<i class="fa fa-angle-left"></i></button>');
        if (currentPage > 1) {
            prevBtn.on('click', function () {
                renderDetailTablePage(currentPage - 1);
            });
        }
        $bar.append(prevBtn);

        const startPage = Math.max(1, currentPage - 1);
        const endPage = Math.min(totalPages, startPage + 2);
        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = $('<button type="button" class="pagination-btn' + (i === currentPage ? ' active' : '') + '">' + i + '</button>');
            if (i !== currentPage) {
                pageBtn.on('click', function () {
                    renderDetailTablePage(i);
                });
            }
            $bar.append(pageBtn);
        }

        const nextBtn = $('<button type="button" class="pagination-btn"' + (currentPage === totalPages ? ' disabled' : '') + '>'
            + '<i class="fa fa-angle-right"></i></button>');
        if (currentPage < totalPages) {
            nextBtn.on('click', function () {
                renderDetailTablePage(currentPage + 1);
            });
        }
        $bar.append(nextBtn);
    }

    function renderDetailTablePage(pageNum) {
        const idx = state.activeMappingIndex;
        const block = state.mappings[idx];
        if (!block) {
            return;
        }
        const rows = getDetailDisplayRows(block);
        const total = rows.length;
        const totalPages = Math.ceil(total / DETAIL_PAGE_SIZE) || 1;
        const page = Math.min(Math.max(1, pageNum), totalPages);
        state.detailPageNum = page;
        const from = (page - 1) * DETAIL_PAGE_SIZE;
        let html = '';
        for (let i = from; i < Math.min(from + DETAIL_PAGE_SIZE, total); i++) {
            html += buildDetailTableRowHtml(block, rows[i], i, idx);
        }
        if (!html) {
            const colSpan = isReadOnly() ? 3 : 4;
            html = '<tr><td colspan="' + colSpan + '" class="text-center text-tertiary py-4">暂无表映射</td></tr>';
        }
        $('#mappingDetailTableBody').html(html);
        renderDetailPagination(page, totalPages, total);
        bindMappingDetailEvents();
    }

    function renderMappingDetail(reset) {
        const idx = state.activeMappingIndex;
        const block = state.mappings[idx];
        if (!block) {
            resetMappingDetailView();
            return;
        }
        $('#mappingDetailPanel').removeClass('hidden');
        const count = (block.tableMappings || []).length;
        $('#mappingDetailTableCount').text(count);
        if (reset) {
            state.detailPageNum = 1;
        }
        renderDetailTablePage(reset ? 1 : state.detailPageNum);
    }

    function bindMappingDetailEvents() {
        $('#mappingDetailTableBody .db-tgt-tbl').off('change.detailTgtTbl').on('change.detailTgtTbl', function () {
            const b = Number($(this).data('b'));
            const r = Number($(this).data('r'));
            const row = state.mappings[b] && state.mappings[b].tableMappings && state.mappings[b].tableMappings[r];
            if (row) {
                row.targetTable = $(this).val();
                syncMappingsJson();
            }
        });

        $('.btn-rm-detail-row').off('click.rmDetailRow').on('click.rmDetailRow', function () {
            const b = Number($(this).data('b'));
            const r = Number($(this).data('r'));
            const block = state.mappings[b];
            if (!block || !block.tableMappings) {
                return;
            }
            block.tableMappings.splice(r, 1);
            if (!block.tableMappings.length) {
                state.mappings.splice(b, 1);
                if (!state.mappings.length) {
                    renderMappingSidebar();
                    syncMappingsJson();
                    return;
                }
                if (state.activeMappingIndex >= state.mappings.length) {
                    state.activeMappingIndex = state.mappings.length - 1;
                } else if (state.activeMappingIndex > b) {
                    state.activeMappingIndex--;
                } else if (state.activeMappingIndex === b) {
                    state.activeMappingIndex = Math.min(b, state.mappings.length - 1);
                }
                renderMappingSidebar();
                renderMappingDetail(true);
            } else {
                renderMappingSidebar();
                const totalPages = Math.ceil(getDetailDisplayRows(block).length / DETAIL_PAGE_SIZE) || 1;
                if (state.detailPageNum > totalPages) {
                    state.detailPageNum = totalPages;
                }
                renderMappingDetail(false);
            }
            syncMappingsJson();
        });
    }

    function addDatabaseMapping() {
        const sourceTables = getCheckedSourceTableNames();
        if (!state.source.connectorId || !state.target.connectorId) {
            bootGrowl('请先选择源端、目标端连接器', 'warning');
            return;
        }
        if (!state.source.database) {
            bootGrowl('请先选择源库', 'warning');
            return;
        }
        if (!sourceTables.length) {
            bootGrowl('请至少勾选一张源表，可使用「全选」', 'warning');
            return;
        }
        const sourceDb = state.source.database;
        const sourceSchema = state.source.schema || '';
        const defaultTargetDb = sourceDb;
        const existIdx = findMappingIndex(sourceDb, sourceSchema);
        const newRows = sourceTables.map(function (srcName) {
            return { sourceTable: srcName, targetTable: srcName };
        });
        if (existIdx >= 0) {
            const exist = state.mappings[existIdx];
            const names = {};
            (exist.tableMappings || []).forEach(function (r) {
                names[r.sourceTable] = true;
            });
            newRows.forEach(function (r) {
                if (!names[r.sourceTable]) {
                    exist.tableMappings.push(r);
                }
            });
            state.activeMappingIndex = existIdx;
        } else {
            state.mappings.push({
                sourceDatabase: sourceDb,
                sourceSchema: sourceSchema,
                targetDatabase: defaultTargetDb,
                tableMappings: newRows
            });
            state.activeMappingIndex = state.mappings.length - 1;
        }
        state.source.checked = {};
        closeTablePickerModal();
        renderMappingSidebar();
        renderMappingDetail(true);
        bootGrowl('已添加库映射', 'success');
    }

    function clearMappings() {
        state.mappings = [];
        renderMappingSidebar();
    }

    function initEditFromTask(task) {
        if (!task) {
            return;
        }
        initializing = true;
        state.mappings = JSON.parse(JSON.stringify(task.databaseMappings || []));
        reindexMappings();
        lastSourceConnectorId = task.sourceConnectorId || '';
        state.source.connectorId = lastSourceConnectorId;
        state.target.connectorId = task.targetConnectorId || '';
        state.activeMappingIndex = state.mappings.length ? 0 : -1;

        const $form = $('#database-syncer-form');
        $form.find('input[name="name"]').val(task.name || '');
        $form.find('input[name="id"]').val(task.id || '');

        if (sourceConnectorSelect && task.sourceConnectorId) {
            sourceConnectorSelect.setValues([task.sourceConnectorId]);
        }
        if (targetConnectorSelect && task.targetConnectorId) {
            targetConnectorSelect.setValues([task.targetConnectorId]);
        }

        renderMappingSidebar();
        if (state.mappings.length) {
            renderMappingDetail(true);
        }
        initializing = false;
    }

    function bindSyncStrategyEvents() {
        $('.db-sync-strategy-enable').each(function () {
            const $enable = $(this);
            const overwriteName = $enable.data('overwrite');
            const $overwrite = $('input[name="' + overwriteName + '"]');
            if (!$overwrite.length) {
                return;
            }
            function refresh() {
                const on = $enable.is(':checked');
                $overwrite.prop('disabled', !on || isReadOnly());
                if (!on) {
                    $overwrite.prop('checked', false);
                }
            }
            $enable.off('change.syncStrategy').on('change.syncStrategy', refresh);
            refresh();
        });
    }

    function initSyncStrategyFromTask(task) {
        if (!task) {
            return;
        }
        $('input[name="enableCopySchema"]').prop('checked', !!task.enableCopySchema);
        $('input[name="overwriteSchema"]').prop('checked', !!task.overwriteSchema);
        $('input[name="enableCopyData"]').prop('checked', !!task.enableCopyData);
        $('input[name="overwriteData"]').prop('checked', !!task.overwriteData);
        bindSyncStrategyEvents();
    }

    $(document).ready(function () {
        window.backIndexPage = function () {
            doLoader('/database-syncer/list');
        };

        bindSyncStrategyEvents();

        sourceSchemaSelect = $('#sourceSchema').dbSelect({
            type: 'single',
            onSelect: function (selected) {
                state.source.schema = selected.length ? selected[0] : '';
                loadSourceTables($('#sourceTableSearch').val().trim(), true);
            }
        });

        sourceDbSelect = $('#sourceDatabase').dbSelect({
            type: 'single',
            onSelect: function (selected) {
                state.source.database = selected.length ? selected[0] : '';
                if (state.source.connectorId) {
                    onDBChange(state.source.connectorId, sourceSchemaSelect, state.source.database);
                }
                loadSourceTables(null, true);
            }
        });

        sourceConnectorSelect = $('#sourceConnectorId').dbSelect({
            type: 'single',
            disabled: isReadOnly(),
            onSelect: function (ids) {
                onSourceConnectorChange(ids.length ? ids[0] : '');
            }
        });

        targetConnectorSelect = $('#targetConnectorId').dbSelect({
            type: 'single',
            disabled: isReadOnly(),
            onSelect: function (ids) {
                state.target.connectorId = ids.length ? ids[0] : '';
            }
        });

        let srcSearchTimer = null;
        $('#sourceTableSearch').on('input', function () {
            clearTimeout(srcSearchTimer);
            const key = $(this).val().trim();
            srcSearchTimer = setTimeout(function () { loadSourceTables(key, true); }, 300);
        });

        bindTableTreeScroll();
        bindTableTreeEvents();

        $('#btnSelectAllTables').on('click', selectAllSourceTables);
        $('#btnClearAllTables').on('click', clearAllSourceTables);
        $('#btnAddDbMapping').on('click', openTablePickerModal);
        $('#btnConfirmTablePicker').on('click', addDatabaseMapping);
        $('#btnCancelTablePicker, #btnCloseTablePicker, #tablePickerModalBackdrop').on('click', closeTablePickerModal);

        $('#database-syncer-submit-btn').on('click', function () {
            syncMappingsJson();
            const $form = $('#database-syncer-form');
            if (!validateForm($form)) {
                return;
            }
            if (!state.mappings.length) {
                bootGrowl('请至少添加一组库映射', 'warning');
                return;
            }
            const btn = $(this);
            const original = btn.html();
            const saveUrl = isEditMode() ? '/database-syncer/edit' : '/database-syncer/add';
            btn.html('<i class="fa fa-spinner fa-spin"></i> 保存中...').prop('disabled', true);
            doPoster(saveUrl, $form.serializeJson(), function (response) {
                btn.html(original).prop('disabled', false);
                if (response.success) {
                    bootGrowl(isEditMode() ? '修改成功' : '保存成功', 'success');
                    backIndexPage();
                } else {
                    bootGrowl(response.message || '保存失败', 'danger');
                }
            });
        });

        if (isEditMode() && page.task) {
            initSyncStrategyFromTask(page.task);
            initEditFromTask(page.task);
        } else {
            renderMappingSidebar();
        }
    });
})();
