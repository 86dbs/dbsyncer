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
            tableSearchKey: '',
            /** 当前连接器在选定库后是否提供 Schema 列表（由 getSchema 接口决定） */
            schemaEnabled: false
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
    let pickerTargetConnectorSelect = null;
    /** 页面加载时的全部连接器选项，用于按源端类型筛选目标连接器 */
    let allConnectorOptions = [];
    /** 暂不支持作为源端的连接器类型 */
    const UNSUPPORTED_SOURCE_CONNECTOR_TYPES = [];
    let tableTreeScrollBound = false;
    let tableCbSeq = 0;
    let initializing = false;
    let lastSourceConnectorId = '';
    let detailPanelBound = false;
    let tablePickerModal = null;
    let tablePickerSearchTimer = null;
    /** add=新建库映射；append=向当前库映射追加表 */
    let tablePickerMode = 'add';
    /** 程序化刷新连接器选项时忽略 onSelect，避免 setData 自动选中首项覆盖映射数据 */
    let suppressConnectorSelect = false;

    function isReadOnly() {
        return !!page.readOnly;
    }

    function isEditMode() {
        return page.mode === 'edit';
    }

    function getConnectorTypeFromOption($option) {
        if (!$option || !$option.length) {
            return '';
        }
        const dataType = ($option.attr('data-connector-type') || '').trim();
        if (dataType) {
            return dataType;
        }
        const text = $option.text() || '';
        const match = text.match(/\(([^()]+)\)\s*$/);
        return match && match[1] ? match[1].trim() : '';
    }

    function getConnectorTypeBySelect(selectId, connectorId) {
        if (!connectorId) {
            return '';
        }
        const $option = $('#' + selectId + ' option[value="' + connectorId + '"]');
        if ($option.length) {
            return getConnectorTypeFromOption($option);
        }
        const cached = allConnectorOptions.find(function (item) {
            return item.value === connectorId;
        });
        return cached ? cached.connectorType : '';
    }

    function initConnectorOptionsCache() {
        allConnectorOptions = [];
        $('#sourceConnectorId option').each(function () {
            const value = ($(this).val() || '').trim();
            if (!value) {
                return;
            }
            allConnectorOptions.push({
                label: $(this).text().trim(),
                value: value,
                connectorType: getConnectorTypeFromOption($(this)),
                disabled: !!$(this).prop('disabled')
            });
        });
    }

    function isSameConnectorType(sourceConnectorId, targetConnectorId) {
        const sourceType = normalizeConnectorType(getConnectorTypeById(sourceConnectorId));
        const targetType = normalizeConnectorType(getConnectorTypeById(targetConnectorId));
        return !!(sourceType && targetType && sourceType === targetType);
    }

    function buildTargetConnectorSelectOptions(sourceConnectorId, currentTargetId) {
        const sourceType = normalizeConnectorType(getConnectorTypeById(sourceConnectorId));
        const filtered = sourceType
            ? allConnectorOptions.filter(function (item) {
                return normalizeConnectorType(item.connectorType) === sourceType;
            })
            : [];
        const options = [{ label: '请选择连接器', value: '', disabled: false }].concat(
            filtered.map(function (item) {
                return { label: item.label, value: item.value, disabled: item.disabled };
            })
        );
        const current = currentTargetId || '';
        const validCurrent = current && filtered.some(function (item) {
            return item.value === current;
        });
        return {
            options: options,
            nextId: validCurrent ? current : ''
        };
    }

    function applyTargetConnectorSelect(select, options, nextId) {
        if (!select || typeof select.setData !== 'function') {
            return;
        }
        suppressConnectorSelect = true;
        try {
            select.setData(options);
            select.setValues(nextId ? [nextId] : [], true);
        } finally {
            suppressConnectorSelect = false;
        }
    }

    /**
     * 详情区目标连接按当前库映射的源/目标连接器过滤；
     * 弹窗内目标连接按顶部源端过滤。二者互不干扰。
     */
    function refreshTargetConnectorOptions() {
        const block = getActiveMappingBlock();

        if (targetConnectorSelect && hasDetailTargetPanel()) {
            const detailSourceId = block ? getMappingSourceConnectorId(block) : state.source.connectorId;
            const detailTargetId = block ? getMappingTargetConnectorId(block) : state.target.connectorId;
            const detail = buildTargetConnectorSelectOptions(detailSourceId, detailTargetId);
            applyTargetConnectorSelect(targetConnectorSelect, detail.options, detail.nextId);
            if (block) {
                state.target.connectorId = block.targetConnectorId || detail.nextId;
            }
        }

        if (pickerTargetConnectorSelect) {
            const pickerTargetId = getPickerTargetConnectorId() || state.target.connectorId;
            const picker = buildTargetConnectorSelectOptions(state.source.connectorId, pickerTargetId);
            applyTargetConnectorSelect(pickerTargetConnectorSelect, picker.options, picker.nextId);
            if (isPickerModalOpen()) {
                state.target.connectorId = picker.nextId;
            }
        } else if (!block || !hasDetailTargetPanel()) {
            const global = buildTargetConnectorSelectOptions(state.source.connectorId, state.target.connectorId);
            if (targetConnectorSelect) {
                applyTargetConnectorSelect(targetConnectorSelect, global.options, global.nextId);
            }
            state.target.connectorId = global.nextId;
        }
    }

    function normalizeConnectorType(connectorType) {
        return (connectorType || '').trim().toLowerCase();
    }

    function isUnsupportedSourceConnectorType(connectorType) {
        return UNSUPPORTED_SOURCE_CONNECTOR_TYPES.indexOf(normalizeConnectorType(connectorType)) >= 0;
    }

    function isUnsupportedSourceConnector(connectorId) {
        return isUnsupportedSourceConnectorType(getConnectorTypeById(connectorId));
    }

    function isSourceConnectorOptionDisabled(connectorType) {
        return isUnsupportedSourceConnectorType(connectorType);
    }

    function refreshSourceConnectorOptions() {
        if (!sourceConnectorSelect || typeof sourceConnectorSelect.setData !== 'function') {
            return;
        }
        const options = [{ label: '请选择连接器', value: '', disabled: false }].concat(
            allConnectorOptions.map(function (item) {
                return {
                    label: item.label,
                    value: item.value,
                    disabled: isSourceConnectorOptionDisabled(item.connectorType)
                };
            })
        );
        const current = state.source.connectorId || '';
        const keepDisabledSelection = isEditMode() && current && isUnsupportedSourceConnector(current);
        const validCurrent = current && options.some(function (item) {
            return item.value === current && (!item.disabled || keepDisabledSelection);
        });
        const nextId = validCurrent ? current : '';
        suppressConnectorSelect = true;
        try {
            sourceConnectorSelect.setData(options);
            sourceConnectorSelect.setValues(nextId ? [nextId] : [], true);
        } finally {
            suppressConnectorSelect = false;
        }
        if (!validCurrent && current) {
            state.source.connectorId = '';
            lastSourceConnectorId = '';
        } else {
            state.source.connectorId = nextId;
        }
    }

    function isOracleConnector(connectorId) {
        return normalizeConnectorType(getConnectorTypeById(connectorId)) === 'oracle';
    }

    function isOracleSourceConnector() {
        return isOracleConnector(state.source.connectorId);
    }

    function isOracleTargetConnector(connectorId) {
        return isOracleConnector(connectorId != null ? connectorId : getActiveTargetConnectorId());
    }

    function getConnectorTypeById(connectorId) {
        if (!connectorId) {
            return '';
        }
        let type = getConnectorTypeBySelect('pickerTargetConnectorId', connectorId);
        if (!type) {
            type = getConnectorTypeBySelect('targetConnectorId', connectorId);
        }
        if (!type) {
            type = getConnectorTypeBySelect('sourceConnectorId', connectorId);
        }
        return type;
    }

    function getPickerTargetConnectorId() {
        if (pickerTargetConnectorSelect && typeof pickerTargetConnectorSelect.getValues === 'function') {
            const ids = pickerTargetConnectorSelect.getValues();
            if (ids.length) {
                return ids[0];
            }
        }
        return $('#pickerTargetConnectorId').val() || '';
    }

    function getActiveMappingBlock() {
        if (state.activeMappingIndex < 0 || state.activeMappingIndex >= state.mappings.length) {
            return null;
        }
        return state.mappings[state.activeMappingIndex];
    }

    function getMappingSourceConnectorId(block) {
        if (block && block.sourceConnectorId) {
            return block.sourceConnectorId;
        }
        return state.source.connectorId || '';
    }

    function getMappingTargetConnectorId(block) {
        if (block && block.targetConnectorId) {
            return block.targetConnectorId;
        }
        return state.target.connectorId || '';
    }

    function getDetailPanelTargetConnectorId() {
        return getMappingTargetConnectorId(getActiveMappingBlock());
    }

    function getActiveTargetConnectorId() {
        if (isPickerModalOpen()) {
            const pickerId = getPickerTargetConnectorId();
            if (pickerId) {
                return pickerId;
            }
        }
        if (hasDetailTargetPanel()) {
            const detailId = getDetailPanelTargetConnectorId();
            if (detailId) {
                return detailId;
            }
        }
        return state.target.connectorId || '';
    }

    function isSourceNamespaceReady() {
        const srcDb = (state.source.database || '').trim();
        const srcSchema = (state.source.schema || '').trim();
        if (isOracleSourceConnector()) {
            return !!srcSchema;
        }
        if (!srcDb) {
            return false;
        }
        return !state.source.schemaEnabled || !!srcSchema;
    }

    function isPickerModalOpen() {
        return !!tablePickerModal;
    }

    /** 弹窗内：源库/Schema 选中后联动覆盖目标库名、目标 Schema */
    function syncPickerTargetFromSource() {
        const srcDb = (state.source.database || '').trim();
        const srcSchema = (state.source.schema || '').trim();
        const pickerTargetId = getPickerTargetConnectorId() || state.target.connectorId;
        if ($('#pickerTargetDatabase').length && !isOracleTargetConnector(pickerTargetId)) {
            $('#pickerTargetDatabase').val(srcDb);
        }
        if ($('#pickerTargetSchema').length) {
            $('#pickerTargetSchema').val(srcSchema);
        }
    }

    /** 源端库/Schema 就绪后填充目标命名空间；弹窗内强制联动，详情区仅在为空时默认填充 */
    function fillTargetFromSource() {
        if (isPickerModalOpen()) {
            syncPickerTargetFromSource();
            return;
        }
        if (!isSourceNamespaceReady()) {
            return;
        }
        const srcDb = (state.source.database || '').trim();
        const srcSchema = (state.source.schema || '').trim();
        if (hasDetailTargetPanel() && !isOracleTargetConnector(getDetailPanelTargetConnectorId())) {
            const block = getActiveMappingBlock();
            const $detailDb = $('#detailTargetDatabase');
            if (!block || !block.targetDatabase) {
                if (!$detailDb.val().trim()) {
                    $detailDb.val(srcDb);
                }
            }
        }
        if (srcSchema) {
            const $detailSchema = $('#detailTargetSchema');
            if ($detailSchema.length && !$detailSchema.val().trim()) {
                $detailSchema.val(srcSchema);
            }
        }
    }

    function hasDetailTargetPanel() {
        return $('#detailTargetDatabase').length > 0;
    }

    function getConnectorLabel(selectId, connectorId) {
        if (!connectorId) {
            return '未选择';
        }
        const text = $('#' + selectId + ' option[value="' + connectorId + '"]').text() || '';
        const name = text.replace(/\([^)]*\)\s*$/, '').trim();
        return name || connectorId;
    }

    /** 程序化赋值时不触发 onSelect，避免与 syncDetailPanelFromActiveMapping 形成递归 */
    function setSelectValues(api, values) {
        if (!api || typeof api.setValues !== 'function') {
            return;
        }
        const list = Array.isArray(values) ? values : [values];
        api.setValues(list, true);
    }

    function refreshMappingCardFlows() {
        if (!hasDetailTargetPanel() || !state.mappings.length) {
            return;
        }
        $('#mappingCardList .list-card').each(function () {
            const idx = Number($(this).data('index'));
            const block = state.mappings[idx];
            if (block) {
                const $src = $(this).find('.list-card-line.is-title');
                const $target = $(this).find('.list-card-line.list-card-target');
                if ($src.length) {
                    $src.attr('title', getMappingSourceLineTitle(block));
                }
                if ($target.length) {
                    $target.html(formatMappingCardTargetLineHtml(block));
                    $target.attr('title', getMappingTargetLineTitle(block));
                }
            }
        });
    }

    function updateMappingCount() {
        $('#mappingCount').text(state.mappings.length);
        updateAddMappingButtonState();
    }

    /**
     * 临时方案：仅允许一条库映射关系。
     */
    function updateAddMappingButtonState() {
        const $btn = $('#btnAddDbMapping');
        if (!$btn.length || isReadOnly()) {
            return;
        }
        const limitReached = state.mappings.length >= 1;
        $btn.prop('disabled', limitReached);
        if (limitReached) {
            $btn.attr('title', '当前仅支持单条库映射关系');
        } else {
            $btn.removeAttr('title');
        }
    }

    function updateWorkspaceEmptyState() {
        const $empty = $('#mappingWorkspaceEmpty');
        if (!$empty.length) {
            return;
        }
        if (!state.mappings.length) {
            $empty.removeClass('hidden');
        } else {
            $empty.addClass('hidden');
        }
    }

    function updateSourceDatabaseFieldVisibility() {
        const hide = isOracleSourceConnector();
        $('#sourceDatabase').closest('.form-item').toggleClass('hidden', hide);
    }

    function toggleSchemaLabelRequired($wrap, required) {
        const $label = $wrap.find('.form-label').first();
        $label.find('.ds-schema-req').remove();
        if (required) {
            $label.append(' <strong class="text-primary ds-schema-req">*</strong>');
        }
    }

    function updateTargetDatabaseFieldVisibility() {
        const pickerOracle = isOracleTargetConnector(getPickerTargetConnectorId() || state.target.connectorId);
        const detailOracle = isOracleTargetConnector(getDetailPanelTargetConnectorId());
        $('#pickerTargetDatabase').closest('.form-item').toggleClass('hidden', pickerOracle);
        $('#detailTargetDatabase').closest('.ds-target-field').toggleClass('hidden', detailOracle);
        toggleSchemaLabelRequired($('#pickerTargetSchemaWrap'), pickerOracle);
        toggleSchemaLabelRequired($('#detailTargetSchemaWrap'), detailOracle);
    }

    function updateTargetSchemaVisibility() {
        const show = !!state.source.schemaEnabled;
        $('#detailTargetSchemaWrap, #pickerTargetSchemaWrap').toggleClass('hidden', !show);
        updateTargetDatabaseFieldVisibility();
    }

    function formatMappingSourceLabel(block) {
        if (!block) {
            return '';
        }
        let label = block.sourceDatabase || '';
        if (block.sourceSchema) {
            label += (label ? '.' : '') + block.sourceSchema;
        }
        return label || '—';
    }

    function getMappingSourceDbName(block) {
        if (!block) {
            return '';
        }
        if (block.sourceDatabase) {
            return block.sourceDatabase;
        }
        if (isOracleConnector(getMappingSourceConnectorId(block))) {
            return block.sourceSchema || '';
        }
        return block.sourceDatabase || block.sourceSchema || '';
    }

    function getMappingTargetDbName(block) {
        if (!block) {
            return '';
        }
        if (block.targetDatabase) {
            return block.targetDatabase;
        }
        if (isOracleTargetConnector(getMappingTargetConnectorId(block))) {
            return block.targetSchema || '';
        }
        return block.targetDatabase || block.targetSchema || '';
    }

    function formatDbWithConnectorType(dbName, connectorId) {
        const name = escapeHtml(dbName || '—');
        const type = getConnectorTypeById(connectorId);
        if (!type) {
            return name;
        }
        return name + '(' + escapeHtml(type) + ')';
    }

    function getConnectorDisplayName(connectorId) {
        if (!connectorId) {
            return '';
        }
        const found = allConnectorOptions.find(function (item) {
            return item.value === connectorId;
        });
        if (found && found.label) {
            return found.label.replace(/\([^)]*\)\s*$/, '').trim() || connectorId;
        }
        return getConnectorLabel('targetConnectorId', connectorId)
            || getConnectorLabel('sourceConnectorId', connectorId)
            || connectorId;
    }

    function formatMappingCardSourceLineHtml(block) {
        return '源&nbsp;&nbsp;&nbsp;&nbsp;库:' + formatDbWithConnectorType(getMappingSourceDbName(block), getMappingSourceConnectorId(block));
    }

    function formatMappingCardTargetLineHtml(block) {
        const connId = getMappingTargetConnectorId(block);
        const connName = getConnectorDisplayName(connId);
        const dbPart = formatDbWithConnectorType(getMappingTargetDbName(block), connId);
        if (!connName || connName === '未选择') {
            return '目标库: ' + dbPart;
        }
        return '目标库: ' + dbPart + ' · ' + escapeHtml(connName);
    }

    function getMappingSourceLineTitle(block) {
        const db = getMappingSourceDbName(block) || '—';
        const type = getConnectorTypeById(getMappingSourceConnectorId(block));
        return type ? ('源库: ' + db + '(' + type + ')') : ('源库: ' + db);
    }

    function getMappingTargetLineTitle(block) {
        const db = getMappingTargetDbName(block) || '—';
        const connId = getMappingTargetConnectorId(block);
        const type = getConnectorTypeById(connId);
        const connName = getConnectorDisplayName(connId);
        let title = type ? ('目标库: ' + db + '(' + type + ')') : ('目标库: ' + db);
        if (connName && connName !== '未选择') {
            title += ' · ' + connName;
        }
        return title;
    }

    function formatMappingCardBodyHtml(block) {
        return '<div class="list-card-line is-title" title="' + escapeHtml(getMappingSourceLineTitle(block)) + '">'
            + formatMappingCardSourceLineHtml(block) + '</div>'
            + '<div class="list-card-line list-card-target" title="' + escapeHtml(getMappingTargetLineTitle(block)) + '">'
            + formatMappingCardTargetLineHtml(block) + '</div>';
    }

    function formatTargetNamespaceLabel(block) {
        if (!block) {
            return '—';
        }
        if (block.targetDatabase) {
            return block.targetDatabase;
        }
        if (isOracleTargetConnector(getMappingTargetConnectorId(block)) && block.targetSchema) {
            return block.targetSchema;
        }
        return block.targetDatabase || block.targetSchema || '—';
    }

    function refreshPickerTargetDefaults() {
        refreshTargetConnectorOptions();
        const pickerTargetId = getPickerTargetConnectorId() || state.target.connectorId;
        if (pickerTargetId && pickerTargetConnectorSelect) {
            setSelectValues(pickerTargetConnectorSelect, [pickerTargetId]);
        }
        updateTargetSchemaVisibility();
    }

    function onPickerSourceContextChange() {
        if (!isPickerModalOpen()) {
            return;
        }
        updateTargetSchemaVisibility();
        fillTargetFromSource();
    }

    function syncDetailPanelFromActiveMapping() {
        if (!hasDetailTargetPanel()) {
            return;
        }
        const block = state.mappings[state.activeMappingIndex];
        if (!block) {
            return;
        }
        const sourceConnectorId = getMappingSourceConnectorId(block);
        const targetConnectorId = getMappingTargetConnectorId(block);
        state.source.connectorId = sourceConnectorId;
        state.target.connectorId = targetConnectorId;
        if (sourceConnectorId) {
            setSelectValues(sourceConnectorSelect, [sourceConnectorId]);
        }
        refreshTargetConnectorOptions();
        $('#detailTargetDatabase').val(block.targetDatabase || '');
        $('#detailTargetSchema').val(block.targetSchema || '');
        if (targetConnectorId) {
            setSelectValues(targetConnectorSelect, [targetConnectorId]);
        } else {
            setSelectValues(targetConnectorSelect, []);
        }
        updateTargetDatabaseFieldVisibility();
        const srcLabel = formatMappingSourceLabel(block);
        const tgtNs = formatTargetNamespaceLabel(block);
        $('#mappingDetailTitle').html(
            '<i class="fa fa-table"></i> 表映射明细'
            + (srcLabel ? ' <span class="text-tertiary text-sm">(' + escapeHtml(srcLabel) + ' ➔ ' + escapeHtml(tgtNs) + ')</span>' : '')
        );
    }

    function syncActiveMappingFromDetailPanel() {
        if (!hasDetailTargetPanel()) {
            return;
        }
        const block = state.mappings[state.activeMappingIndex];
        if (!block) {
            return;
        }
        let targetConnectorId = state.target.connectorId;
        if (targetConnectorSelect && typeof targetConnectorSelect.getValues === 'function') {
            const ids = targetConnectorSelect.getValues();
            if (ids.length) {
                targetConnectorId = ids[0];
            }
        }
        targetConnectorId = targetConnectorId || $('#targetConnectorId').val() || '';
        if (!targetConnectorId && block.targetConnectorId) {
            targetConnectorId = block.targetConnectorId;
        }
        block.targetConnectorId = targetConnectorId;
        state.target.connectorId = targetConnectorId;
        if (!isOracleTargetConnector(targetConnectorId)) {
            block.targetDatabase = ($('#detailTargetDatabase').val() || '').trim();
        } else {
            block.targetDatabase = '';
        }
        block.targetSchema = ($('#detailTargetSchema').val() || '').trim();
        syncMappingsJson();
        const $activeCard = $('#mappingCardList .list-card.is-active');
        if ($activeCard.length) {
            $activeCard.find('.list-card-target').html(formatMappingCardTargetLineHtml(block));
        }
        const srcLabel = formatMappingSourceLabel(block);
        const tgtNs = formatTargetNamespaceLabel(block);
        $('#mappingDetailTitle').html(
            '<i class="fa fa-table"></i> 表映射明细'
            + (srcLabel ? ' <span class="text-tertiary text-sm">(' + escapeHtml(srcLabel) + ' ➔ ' + escapeHtml(tgtNs) + ')</span>' : '')
        );
    }

    function bindDetailPanelEvents() {
        if (!hasDetailTargetPanel() || detailPanelBound) {
            return;
        }
        detailPanelBound = true;
        $('#detailTargetDatabase').on('input change', function () {
            syncActiveMappingFromDetailPanel();
        });
        $('#detailTargetSchema').on('input change', function () {
            syncActiveMappingFromDetailPanel();
        });
        $('#btnBatchTablePrefix').on('click', batchAddTablePrefix);
        $('#btnAddTablesToMapping').on('click', openAppendTablePickerModal);
    }

    function applyTablePrefixToBlock(block, prefix) {
        (block.tableMappings || []).forEach(function (row) {
            row.targetTable = prefix + (row.targetTable || '');
        });
        syncMappingsJson();
        renderMappingDetail(false);
        bootGrowl('已批量更新目标表名', 'success');
    }

    function batchAddTablePrefix() {
        if (isReadOnly()) {
            return;
        }
        const block = state.mappings[state.activeMappingIndex];
        if (!block || !(block.tableMappings || []).length) {
            bootGrowl('当前映射没有可编辑的表', 'warning');
            return;
        }
        const inputId = 'batchTablePrefixInput';
        showConfirm({
            title: '批量加目标表前缀',
            message: '将为当前映射下的所有目标表名追加此前缀',
            icon: 'info',
            size: 'normal',
            confirmText: '确定',
            confirmType: 'primary',
            body: '<div class="form-item mb-0">'
                + '<label class="form-label" for="' + inputId + '">表名前缀</label>'
                + '<div class="form-control-area">'
                + '<input type="text" id="' + inputId + '" class="form-control" maxlength="64" value="sync_" placeholder="例如 sync_"/>'
                + '</div></div>',
            onConfirm: function () {
                const $input = $('#' + inputId);
                const prefix = ($input.length ? $input.val() : '') || '';
                if (!String(prefix).trim()) {
                    bootGrowl('前缀不能为空', 'warning');
                    return;
                }
                applyTablePrefixToBlock(block, prefix);
            }
        });
        setTimeout(function () {
            const el = document.getElementById(inputId);
            if (el) {
                el.focus();
                el.select();
            }
        }, 0);
    }

    function onDBChange(connectorId, schemaSelect, dbName) {
        if (!connectorId) {
            state.source.schemaEnabled = false;
            schemaSelect.setData([]);
            updateTargetSchemaVisibility();
            return;
        }
        doGetter('/connector/getSchema', { id: connectorId, database: dbName }, function (response) {
            if (response.success && response.data) {
                const array = (response.data || []).map(function (schema) {
                    return { label: schema, value: schema, disabled: false };
                });
                state.source.schemaEnabled = array.length > 0;
                schemaSelect.setData(array);
            } else {
                state.source.schemaEnabled = false;
                schemaSelect.setData([]);
                bootGrowl('获取 Schema 失败: ' + (response.message || ''), 'danger');
            }
            updateTargetSchemaVisibility();
        });
    }

    function resetSourcePickerContext() {
        state.source.database = '';
        state.source.schema = '';
        state.source.schemaEnabled = false;
        state.source.checked = {};
        resetSourceTableList();
        renderSourceTableTreeEmpty('暂无表，请选择连接器、库与 Schema');
        if (sourceDbSelect) {
            sourceDbSelect.setData([]);
        }
        if (sourceSchemaSelect) {
            sourceSchemaSelect.setData([]);
        }
    }

    /** 打开添加映射弹窗时重置表单，保留已加载的库列表，不沿用上次选择 */
    function resetTablePickerForm() {
        tablePickerMode = 'add';
        state.source.database = '';
        state.source.schema = '';
        state.source.schemaEnabled = false;
        state.source.checked = {};
        state.source.tableSearchKey = '';
        resetSourceTableList();
        renderSourceTableTreeEmpty('暂无表，请选择连接器、库与 Schema');
        $('#sourceTableSearch').val('');
        $('#pickerTargetDatabase').val('');
        $('#pickerTargetSchema').val('');
        if (sourceDbSelect) {
            setSelectValues(sourceDbSelect, []);
        }
        if (sourceSchemaSelect) {
            sourceSchemaSelect.setData([]);
            setSelectValues(sourceSchemaSelect, []);
        }
    }

    function isAppendTablePickerMode() {
        return tablePickerMode === 'append';
    }

    /** 当前库映射已添加的源表名集合（追加模式下去重展示） */
    function getExcludedSourceTableSet() {
        const excluded = {};
        if (!isAppendTablePickerMode()) {
            return excluded;
        }
        const block = getActiveMappingBlock();
        if (!block) {
            return excluded;
        }
        (block.tableMappings || []).forEach(function (row) {
            if (row && row.sourceTable) {
                excluded[row.sourceTable] = true;
            }
        });
        return excluded;
    }

    function getExcludedSourceTableNames() {
        return Object.keys(getExcludedSourceTableSet());
    }

    function hasSelectOptions(selectApi) {
        if (!selectApi || typeof selectApi.getData !== 'function') {
            return false;
        }
        const data = selectApi.getData();
        return Array.isArray(data) && data.length > 0;
    }

    /** 编辑页等场景下源端连接器已赋值但未走 onSelect 时，补拉弹窗内的库/Schema 选项 */
    function ensureSourcePickerOptionsLoaded() {
        const connectorId = state.source.connectorId;
        if (!connectorId) {
            return;
        }
        if (isOracleSourceConnector()) {
            if (!hasSelectOptions(sourceSchemaSelect)) {
                onSourceConnectorChange(connectorId);
            }
            return;
        }
        if (!hasSelectOptions(sourceDbSelect)) {
            onSourceConnectorChange(connectorId);
        }
    }

    function onSourceConnectorChange(connectorId) {
        if (!connectorId) {
            resetSourcePickerContext();
            state.source.connectorId = '';
            clearMappings();
            lastSourceConnectorId = '';
            refreshTargetConnectorOptions();
            return;
        }
        if (!initializing && isUnsupportedSourceConnector(connectorId)) {
            const rollbackId = lastSourceConnectorId && !isUnsupportedSourceConnector(lastSourceConnectorId)
                ? lastSourceConnectorId
                : '';
            setSelectValues(sourceConnectorSelect, rollbackId ? [rollbackId] : [], true);
            state.source.connectorId = rollbackId;
            refreshTargetConnectorOptions();
            return;
        }
        const sourceChanged = lastSourceConnectorId && lastSourceConnectorId !== connectorId;
        if (sourceChanged && !initializing) {
            resetSourcePickerContext();
        }
        lastSourceConnectorId = connectorId;
        state.source.connectorId = connectorId;
        refreshTargetConnectorOptions();
        updateSourceDatabaseFieldVisibility();
        if (isOracleSourceConnector()) {
            state.source.database = '';
            if (sourceDbSelect) {
                sourceDbSelect.setData([]);
            }
            if (sourceSchemaSelect) {
                onDBChange(connectorId, sourceSchemaSelect, '');
            }
            return;
        }
        doGetter('/connector/getDatabase', { id: connectorId }, function (response) {
            if (response.success && response.data) {
                const db = (response.data || []).map(function (dbName) {
                    return { label: dbName, value: dbName, disabled: false };
                });
                if (sourceDbSelect) {
                    sourceDbSelect.setData(db);
                }
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
            typeCounts: {},
            fetchedCount: 0
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
        if (!reset && (!s.tableMeta.hasMore || s.tableMeta.fetchedCount >= TABLE_DISPLAY_MAX)) {
            return;
        }
        fetchSourceTablePage(reset);
    }

    function refreshTableListDisplayMeta(s, serverHasMore) {
        const fetched = s.tableMeta.fetchedCount || 0;
        s.tableMeta.cappedTotal = Math.min(s.tableMeta.total, TABLE_DISPLAY_MAX);
        s.tableMeta.truncated = s.tableMeta.total > TABLE_DISPLAY_MAX;
        s.tableMeta.hasMore = !!serverHasMore && fetched < s.tableMeta.cappedTotal;
    }

    function fetchSourceTablePage(reset, onComplete) {
        const s = state.source;
        const offset = reset ? 0 : (s.tableMeta.fetchedCount || 0);
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
        const request = {
            connectorId: s.connectorId,
            database: s.database || '',
            schema: s.schema || '',
            searchKey: s.tableSearchKey || '',
            offset: offset,
            limit: limit
        };
        if (isAppendTablePickerMode()) {
            request.excludeTablesJson = JSON.stringify(getExcludedSourceTableNames());
        }
        doPoster('/database-sync/previewTables', request, function (res) {
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
            // 服务端已按 excludeTablesJson 排除已添加表，前端不再二次过滤翻页
            const pageItems = payload.data || [];
            s.tableMeta.fetchedCount = offset + pageItems.length;
            s.tableMeta.total = payload.total || 0;
            s.tableMeta.typeCounts = payload.typeCounts || {};
            if (!s.tableMeta.total) {
                refreshTableListDisplayMeta(s, false);
                renderSourceTableTreeEmpty(
                    isAppendTablePickerMode()
                        ? '当前库映射下无可添加的表（已添加的表已隐藏）'
                        : '暂无表，请检查库/Schema 或调整搜索条件'
                );
            } else if (reset) {
                s.tables = pageItems.slice();
                refreshTableListDisplayMeta(s, payload.hasMore);
                renderSourceTableTreeShell();
                appendSourceTableRows(pageItems);
                syncFolderCheckboxState($('#sourceTableTree'));
            } else {
                s.tables = s.tables.concat(pageItems);
                refreshTableListDisplayMeta(s, payload.hasMore);
                if (pageItems.length) {
                    if (!$('#sourceTableTree .picker-tree-body').length) {
                        renderSourceTableTreeShell();
                    }
                    appendSourceTableRows(pageItems);
                    syncFolderCheckboxState($('#sourceTableTree'));
                }
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


    function clearAllSourceTables() {
        state.source.checked = {};
        $('#sourceTableTree .db-sync-table-cb').each(function () {
            setCheckboxGroupChecked($(this), false, true);
        });
        $('#sourceTableTree .db-sync-folder-cb').each(function () {
            setCheckboxGroupChecked($(this), false, true);
            setCheckboxGroupIndeterminate($(this), false);
        });
        updateTableTreeFooter(false);
    }

    function getSelectedTableCount() {
        syncCheckedFromTableTreeDom();
        let count = 0;
        Object.keys(state.source.checked).forEach(function (name) {
            if (state.source.checked[name]) {
                count++;
            }
        });
        return count;
    }

    const TABLE_TREE_CHECKBOX_OPTIONS = {
        size: 'sm',
        layout: 'inline'
    };

    function nextTableCheckboxId(prefix) {
        tableCbSeq += 1;
        return prefix + tableCbSeq;
    }

    function initTableTreeCheckboxStyle($root) {
        ($root || $('#sourceTableTree')).find('.db-sync-table-cb, .db-sync-folder-cb').each(function () {
            const $cb = $(this);
            if ($cb.data('checkboxGroupInitialized')) {
                return;
            }
            const cbId = $cb.attr('id');
            $cb.checkboxGroup(TABLE_TREE_CHECKBOX_OPTIONS);
            // 单选初始化会先包裹 checkbox-group，原 label 不再紧邻 input，需手动移除避免文案重复
            if (cbId) {
                $('label[for="' + cbId + '"]').each(function () {
                    const $label = $(this);
                    if (!$label.closest('.checkbox-group-item').length) {
                        $label.remove();
                    }
                });
            }
        });
    }

    function setCheckboxGroupChecked($cb, checked, silent) {
        $cb.prop('indeterminate', false);
        const $item = $cb.closest('.checkbox-group-item');
        if ($item.length) {
            $item.removeClass('indeterminate');
        }
        const api = $cb.data('checkboxGroup');
        if (!silent && api && typeof api.setValue === 'function') {
            api.setValue(checked);
        } else {
            $cb.prop('checked', checked);
            if ($item.length) {
                $item.toggleClass('active', !!checked);
            }
        }
    }

    function setCheckboxGroupIndeterminate($cb, indeterminate) {
        $cb.prop('indeterminate', !!indeterminate);
        const $item = $cb.closest('.checkbox-group-item');
        if (!$item.length) {
            return;
        }
        if (indeterminate) {
            $item.addClass('indeterminate').removeClass('active');
        } else {
            $item.removeClass('indeterminate');
        }
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
        tableCbSeq = 0;
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
            const folderCbId = nextTableCheckboxId('dbSyncFolder_');
            html += '<div class="picker-tree-node" data-folder-key="' + folder.key + '">'
                + '<div class="picker-tree-folder picker-tree-item picker-tree-folder-label" data-folder="TABLE">'
                + '<input type="checkbox" class="db-sync-folder-cb" id="' + folderCbId + '" data-folder-key="' + folder.key + '"'
                + (folderChecked ? ' checked' : '')
                + (folderIndeterminate ? ' data-indeterminate="1"' : '')
                + '>'
                + '<label for="' + folderCbId + '" class="picker-tree-folder-cb-label"></label>'
                + '<i class="fa fa-caret-down picker-tree-folder-caret"></i>'
                + '<span class="picker-tree-folder-title">' + folder.label + ' (' + count + ')</span>'
                + '</div>'
                + '<div class="picker-tree-children" data-folder-key="' + folder.key + '"></div>'
                + '</div>';
        });
        html += '</div><div class="picker-scroll-footer" id="sourceTableTreeFooter"></div>';
        $box.html(html);
        initTableTreeCheckboxStyle($box);

        $box.find('.db-sync-folder-cb[data-indeterminate="1"]').each(function () {
            setCheckboxGroupIndeterminate($(this), true);
        });

        bindTableTreeScroll();
        updateTableTreeFooter(false);
    }

    function buildTableRowHtml(t) {
        const name = t.name || '';
        const ck = state.source.checked[name] ? ' checked' : '';
        const safeName = String(name).replace(/"/g, '&quot;');
        const cbId = nextTableCheckboxId('dbSyncTable_');
        return '<div class="picker-tree-item">'
            + '<input type="checkbox" class="db-sync-table-cb" id="' + cbId + '" data-name="' + safeName + '" value="' + safeName + '"' + ck + '>'
            + '<label for="' + cbId + '">' + escapeHtml(name) + '</label>'
            + '</div>';
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
            initTableTreeCheckboxStyle($children);
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
        const selected = getSelectedTableCount();
        const total = meta.cappedTotal || meta.total;
        let html = '<span class="picker-scroll-footer-count">已选中 ' + selected + ' / 共' + total + ' 张表</span>';
        if (meta.hasMore) {
            html += '<span class="picker-scroll-footer-hint">向下滚动加载更多</span>';
        } else if (meta.truncated) {
            html += '<span class="picker-scroll-footer-hint">共 ' + meta.total + ' 张，最多展示 ' + TABLE_DISPLAY_MAX + ' 张</span>';
        }
        $footer.html(html);
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
            if ($(e.target).closest('.checkbox-group-item, .checkbox-group').length) {
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
            updateTableTreeFooter(false);
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
            $node.find('.db-sync-table-cb').each(function () {
                setCheckboxGroupChecked($(this), checked, true);
            });
            updateTableTreeFooter(false);
        });
    }

    function syncFolderCheckboxState($box) {
        $box.find('.picker-tree-node').each(function () {
            const $node = $(this);
            const $folderCb = $node.children('.picker-tree-folder').find('.db-sync-folder-cb').first();
            const $tableCbs = $node.children('.picker-tree-children').find('.db-sync-table-cb');
            if (!$folderCb.length || !$tableCbs.length) {
                return;
            }
            const total = $tableCbs.length;
            const checkedCount = $tableCbs.filter(':checked').length;
            if (checkedCount === total) {
                setCheckboxGroupChecked($folderCb, true, true);
            } else if (checkedCount === 0) {
                setCheckboxGroupChecked($folderCb, false, true);
            } else {
                setCheckboxGroupIndeterminate($folderCb, true);
            }
        });
    }

    /**
     * 将表树 DOM 中的勾选状态写回 state（仅更新当前已渲染的表行，不覆盖未渲染项）。
     * 解决再次打开弹窗时 UI 仍勾选但 state 已被清空导致校验失败的问题。
     */
    function syncCheckedFromTableTreeDom() {
        $('#sourceTableTree .db-sync-table-cb').each(function () {
            const name = $(this).data('name');
            if (!name) {
                return;
            }
            if (this.checked) {
                state.source.checked[name] = true;
            } else {
                delete state.source.checked[name];
            }
        });
    }

    function getCheckedSourceTableNames() {
        syncCheckedFromTableTreeDom();
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

    /**
     * 同一源库可配置多个不同目标库；仅当源端与目标端命名空间完全一致时才合并表映射。
     */
    function findMappingIndex(sourceDatabase, sourceSchema, targetDatabase, targetSchema,
                              sourceConnectorId, targetConnectorId) {
        const srcSchema = sourceSchema || '';
        const tgtDb = targetDatabase || '';
        const tgtSchema = targetSchema || '';
        const srcConn = sourceConnectorId || '';
        const tgtConn = targetConnectorId || '';
        return state.mappings.findIndex(function (m) {
            return m.sourceDatabase === sourceDatabase
                && (m.sourceSchema || '') === srcSchema
                && (m.targetDatabase || '') === tgtDb
                && (m.targetSchema || '') === tgtSchema
                && getMappingSourceConnectorId(m) === srcConn
                && getMappingTargetConnectorId(m) === tgtConn;
        });
    }

    function buildPickerFormField(labelHtml, controlHtml, options) {
        options = options || {};
        let wrapClass = 'form-item';
        if (options.wrapClass) {
            wrapClass += ' ' + options.wrapClass;
        }
        const wrapId = options.wrapId ? ' id="' + options.wrapId + '"' : '';
        const controlBlock = options.controlType === 'select'
            ? controlHtml
            : '<div class="form-control-area">' + controlHtml + '</div>';
        return '<div class="' + wrapClass + '"' + wrapId + '>'
            + '<label class="form-label">' + labelHtml + '</label>'
            + controlBlock
            + '</div>';
    }

    function buildTablePickerBodyHtml() {
        const ro = isReadOnly();
        const appendMode = isAppendTablePickerMode();
        const selectDisabled = (ro || appendMode) ? ' disabled' : '';
        const inputReadonly = (ro || appendMode) ? ' readonly' : '';
        const btnDisabled = ro ? ' disabled' : '';
        return '<div class="dbsyncer-select-panel table-picker-body">'
            + '<div class="mb-4">'
            + '<h5 class="text-sm font-medium mb-3"><span class="text-primary">1.</span> 选择源库</h5>'
            + (appendMode
                ? '<p class="text-warning text-sm mb-3"><i class="fa fa-info-circle"></i> 已固定为当前库映射的源库，已添加的表不显示</p>'
                : '')
            + '<div class="grid grid-cols-2">'
            + buildPickerFormField(
                '库',
                '<select id="sourceDatabase" class="form-control"' + selectDisabled + '></select>',
                { controlType: 'select' }
            )
            + buildPickerFormField(
                'Schema',
                '<select id="sourceSchema" class="form-control"' + selectDisabled + '></select>',
                { controlType: 'select' }
            )
            + '</div></div>'
            + '<div class="mb-4">'
            + '<h5 class="text-sm font-medium mb-3"><span class="text-primary">2.</span> 指定目标库</h5>'
            + '<p class="text-warning text-sm mb-3">'
            + '<i class="fa fa-info-circle"></i> 暂时不支持异构类型(正在开发中)'
            + '</p>'
            + '<div class="border border-dashed rounded-lg p-4 bg-secondary">'
            + '<div class="grid grid-cols-2">'
            + buildPickerFormField(
                '目标连接<strong class="text-primary">*</strong>',
                '<select id="pickerTargetConnectorId" class="form-control"' + selectDisabled + '></select>',
                { controlType: 'select' }
            )
            + buildPickerFormField(
                '目标库名',
                '<input type="text" id="pickerTargetDatabase" class="form-control" maxlength="128" placeholder="目标库名"'
                + inputReadonly + '/>'
            )
            + buildPickerFormField(
                '目标 Schema',
                '<input type="text" id="pickerTargetSchema" class="form-control" maxlength="128" placeholder="目标 Schema"'
                + inputReadonly + '/>',
                { wrapId: 'pickerTargetSchemaWrap' }
            )
            + '</div></div></div>'
            + '<div>'
            + '<h5 class="text-sm font-medium mb-3"><span class="text-primary">3.</span> 勾选需要同步的物理表</h5>'
            + '<div class="flex items-center justify-between gap-2 mb-2">'
            + '<div class="search-wrapper flex-1"><i class="fa fa-search search-icon"></i>'
            + '<input id="sourceTableSearch" class="search-input" type="text" maxlength="64" placeholder="输入表名过滤搜索..."'
            + (ro ? ' readonly' : '') + '/></div>'
            + '<div class="flex items-center gap-2 flex-shrink-0">'
            + '<button type="button" id="btnClearAllTables" class="btn btn-outline btn-sm"' + btnDisabled + '>清空</button>'
            + '</div></div>'
            + '<div id="sourceTableTree" class="picker-scroll picker-tree-checkbox-group"></div>'
            + '</div></div>';
    }

    function initTablePickerControls() {
        tableTreeScrollBound = false;

        sourceSchemaSelect = $('#sourceSchema').dbSelect({
            type: 'single',
            disabled: isReadOnly() || isAppendTablePickerMode(),
            onSelect: function (selected) {
                if (isAppendTablePickerMode()) {
                    return;
                }
                state.source.schema = selected.length ? selected[0] : '';
                loadSourceTables($('#sourceTableSearch').val().trim(), true);
                fillTargetFromSource();
                onPickerSourceContextChange();
            }
        });

        sourceDbSelect = $('#sourceDatabase').dbSelect({
            type: 'single',
            disabled: isReadOnly() || isAppendTablePickerMode(),
            onSelect: function (selected) {
                if (isAppendTablePickerMode()) {
                    return;
                }
                state.source.database = selected.length ? selected[0] : '';
                state.source.schema = '';
                if (sourceSchemaSelect && typeof sourceSchemaSelect.setValues === 'function') {
                    sourceSchemaSelect.setValues([], true);
                }
                if (state.source.connectorId) {
                    onDBChange(state.source.connectorId, sourceSchemaSelect, state.source.database);
                }
                loadSourceTables(null, true);
                fillTargetFromSource();
                onPickerSourceContextChange();
            }
        });

        pickerTargetConnectorSelect = $('#pickerTargetConnectorId').dbSelect({
            type: 'single',
            disabled: isReadOnly() || isAppendTablePickerMode(),
            onSelect: function (ids) {
                if (suppressConnectorSelect || isAppendTablePickerMode()) {
                    return;
                }
                state.target.connectorId = ids.length ? ids[0] : '';
                updateTargetSchemaVisibility();
                updateTargetDatabaseFieldVisibility();
            }
        });

        refreshTargetConnectorOptions();

        $('#sourceTableSearch').off('input.dbSyncPicker').on('input.dbSyncPicker', function () {
            clearTimeout(tablePickerSearchTimer);
            const key = $(this).val().trim();
            tablePickerSearchTimer = setTimeout(function () { loadSourceTables(key, true); }, 300);
        });

        $('#btnClearAllTables').off('click.dbSyncPicker').on('click.dbSyncPicker', clearAllSourceTables);

        bindTableTreeScroll();
        bindTableTreeEvents();
        updateSourceDatabaseFieldVisibility();
        updateTargetSchemaVisibility();
    }

    function destroyTablePickerControls() {
        clearTimeout(tablePickerSearchTimer);
        tablePickerSearchTimer = null;
        tableTreeScrollBound = false;
        sourceDbSelect = null;
        sourceSchemaSelect = null;
        pickerTargetConnectorSelect = null;
    }

    function cleanupTablePickerState() {
        resetTablePickerForm();
        destroyTablePickerControls();
        tablePickerModal = null;
    }

    function openTablePickerModal() {
        if (isReadOnly()) {
            return;
        }
        syncActiveMappingFromDetailPanel();
        if (!state.source.connectorId) {
            bootGrowl('请先选择源端连接器', 'warning');
            return;
        }
        closeTablePickerModal();
        tablePickerMode = 'add';

        tablePickerModal = showConfirm({
            title: '选择源库与目标表映射',
            size: 'max',
            position: 'center',
            body: buildTablePickerBodyHtml(),
            confirmText: '确认添加',
            cancelText: '取消',
            closeOnConfirm: false,
            onConfirm: function () {
                if (addDatabaseMapping()) {
                    closeTablePickerModal();
                }
            },
            onCancel: function () {
                cleanupTablePickerState();
            }
        });

        initTablePickerControls();
        ensureSourcePickerOptionsLoaded();
        refreshPickerTargetDefaults();
        renderSourceTableTreeEmpty('暂无表，请选择连接器、库与 Schema');
    }

    /**
     * 针对当前库映射继续添加表：源/目标命名空间固定，已添加源表不展示。
     */
    function openAppendTablePickerModal() {
        if (isReadOnly()) {
            return;
        }
        syncActiveMappingFromDetailPanel();
        const block = getActiveMappingBlock();
        if (!block) {
            bootGrowl('请先选择左侧库映射', 'warning');
            return;
        }
        const sourceConnectorId = getMappingSourceConnectorId(block) || state.source.connectorId;
        if (!sourceConnectorId) {
            bootGrowl('请先选择源端连接器', 'warning');
            return;
        }
        closeTablePickerModal();
        tablePickerMode = 'append';
        // 先写入固定命名空间，避免库列表 setData 自动选中首项触发误拉表
        state.source.connectorId = sourceConnectorId;
        state.source.database = block.sourceDatabase || '';
        state.source.schema = (block.sourceSchema || '').trim();
        state.source.checked = {};
        state.source.tableSearchKey = '';
        resetSourceTableList();

        tablePickerModal = showConfirm({
            title: '继续添加表（已添加的表不显示）',
            size: 'max',
            position: 'center',
            body: buildTablePickerBodyHtml(),
            confirmText: '确认添加',
            cancelText: '取消',
            closeOnConfirm: false,
            onConfirm: function () {
                if (addDatabaseMapping()) {
                    closeTablePickerModal();
                }
            },
            onCancel: function () {
                cleanupTablePickerState();
            }
        });

        initTablePickerControls();
        applyAppendPickerContext(block);
    }

    function applyAppendPickerContext(block) {
        const targetConnectorId = getMappingTargetConnectorId(block) || state.target.connectorId || '';
        state.target.connectorId = targetConnectorId;
        refreshTargetConnectorOptions();
        if (targetConnectorId && pickerTargetConnectorSelect) {
            setSelectValues(pickerTargetConnectorSelect, [targetConnectorId]);
        }
        $('#pickerTargetDatabase').val(block.targetDatabase || '').prop('readonly', true);
        $('#pickerTargetSchema').val(block.targetSchema || '').prop('readonly', true);
        updateTargetSchemaVisibility();
        updateTargetDatabaseFieldVisibility();
        renderSourceTableTreeEmpty('正在加载可添加的表...');
        loadAppendSourceOptionsThenTables(block);
    }

    /**
     * 追加模式专用：固定源库后立即拉表；库/Schema 下拉仅做展示填充，互不等待，避免空白。
     */
    function loadAppendSourceOptionsThenTables(block) {
        const connectorId = state.source.connectorId;
        const sourceDb = block.sourceDatabase || '';
        const sourceSchema = (block.sourceSchema || '').trim();
        if (!connectorId) {
            renderSourceTableTreeEmpty('源端连接器不能为空');
            return;
        }
        // 关键：先按映射固定值拉表，不依赖下拉选项加载结果
        state.source.database = isOracleSourceConnector() ? '' : sourceDb;
        state.source.schema = sourceSchema;
        loadSourceTables(null, true);

        if (isOracleSourceConnector()) {
            doGetter('/connector/getSchema', { id: connectorId, database: '' }, function (response) {
                if (!isPickerModalOpen() || !isAppendTablePickerMode()) {
                    return;
                }
                const array = (response.success && response.data)
                    ? (response.data || []).map(function (schema) {
                        return { label: schema, value: schema, disabled: false };
                    })
                    : [];
                state.source.schemaEnabled = array.length > 0;
                if (sourceSchemaSelect) {
                    sourceSchemaSelect.setData(array);
                    setSelectValues(sourceSchemaSelect, sourceSchema ? [sourceSchema] : []);
                }
                state.source.database = '';
                state.source.schema = sourceSchema;
                updateTargetSchemaVisibility();
            });
            return;
        }
        doGetter('/connector/getDatabase', { id: connectorId }, function (response) {
            if (!isPickerModalOpen() || !isAppendTablePickerMode()) {
                return;
            }
            if (!response.success) {
                bootGrowl('获取数据库失败: ' + (response.message || ''), 'danger');
                return;
            }
            const dbOptions = (response.data || []).map(function (dbName) {
                return { label: dbName, value: dbName, disabled: false };
            });
            if (sourceDbSelect) {
                sourceDbSelect.setData(dbOptions);
                setSelectValues(sourceDbSelect, sourceDb ? [sourceDb] : []);
            }
            // 下拉回填后再次固定，防止 setData 改写 state
            state.source.database = sourceDb;
            state.source.schema = sourceSchema;
            if (!sourceDb) {
                return;
            }
            doGetter('/connector/getSchema', { id: connectorId, database: sourceDb }, function (schemaRes) {
                if (!isPickerModalOpen() || !isAppendTablePickerMode()) {
                    return;
                }
                if (schemaRes.success && schemaRes.data) {
                    const array = (schemaRes.data || []).map(function (schema) {
                        return { label: schema, value: schema, disabled: false };
                    });
                    state.source.schemaEnabled = array.length > 0;
                    if (sourceSchemaSelect) {
                        sourceSchemaSelect.setData(array);
                        setSelectValues(sourceSchemaSelect, sourceSchema ? [sourceSchema] : []);
                    }
                } else {
                    state.source.schemaEnabled = false;
                    if (sourceSchemaSelect) {
                        sourceSchemaSelect.setData([]);
                    }
                }
                state.source.database = sourceDb;
                state.source.schema = sourceSchema;
                updateTargetSchemaVisibility();
            });
        });
    }

    function closeTablePickerModal() {
        if (!tablePickerModal) {
            return;
        }
        const modal = tablePickerModal;
        cleanupTablePickerState();
        modal.close();
    }

    /**
     * 无库映射时清空右侧表映射展示区
     */
    function resetMappingDetailView() {
        state.activeMappingIndex = -1;
        state.detailPageNum = 1;
        $('#mappingDetailPanel').addClass('hidden');
        $('#mappingDetailTableBody').empty();
        $('#mappingDetailPagination').addClass('hidden');
        updateWorkspaceEmptyState();
    }

    function renderMappingSidebar() {
        const $list = $('#mappingCardList');
        const $empty = $('#mappingSidebarEmpty');
        updateMappingCount();
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
        const useFlowCard = hasDetailTargetPanel();
        let html = '';
        state.mappings.forEach(function (block, idx) {
            const count = (block.tableMappings || []).length;
            const active = idx === state.activeMappingIndex ? ' is-active' : '';
            html += '<div class="list-card' + active + '" data-index="' + idx + '">';
            if (!isReadOnly()) {
                html += '<button type="button" class="list-card-remove" data-index="' + idx + '" title="删除库映射">'
                    + '<i class="fa fa-times"></i></button>';
            }
            if (useFlowCard) {
                html += formatMappingCardBodyHtml(block)
                    + '<div class="list-card-footer">已选择 ' + count + ' 张表</div>';
            } else {
                html += '<div class="flex items-center gap-2 text-sm mb-1">'
                    + '<span class="list-card-label">源&nbsp;&nbsp;库</span>'
                    + '<span class="list-card-value" title="' + escapeHtml(block.sourceDatabase || '') + '">'
                    + escapeHtml(block.sourceDatabase || '') + '</span></div>'
                    + (block.sourceSchema
                        ? ('<div class="flex items-center gap-2 text-sm mb-1">'
                            + '<span class="list-card-label">源Schema</span>'
                            + '<span class="list-card-value" title="' + escapeHtml(block.sourceSchema || '') + '">'
                            + escapeHtml(block.sourceSchema || '') + '</span></div>')
                        : '')
                    + '<div class="flex items-center gap-2 text-sm mb-1">'
                    + '<span class="list-card-label">目标</span>'
                    + '<input type="text" class="form-control form-control-sm flex-1 min-w-0 mapping-card-tgt-db"'
                    + ' data-index="' + idx + '" value="' + escapeHtml(block.targetDatabase || '') + '"'
                    + ' title="' + escapeHtml(block.targetDatabase || '') + '"'
                    + (isReadOnly() ? ' readonly' : '') + '/>'
                    + '</div>'
                    + (block.sourceSchema
                        ? ('<div class="flex items-center gap-2 text-sm mb-1">'
                            + '<span class="list-card-label">目标Schema</span>'
                            + '<input type="text" class="form-control form-control-sm flex-1 min-w-0 mapping-card-tgt-schema"'
                            + ' data-index="' + idx + '" value="' + escapeHtml(block.targetSchema || '') + '"'
                            + ' title="' + escapeHtml(block.targetSchema || '') + '"'
                            + (isReadOnly() ? ' readonly' : '') + '/>'
                            + '</div>')
                        : '')
                    + '<div class="list-card-footer">#' + (block.index || (idx + 1)) + ' · 共 ' + count + ' 个对象</div>';
            }
            html += '</div>';
        });
        $list.html(html);
        bindMappingSidebarEvents();
        syncMappingsJson();
    }

    function bindMappingSidebarEvents() {
        $('#mappingCardList .list-card').off('click.selectCard').on('click.selectCard', function (e) {
            if ($(e.target).closest('.list-card-remove, .mapping-card-tgt-db, .mapping-card-tgt-schema').length) {
                return;
            }
            syncActiveMappingFromDetailPanel();
            const idx = Number($(this).data('index'));
            state.activeMappingIndex = idx;
            renderMappingSidebar();
            renderMappingDetail(true);
        });

        $('.list-card-remove').off('click.removeCard').on('click.removeCard', function (e) {
            e.stopPropagation();
            const idx = Number($(this).data('index'));
            showConfirm({
                title: '确定要删除这条库同步路由吗？',
                icon: 'warning',
                size: 'large',
                confirmType: 'danger',
                onConfirm: function () {
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
                }
            });
        });

        $('.mapping-card-tgt-db').off('change.cardTgtDb').on('change.cardTgtDb', function () {
            const idx = Number($(this).data('index'));
            if (!state.mappings[idx]) {
                return;
            }
            const value = $(this).val();
            state.mappings[idx].targetDatabase = value;
            $(this).attr('title', value || '');
            syncMappingsJson();
        });

        $('.mapping-card-tgt-schema').off('change.cardTgtSchema').on('change.cardTgtSchema', function () {
            const idx = Number($(this).data('index'));
            if (!state.mappings[idx]) {
                return;
            }
            const value = $(this).val();
            state.mappings[idx].targetSchema = value;
            $(this).attr('title', value || '');
            syncMappingsJson();
        });
    }

    function buildDetailTableRowHtml(block, row, rowIndex, blockIndex) {
        const sourceTable = row.sourceTable || '';
        const targetTable = row.targetTable || '';
        let html = '<tr data-r="' + rowIndex + '">'
            + '<td class="text-center text-tertiary">' + (row.index || (rowIndex + 1)) + '</td>'
            + '<td class="text-primary font-medium text-truncate" title="' + escapeHtml(sourceTable) + '">' + escapeHtml(sourceTable) + '</td>';
        if (isReadOnly()) {
            html += '<td title="' + escapeHtml(targetTable) + '">' + escapeHtml(targetTable) + '</td>';
        } else {
            html += '<td><input type="text" class="form-control form-control-sm db-tgt-tbl"'
                + ' data-b="' + blockIndex + '" data-r="' + rowIndex + '" value="' + escapeHtml(targetTable) + '"'
                + ' title="' + escapeHtml(targetTable) + '"/></td>'
                + '<td><button type="button" class="table-action-btn delete btn-rm-detail-row"'
                + ' data-b="' + blockIndex + '" data-r="' + rowIndex + '" title="删除">'
                + '<i class="fa fa-times"></i></button></td>';
        }
        return html + '</tr>';
    }

    function getDetailDisplayRows(block) {
        const rows = block.tableMappings || [];
        return rows;
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

        $bar.append(createDetailNavBtn('首页', 'fa-angle-double-left', currentPage === 1, function () {
            renderDetailTablePage(1);
        }));

        $bar.append(createDetailNavBtn('上一页', 'fa-angle-left', currentPage === 1, function () {
            renderDetailTablePage(currentPage - 1);
        }));

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

        $bar.append(createDetailNavBtn('下一页', 'fa-angle-right', currentPage === totalPages, function () {
            renderDetailTablePage(currentPage + 1);
        }));

        $bar.append(createDetailNavBtn('末页', 'fa-angle-double-right', currentPage === totalPages, function () {
            renderDetailTablePage(totalPages);
        }));
    }

    function createDetailNavBtn(label, iconClass, disabled, onClick) {
        const btn = $('<button type="button" class="pagination-btn pagination-btn-nav" title="' + label + '"' + (disabled ? ' disabled' : '') + '>'
            + '<i class="fa ' + iconClass + '"></i></button>');
        if (!disabled) {
            btn.on('click', onClick);
        }
        return btn;
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
        updateWorkspaceEmptyState();
        syncDetailPanelFromActiveMapping();
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
        if (!state.source.connectorId) {
            bootGrowl('请先选择源端连接器', 'warning');
            return false;
        }
        const pickerTargetId = getPickerTargetConnectorId();
        const sourceConnectorId = state.source.connectorId;
        const targetConnectorId = pickerTargetId || state.target.connectorId;
        if (!sourceConnectorId) {
            bootGrowl('请先选择源端连接器', 'warning');
            return false;
        }
        if (!targetConnectorId) {
            bootGrowl('请选择目标连接', 'warning');
            return false;
        }
        if (!isSameConnectorType(sourceConnectorId, targetConnectorId)) {
            bootGrowl('目标连接器必须与源端连接器同类型', 'warning');
            return false;
        }
        state.target.connectorId = targetConnectorId;
        setSelectValues(targetConnectorSelect, [targetConnectorId]);
        setSelectValues(pickerTargetConnectorSelect, [targetConnectorId]);
        const sourceIsOracle = isOracleSourceConnector();
        const targetIsOracle = isOracleTargetConnector(targetConnectorId);
        if (!isSourceNamespaceReady()) {
            if (state.source.schemaEnabled) {
                bootGrowl('请先选择源库和 Schema', 'warning');
            } else if (sourceIsOracle) {
                bootGrowl('请先选择源 Schema', 'warning');
            } else {
                bootGrowl('请先选择源库', 'warning');
            }
            return false;
        }
        if (!sourceTables.length) {
            bootGrowl('请至少勾选一张源表，可使用「全选」', 'warning');
            return false;
        }
        const sourceDb = sourceIsOracle ? '' : (state.source.database || '');
        const sourceSchema = (state.source.schema || '').trim();
        let defaultTargetDb = ($('#pickerTargetDatabase').val() || '').trim();
        let defaultTargetSchema = ($('#pickerTargetSchema').val() || '').trim();
        if (targetIsOracle) {
            defaultTargetDb = '';
            if (!defaultTargetSchema) {
                defaultTargetSchema = sourceSchema;
            }
        } else {
            if (!defaultTargetDb) {
                defaultTargetDb = sourceDb;
            }
            if (!defaultTargetSchema && sourceSchema) {
                defaultTargetSchema = sourceSchema;
            }
        }
        if (targetIsOracle) {
            if (!defaultTargetSchema) {
                bootGrowl('请填写目标 Schema', 'warning');
                return false;
            }
        } else if (!defaultTargetDb) {
            bootGrowl('目标库名不能为空', 'warning');
            return false;
        }
        const existIdx = findMappingIndex(sourceDb, sourceSchema, defaultTargetDb, defaultTargetSchema,
            sourceConnectorId, targetConnectorId);
        const newRows = sourceTables.map(function (srcName) {
            return { sourceTable: srcName, targetTable: srcName };
        });
        if (existIdx >= 0) {
            const exist = state.mappings[existIdx];
            if (!exist.sourceConnectorId) {
                exist.sourceConnectorId = sourceConnectorId;
            }
            if (!exist.targetConnectorId) {
                exist.targetConnectorId = targetConnectorId;
            }
            const names = {};
            (exist.tableMappings || []).forEach(function (r) {
                names[r.sourceTable] = true;
            });
            let added = 0;
            newRows.forEach(function (r) {
                if (!names[r.sourceTable]) {
                    exist.tableMappings.push(r);
                    added++;
                }
            });
            state.activeMappingIndex = existIdx;
            if (!added) {
                bootGrowl('该源库到目标库的映射已存在!', 'info');
            }
        } else {
            state.mappings.push({
                sourceDatabase: sourceDb,
                sourceSchema: sourceSchema,
                sourceConnectorId: sourceConnectorId,
                targetConnectorId: targetConnectorId,
                targetDatabase: defaultTargetDb,
                targetSchema: defaultTargetSchema,
                tableMappings: newRows
            });
            state.activeMappingIndex = state.mappings.length - 1;
        }
        renderMappingSidebar();
        renderMappingDetail(true);
        return true;
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
        const first = state.mappings.length ? state.mappings[0] : null;
        lastSourceConnectorId = (first && first.sourceConnectorId) || '';
        state.source.connectorId = lastSourceConnectorId;
        state.target.connectorId = (first && first.targetConnectorId) || '';
        state.activeMappingIndex = state.mappings.length ? 0 : -1;

        const $form = $('#database-sync-form');
        $form.find('input[name="name"]').val(task.name || '');
        $form.find('input[name="id"]').val(task.id || '');

        if (state.source.connectorId) {
            setSelectValues(sourceConnectorSelect, [state.source.connectorId]);
            onSourceConnectorChange(state.source.connectorId);
        }
        if (state.target.connectorId && targetConnectorSelect) {
            setSelectValues(targetConnectorSelect, [state.target.connectorId]);
        }

        renderMappingSidebar();
        if (state.mappings.length) {
            renderMappingDetail(true);
        }
        updateWorkspaceEmptyState();
        updateSourceDatabaseFieldVisibility();
        updateTargetSchemaVisibility();
        initializing = false;
    }

    let overwriteSchemaProgrammatic = false;

    function setOverwriteSchemaChecked(checked) {
        overwriteSchemaProgrammatic = true;
        $('input[name="overwriteSchema"]').prop('checked', checked);
        overwriteSchemaProgrammatic = false;
    }

    function bindOverwriteSchemaConfirm() {
        $('input[name="overwriteSchema"]').off('change.syncStrategyOverwrite').on('change.syncStrategyOverwrite', function () {
            if (overwriteSchemaProgrammatic || isReadOnly()) {
                return;
            }
            if (!$(this).is(':checked')) {
                setOverwriteSchemaChecked(false);
                return;
            }
            setOverwriteSchemaChecked(false);
            showConfirm({
                title: '确认开启覆盖结构？',
                message: '开启会删除目标库已经存在的表',
                size: 'large',
                icon: 'warning',
                confirmType: 'danger',
                confirmText: '确认',
                cancelText: '取消',
                onConfirm: function () {
                    setOverwriteSchemaChecked(true);
                },
                onCancel: function () {
                    setOverwriteSchemaChecked(false);
                }
            });
        });
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
        bindOverwriteSchemaConfirm();
    }

    function initSyncStrategyCheckboxStyle() {
        $('.ds-strategy-enable-group').each(function () {
            const $group = $(this);
            if ($group.data('checkboxGroupInitialized')) {
                return;
            }
            $group.checkboxGroup({
                size: 'sm',
                layout: 'inline'
            });
        });
    }

    function refreshSyncStrategyCheckboxState() {
        $('input.db-sync-strategy-enable').each(function () {
            const $input = $(this);
            const api = $input.data('checkboxGroup');
            if (api && typeof api.setValue === 'function') {
                api.setValue($input.prop('checked'));
            }
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
        refreshSyncStrategyCheckboxState();
        bindSyncStrategyEvents();
    }

    function resetDatabaseSyncListPagination() {
        try {
            const key = 'dbsyncer.pagination.database-sync-list';
            const raw = localStorage.getItem(key);
            if (raw) {
                const parsed = JSON.parse(raw);
                parsed.pageNum = 1;
                localStorage.setItem(key, JSON.stringify(parsed));
            }
        } catch (e) {
            console.warn('[database-sync] 重置列表分页失败', e);
        }
    }

    $(document).ready(function () {
        window.backIndexPage = function () {
            doLoader('/database-sync/list');
        };

        initSyncStrategyCheckboxStyle();
        bindSyncStrategyEvents();
        initConnectorOptionsCache();

        sourceConnectorSelect = $('#sourceConnectorId').dbSelect({
            type: 'single',
            disabled: isReadOnly(),
            onSelect: function (ids) {
                if (suppressConnectorSelect) {
                    return;
                }
                onSourceConnectorChange(ids.length ? ids[0] : '');
                updateSourceDatabaseFieldVisibility();
                updateTargetSchemaVisibility();
            }
        });
        refreshSourceConnectorOptions();

        targetConnectorSelect = $('#targetConnectorId').dbSelect({
            type: 'single',
            disabled: isReadOnly(),
            onSelect: function (ids) {
                if (suppressConnectorSelect) {
                    return;
                }
                const id = ids.length ? ids[0] : '';
                state.target.connectorId = id;
                const block = getActiveMappingBlock();
                if (block) {
                    block.targetConnectorId = id;
                    syncMappingsJson();
                    const $card = $('#mappingCardList .list-card.is-active');
                    if ($card.length) {
                        $card.find('.list-card-target').html(formatMappingCardTargetLineHtml(block));
                    }
                }
                setSelectValues(pickerTargetConnectorSelect, id ? [id] : []);
                updateTargetSchemaVisibility();
                updateTargetDatabaseFieldVisibility();
            }
        });

        bindDetailPanelEvents();
        refreshTargetConnectorOptions();
        updateTargetSchemaVisibility();

        $('#btnAddDbMapping').on('click', openTablePickerModal);

        $('#database-sync-submit-btn').on('click', function () {
            syncActiveMappingFromDetailPanel();
            syncMappingsJson();
            const $form = $('#database-sync-form');
            if (!validateForm($form)) {
                return;
            }
            if (!state.mappings.length) {
                bootGrowl('请至少添加一组库映射', 'warning');
                return;
            }
            if (isUnsupportedSourceConnector(state.source.connectorId)) {
                return;
            }
            for (let i = 0; i < state.mappings.length; i++) {
                const mapping = state.mappings[i];
                const srcId = getMappingSourceConnectorId(mapping);
                const tgtId = getMappingTargetConnectorId(mapping);
                if (!isSameConnectorType(srcId, tgtId)) {
                    bootGrowl('库映射 #' + (i + 1) + '：目标连接器必须与源端连接器同类型', 'warning');
                    return;
                }
            }
            const btn = $(this);
            const original = btn.html();
            const saveUrl = isEditMode() ? '/database-sync/edit' : '/database-sync/add';
            btn.html('<i class="fa fa-spinner fa-spin"></i> 保存中...').prop('disabled', true);
            doPoster(saveUrl, $form.serializeJson(), function (response) {
                btn.html(original).prop('disabled', false);
                if (response.success) {
                    bootGrowl(isEditMode() ? '修改成功' : '保存成功', 'success');
                    if (isEditMode()) {
                        resetDatabaseSyncListPagination();
                    }
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
            updateWorkspaceEmptyState();
        }
    });
})();
