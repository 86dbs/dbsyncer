 // 全局变量
    let currentStep = 1; // 当前步骤
    let savedTaskId = null; // 保存的任务ID
    let tableMappingIndex = 0; // 下一个表映射的索引
    let fieldMappingIndex = {}; // 每个表的字段映射索引
    let step1Data = null; // 第一步的数据（暂存，不提交）

    // 初始化
    $(document).ready(function () {

        // 初始化字段映射索引
        $('.table-mapping-item').each(function () {
            const tableIndex = $(this).data('index');
            fieldMappingIndex[tableIndex] = 1; // 下一个字段映射的索引
        });

        // 初始化所有 select 元素（如果 enhanceAllSelects 函数存在）
        if (typeof enhanceAllSelects === 'function') {
            enhanceAllSelects();
        }

        // 绑定步骤切换按钮事件
        $('#step1BackBtn').click(function () {
            backTaskIndexPage();
        });

        $('#step1NextBtn').click(function () {
            saveStep1AndGoNext();
        });

        $('#step2PrevBtn').click(function () {
            // 返回第一步时不清空数据，允许用户修改
            goToStep(1);
        });

        $('#step2BackBtn').click(function () {
            backTaskIndexPage();
        });

        $('#step2SubmitBtn').click(function () {
            saveStep2AndComplete();
        });

        // 隐藏顶部工具栏的保存和返回按钮（使用步骤按钮代替）
        $('#taskSubmitBtn, #taskBackBtn').hide();

        // 触发方式变化事件
        $('input[name="trigger"]').change(function () {
            if ($(this).val() === 'timing') {
                $('#cronGroup').show();
            } else {
                $('#cronGroup').hide();
            }
        });

        // 源连接器变化事件
        $('#sourceConnectorId').change(function () {
            const connectorId = $(this).val();
            if (connectorId) {
                loadDatabases(connectorId, 'source');
                checkAndShowSchema(connectorId, 'source');
            } else {
                $('#sourceDatabaseName').html('<option value="">请先选择源连接器</option>');
                $('#sourceSchemaGroup').hide();
            }
        });

        // 目标连接器变化事件
        $('#targetConnectorId').change(function () {
            const connectorId = $(this).val();
            if (connectorId) {
                loadDatabases(connectorId, 'target');
                checkAndShowSchema(connectorId, 'target');
            } else {
                $('#targetDatabaseName').html('<option value="">请先选择目标连接器</option>');
                $('#sourceSchemaGroup').hide();
            }
        });

        // 源表选择变化事件
        $(document).on('change', '.source-table-select', function () {
            const tableIndex = $(this).data('table-index');
            const tableName = $(this).val();
            const connectorId = $('#sourceConnectorId').val();

            if (tableName && connectorId) {
                loadTableFields(connectorId, tableName, tableIndex, 'source');
            } else {
                clearTableFields(tableIndex, 'source');
            }
        });

        // 目标表选择变化事件
        $(document).on('change', '.target-table-select', function () {
            const tableIndex = $(this).data('table-index');
            const tableName = $(this).val();
            const connectorId = $('#targetConnectorId').val();

            if (tableName && connectorId) {
                loadTableFields(connectorId, tableName, tableIndex, 'target');
            } else {
                clearTableFields(tableIndex, 'target');
            }
        });

        // 初始化 tagsinput
        if (typeof $.fn.tagsinput === 'function') {
            $('#step2SourceTablePK, #step2TargetTablePK').tagsinput({
                tagClass: 'label label-info'
            });
        }

        // 添加表映射
        $('#addTableMappingBtn').click(function () {
            addTableMappingRow();
        });

        // 刷新表列表
        $('#refreshTableBtn').click(function () {
            if (step1Data) {
                loadTablesForStep2(step1Data);
                bootGrowl("表列表刷新成功！", "success");
            } else {
                bootGrowl("无法刷新，请先完成第一步配置！", "warning");
            }
        });

        // 删除选中的表映射
        $('#removeTableMappingBtn').click(function () {
            removeSelectedTableMappings();
        });

        // 全选/取消全选表映射
        $('#selectAllMappings').change(function () {
            const isChecked = $(this).prop('checked');
            $('.table-mapping-checkbox').prop('checked', isChecked);
            updateDeleteButtonState();
        });

        // 监听复选框变化，更新删除按钮状态
        $(document).on('change', '.table-mapping-checkbox', function () {
            updateDeleteButtonState();
        });

        // 点击表映射行打开字段映射配置
        $(document).on('click', '.table-mapping-row', function (e) {
            // 如果点击的是复选框，不触发
            if ($(e.target).is('input[type="checkbox"]')) {
                return;
            }
            const $row = $(this);
            openFieldMappingDialog($row);
        });



        // 删除表映射
        $(document).on('click', '.remove-table-mapping', function () {
            $(this).closest('.table-mapping-item').remove();
            updateTableMappingIndexes();
        });

        // 添加字段映射
        $(document).on('click', '.add-field-mapping', function () {
            const tableIndex = $(this).data('table-index');
            addFieldMapping(tableIndex);
        });

        // 删除字段映射
        $(document).on('click', '.remove-field-mapping', function () {
            $(this).closest('.field-mapping-item').remove();
        });
    });

    /**
     * 步骤切换函数
     */
    function goToStep(step) {
        console.log('[步骤切换] 切换到步骤:', step);
        currentStep = step;

        if (step === 1) {
            $('#step1Content').removeClass('hidden').show();
            $('#step2Content').addClass('hidden').hide();

            // 更新步骤指示器
            $('#step1Indicator').addClass('active');
            $('#step1Indicator .step-circle').css({
                'background': 'var(--primary-color)',
                'color': 'white'
            });
            $('#step1Indicator .step-label > div:first-child').css('color', 'var(--text-primary)');

            $('#step2Indicator').removeClass('active');
            $('#step2Indicator .step-circle').css({
                'background': '#e5e7eb',
                'color': '#9ca3af'
            });
            $('#step2Indicator .step-label > div:first-child').css('color', 'var(--text-secondary)');

        } else if (step === 2) {
            $('#step1Content').addClass('hidden').hide();
            $('#step2Content').removeClass('hidden').show();

            // 更新步骤指示器
            $('#step1Indicator .step-circle').css({
                'background': 'var(--success-color)',
                'color': 'white'
            });
            $('#step1Indicator .step-label > div:first-child').css('color', 'var(--text-secondary)');

            $('#step2Indicator').addClass('active');
            $('#step2Indicator .step-circle').css({
                'background': 'var(--primary-color)',
                'color': 'white'
            });
            $('#step2Indicator .step-label > div:first-child').css('color', 'var(--text-primary)');

            // 更新连接线
            $('.step-line').css('background', 'var(--success-color)');
        }

        // 滚动到顶部
        window.scrollTo(0, 0);
    }

    /**
     * 构建第一步的配置（基础配置）
     */
    function buildStep1Config() {
        const formData = $('#taskAddForm').serializeArray();
        const config = {};

        // 基础配置
        formData.forEach(function (item) {
            if (item.name.includes('tableMappings')) {
                // 表映射配置跳过，第二步处理
                return;
            }

            if (item.name === 'trigger') {
                config[item.name] = item.value;
            } else if (item.name === 'cron' && config.trigger === 'timing') {
                config[item.name] = item.value;
            } else if (['autoMatchTable', 'verification', 'correction', 'tableStructure', 'rowData', 'index', 'triggerFlag', 'function', 'storedProcedure'].includes(item.name)) {
                config[item.name] = item.value === 'true';
            } else {
                config[item.name] = item.value;
            }
        });

        return config;
    }

    /**
     * 验证第一步并进入下一步（不保存到服务器）
     */
    function saveStep1AndGoNext() {
        const config = buildStep1Config();

        // 验证必填字段
        if (!config.name) {
            bootGrowl("请输入任务名称！", "danger");
            return;
        }
        // 如果没有填写任务类型，使用默认值
        if (!config.type) {
            config.type = 'SYNC';
        }
        if (!config.sourceConnectorId) {
                bootGrowl("请选择源连接器！", "danger");
            return;
        }
        if (!config.targetConnectorId) {
                bootGrowl("请选择目标连接器！", "danger");
            return;
        }
        if (!config.sourceDatabaseName) {
                bootGrowl("请输入源数据库名称！", "danger");
            return;
        }
        if (!config.targetDatabaseName) {
                bootGrowl("请输入目标数据库名称！", "danger");
            return;
        }
        if (config.trigger === 'timing' && !config.cron) {
                bootGrowl("定时触发模式下，请输入Cron表达式！", "danger");
            return;
        }

        // 暂存第一步数据（不提交到服务器）
        step1Data = config;
        bootGrowl("基础配置验证通过！", "success");

        // 确保 DOM 元素存在
        if ($('#step2Content').length === 0) {
            console.error('[第一步] 错误：第二步的 DOM 元素不存在！');
                bootGrowl("页面错误，请刷新后重试", "danger");
            return;
        }

        // 加载表列表并进入第二步
        loadTablesForStep2(config);
    }

    /**
     * 为第二步加载表列表
     */
    function loadTablesForStep2(config) {

        // 同时加载源表和目标表
        let sourceTablesLoaded = false;
        let targetTablesLoaded = false;
        let sourceTables = [];
        let targetTables = [];

        // 加载源表
        $.ajax({
            url: $basePath + "/task/getTables",
            type: "GET",
            dataType: "json",
            data: {
                connectorId: config.sourceConnectorId,
                database: config.sourceDatabaseName || '',
                schema: config.sourceSchema || ''
            },
            success: function (response) {
                console.log('[第二步] 源表加载成功:', response);
                sourceTablesLoaded = true;
                if (response.success && response.data) {
                    sourceTables = response.data;
                }
                checkBothLoaded();
            },
            error: function (xhr, status, error) {
                console.error('[第二步] 源表加载失败:', status, error);
                sourceTablesLoaded = true;
                bootGrowl("加载源表列表失败", "danger");
                checkBothLoaded();
            }
        });

        // 加载目标表
        $.ajax({
            url: $basePath + "/task/getTables",
            type: "GET",
            dataType: "json",
            data: {
                connectorId: config.targetConnectorId,
                database: config.targetDatabaseName || '',
                schema: config.targetSchema || ''
            },
            success: function (result) {
                console.log('[第二步] 目标表加载成功:', result);
                targetTablesLoaded = true;
                if (result.success && result.data) {
                    targetTables = result.data;
                }
                checkBothLoaded();
            },
            error: function (xhr, status, error) {
                console.error('[第二步] 目标表加载失败:', status, error);
                targetTablesLoaded = true;
                bootGrowl("加载目标表列表失败", "danger");
                checkBothLoaded();
            }
        });

        // 检查是否都加载完成
        function checkBothLoaded() {
            if (sourceTablesLoaded && targetTablesLoaded) {
                console.log('[第二步] 表列表加载完成，源表数量:', sourceTables.length, '目标表数量:', targetTables.length);

                // 填充表选择器
                populateTableSelects(sourceTables, targetTables);

                // 切换到第二步
                goToStep(2);

                console.log('[第二步] 已切换到第二步，表列表已填充');

                // 如果勾选了自动匹配表，则自动创建表映射
                if (config.autoMatchTable === true) {
                    console.log('[自动匹配表] 检测到勾选了自动匹配表，开始自动匹配');
                    setTimeout(function() {
                        autoMatchTables(sourceTables, targetTables);
                    }, 500);
                }
            }
        }
    }

    /**
     * 自动匹配表（找出源表和目标表中同名的表，自动创建表映射）
     */
    function autoMatchTables(sourceTables, targetTables) {
        console.log('[自动匹配表] 开始自动匹配同名表');

        // 构建目标表名称映射
        const targetTableMap = {};
        targetTables.forEach(function(table) {
            const tableName = table.name || table;
            targetTableMap[tableName] = table;
        });

        console.log('[自动匹配表] 目标表映射:', Object.keys(targetTableMap));

        let matchedCount = 0;

        // 遍历源表，查找目标表中是否有同名表
        sourceTables.forEach(function(sourceTable) {
            const sourceTableName = sourceTable.name || sourceTable;

            // 如果目标表中有同名表
            if (targetTableMap[sourceTableName]) {
                const targetTableName = sourceTableName;

                // 检查是否已存在相同的映射
                let exists = false;
                $('#tableMappingsContainer tr').each(function() {
                    const existingSource = $(this).find('td:eq(1)').text().trim();
                    const existingTarget = $(this).find('td:eq(2)').text().trim();
                    if (existingSource === sourceTableName && existingTarget === targetTableName) {
                        exists = true;
                        return false; // break
                    }
                });

                if (!exists) {
                    console.log('[自动匹配表] 找到同名表:', sourceTableName);

                    const index = tableMappingIndex++;
                    const rowHtml = `
                        <tr data-mapping-index="${index}"
                            title="点击配置字段映射"
                            class="table-mapping-row"
                            data-source-table="${sourceTableName}"
                            data-target-table="${targetTableName}"
                            data-source-isPk=""
                            data-target-isPk="">
                            <td>${index + 1}</td>
                            <td>${sourceTableName}</td>
                            <td>${targetTableName}</td>
                            <td onclick="event.stopPropagation();">
                                <input type="checkbox" class="table-mapping-checkbox"
                                       onclick="event.stopPropagation();" />
                            </td>
                        </tr>
                    `;

                    const $row = $(rowHtml);
                    $('#tableMappingsContainer').append($row);

                    // 自动加载字段映射
                    loadFieldMappingForRow($row, sourceTableName, targetTableName);

                    matchedCount++;
                }
            }
        });

        if (matchedCount > 0) {
            updateTableMappingIndexes();
            updateEmptyState();
            updateDeleteButtonState();
            bootGrowl(`自动匹配成功！已创建 ${matchedCount} 个表映射`, "success");
            console.log('[自动匹配表] 自动匹配完成，创建了', matchedCount, '个表映射');
        } else {
            bootGrowl("未找到同名表，请手动添加表映射", "info");
            console.log('[自动匹配表] 未找到同名表');
        }
    }

    /**
     * 填充表选择器
     */
    function populateTableSelects(sourceTables, targetTables) {
        console.log('[第二步] 填充表选择器，源表数量:', sourceTables.length, '目标表数量:', targetTables.length);

        // 缓存表列表，供后续使用
        window.cachedSourceTables = sourceTables;
        window.cachedTargetTables = targetTables;

        // 构建源表数据
        const sourceTableData = [];
        if (sourceTables && sourceTables.length > 0) {
            sourceTables.forEach(function (table) {
                const tableName = table.name || table;
                const tableType = table.type || 'TABLE';
                sourceTableData.push({
                    value: tableName,
                    label: tableName + ' (' + tableType + ')'
                });
            });
        }

        // 构建目标表数据
        const targetTableData = [];
        if (targetTables && targetTables.length > 0) {
            targetTables.forEach(function (table) {
                const tableName = table.name || table;
                const tableType = table.type || 'TABLE';
                targetTableData.push({
                    value: tableName,
                    label: tableName + ' (' + tableType + ')'
                });
            });
        }

        // 销毁旧的 dbSelect 实例（如果存在）
        const oldSourceInstance = $('#step2SourceTable').data('dbSelect');
        if (oldSourceInstance && typeof oldSourceInstance.destroy === 'function') {
            oldSourceInstance.destroy();
        }
        const oldTargetInstance = $('#step2TargetTable').data('dbSelect');
        if (oldTargetInstance && typeof oldTargetInstance.destroy === 'function') {
            oldTargetInstance.destroy();
        }

        // 使用 dbSelect 组件初始化源表选择器
        if (typeof $.fn.dbSelect === 'function') {
            console.log('[第二步] 使用 dbSelect 初始化源表选择器，数据:', sourceTableData.length);
            $('#step2SourceTable').dbSelect({
                type: 'multiple',
                data: sourceTableData,
                placeholder: '请选择数据源表...',
                searchPlaceholder: '搜索表...',
                onSelect: function(values) {
                    console.log('[源表选择器] 选中的表:', values);
                    // 联动选择：自动勾选目标表中的同名表
                    autoSelectMatchingTargetTables(values);
                }
            });
            console.log('[第二步] 源表选择器初始化完成');
            } else {
            console.warn('[第二步] dbSelect 组件不可用，使用原生 select');
            // 回退到原生 select
            let sourceOptions = '<option value="">请选择源表</option>';
            sourceTableData.forEach(function(item) {
                sourceOptions += '<option value="' + item.value + '">' + item.label + '</option>';
            });
            $('#step2SourceTable').html(sourceOptions);
        }

        // 使用 dbSelect 组件初始化目标表选择器
        if (typeof $.fn.dbSelect === 'function') {
            console.log('[第二步] 使用 dbSelect 初始化目标表选择器，数据:', targetTableData.length);
            $('#step2TargetTable').dbSelect({
                type: 'multiple',
                data: targetTableData,
                placeholder: '请选择目标源表...',
                searchPlaceholder: '搜索表...',
                onSelect: function(values) {
                    console.log('[目标表选择器] 选中的表:', values);
                }
            });
            console.log('[第二步] 目标表选择器初始化完成');
        } else {
            console.warn('[第二步] dbSelect 组件不可用，使用原生 select');
            // 回退到原生 select
            let targetOptions = '<option value="">请选择目标表</option>';
            targetTableData.forEach(function(item) {
                targetOptions += '<option value="' + item.value + '">' + item.label + '</option>';
            });
            $('#step2TargetTable').html(targetOptions);
        }
    }

    /**
     * 自动选择目标表中的同名表（联动选择）
     */
    function autoSelectMatchingTargetTables(sourceTableValues) {
        const targetInstance = $('#step2TargetTable').data('dbSelect');
        if (!targetInstance || typeof targetInstance.getValues !== 'function') {
            console.warn('[联动选择] 目标表实例不可用');
            return;
        }

        // 获取当前已选中的目标表
        const currentTargetValues = targetInstance.getValues() || [];
        console.log('[联动选择] 当前目标表选中:', currentTargetValues);

        // 获取所有可用的目标表数据
        const targetTableData = window.cachedTargetTables || [];
        const targetTableNames = targetTableData.map(t => t.name || t);
        console.log('[联动选择] 可用的目标表:', targetTableNames);

        // 找出源表中有、但目标表还没选的同名表
        const newTargetValues = [...currentTargetValues];
        sourceTableValues.forEach(function(sourceTable) {
            // 如果目标表中有同名表，且还没被选中，则自动选中
            if (targetTableNames.indexOf(sourceTable) > -1 && currentTargetValues.indexOf(sourceTable) === -1) {
                newTargetValues.push(sourceTable);
                console.log('[联动选择] 自动勾选目标表:', sourceTable);
            }
        });

        // 如果有新的选中项，更新目标表选择器
        if (newTargetValues.length !== currentTargetValues.length) {
            targetInstance.setValues(newTargetValues);
            console.log('[联动选择] 已更新目标表选中项:', newTargetValues);
        }
    }

    /**
     * 保存第二步（表映射配置）并完成（一次性提交所有配置）
     */
    function saveStep2AndComplete() {
        console.log('[第二步] 开始保存表映射配置');

        // 检查第一步数据
        if (!step1Data) {
            bootGrowl("缺少第一步数据，请先完成第一步！", "danger");
            goToStep(1);
            return;
        }

        // 构建表映射配置
        const tableMappings = [];
        $('#tableMappingsContainer tr').each(function () {
            const $row = $(this);

            const sourceTableName = $row.find('td:eq(1)').text().trim();
            const targetTableName = $row.find('td:eq(2)').text().trim();
            const sourceFields = $row.data('source-fields') || [];
            const targetFields = $row.data('target-fields') || [];
            const fieldMappings = $row.data('field-mappings') || [];

            if (sourceTableName && targetTableName) {
                // 构建字段映射数组 - 新格式
                const fieldMappingArray = [];
                
                fieldMappings.forEach(function(fm) {
                    // 查找源字段完整信息
                    const sourceField = sourceFields.find(function(field) {
                        const fieldName = field.name || (typeof field.getName === 'function' ? field.getName() : '');
                        return fieldName === fm.source;
                    });
                    
                    // 查找目标字段完整信息
                    const targetField = targetFields.find(function(field) {
                        const fieldName = field.name || (typeof field.getName === 'function' ? field.getName() : '');
                        return fieldName === fm.target;
                    });
                    
                    if (sourceField && targetField) {
                        // 构建源字段对象
                        const sourceFieldObj = {
                            name: sourceField.name || (typeof sourceField.getName === 'function' ? sourceField.getName() : ''),
                            typeName: sourceField.typeName || (typeof sourceField.getTypeName === 'function' ? sourceField.getTypeName() : ''),
                            type: sourceField.type || (typeof sourceField.getType === 'function' ? sourceField.getType() : 0),
                            pk: fm.sourcePk || false,
                            labelName: sourceField.labelName || null,
                            columnSize: sourceField.columnSize || (typeof sourceField.getColumnSize === 'function' ? sourceField.getColumnSize() : 0),
                            ratio: sourceField.ratio || 0
                        };
                        
                        // 构建目标字段对象
                        const targetFieldObj = {
                            name: targetField.name || (typeof targetField.getName === 'function' ? targetField.getName() : ''),
                            typeName: targetField.typeName || (typeof targetField.getTypeName === 'function' ? targetField.getTypeName() : ''),
                            type: targetField.type || (typeof targetField.getType === 'function' ? targetField.getType() : 0),
                            pk: fm.targetPk || false,
                            labelName: targetField.labelName || null,
                            columnSize: targetField.columnSize || (typeof targetField.getColumnSize === 'function' ? targetField.getColumnSize() : 0),
                            ratio: targetField.ratio || 0
                        };
                        
                        fieldMappingArray.push({
                            source: sourceFieldObj,
                            target: targetFieldObj
                        });
                    }
                });

                // 获取表类型
                const sourceTableType = getTableType(sourceTableName, window.cachedSourceTables);
                const targetTableType = getTableType(targetTableName, window.cachedTargetTables);

                const tableMapping = {
                    sourceTable: {
                        name: sourceTableName,
                        type: sourceTableType
                    },
                    targetTable: {
                        name: targetTableName,
                        type: targetTableType
                    },
                    fieldMapping: fieldMappingArray
                };

                tableMappings.push(tableMapping);
            }
        });

        if (tableMappings.length === 0) {
            bootGrowl("请至少配置一个表映射！", "danger");
            return;
        }

        // 合并第一步和第二步的配置
        const completeConfig = Object.assign({}, step1Data);
        completeConfig.tableMappings = tableMappings;

        console.log('[第二步] 完整配置:', completeConfig);

        // 一次性提交所有配置创建任务
        showLoading();
        $.ajax({
            url: $basePath + "/task/add",
            type: "POST",
            contentType: "application/json",
            dataType: "json",
            data: JSON.stringify({
                name: completeConfig.name,
                type: completeConfig.type,
                json: JSON.stringify(completeConfig),
                STATUS:"0"
            }),
            success: function (result) {
                hideLoading();
                console.log('[第二步] Ajax 响应结果:', result);

                if (result.success === true) {
                    savedTaskId = result.data;
                    bootGrowl("任务创建完成！", "success");
                    console.log('[第二步] 保存成功，任务ID:', savedTaskId);

                    // 返回任务列表
                    setTimeout(function () {
                        backTaskIndexPage();
                    }, 1000);
                } else {
                    bootGrowl(result.message, "danger");
                }
            },
            error: function (xhr, status, info) {
                hideLoading();
                console.error('[第二步] Ajax 请求失败:', status, info);
                bootGrowl("访问异常，请刷新或重试.", "danger");
            }
        });
    }

    /**
     * 获取表类型
     */
    function getTableType(tableName, tables) {
        if (!tables || tables.length === 0) {
            return 'TABLE';
        }
        
        const table = tables.find(function(t) {
            const name = t.name || t;
            return name === tableName;
        });
        
        return table && table.type ? table.type : 'TABLE';
    }

    /**
     * 添加字段映射
     */
    function addFieldMapping(tableIndex) {
        const fieldIndex = fieldMappingIndex[tableIndex]++;

        const fieldMappingHtml = `
            <div class="field-mapping-item" data-field-index="${fieldIndex}"
                style="background: #f9fafb; padding: 12px; border-radius: 8px; border: 1px solid #e5e7eb;">
                <div style="display: grid; grid-template-columns: 3fr 40px 3fr 2.5fr 80px; align-items: center; gap: 12px;">
                    <select class="form-control source-field-select select-control"
                        name="tableMappings[${tableIndex}].fieldMapping[${fieldIndex}].sourceField"
                        data-table-index="${tableIndex}" data-field-index="${fieldIndex}" data-type="source">
                        <option value="">请选择源字段</option>
                    </select>
                    <div style="text-align: center;">
                        <i class="fa fa-arrow-right" style="color: var(--primary-color);"></i>
                    </div>
                    <select class="form-control target-field-select select-control"
                        name="tableMappings[${tableIndex}].fieldMapping[${fieldIndex}].targetField"
                        data-table-index="${tableIndex}" data-field-index="${fieldIndex}" data-type="target">
                        <option value="">请选择目标字段</option>
                    </select>
                    <select class="form-control select-control"
                        name="tableMappings[${tableIndex}].fieldMapping[${fieldIndex}].typeHandler">
                        <option value="DEFAULT" selected>DEFAULT</option>
                        <option value="DATETIME_TO_TIMESTAMP">DATETIME_TO_TIMESTAMP</option>
                        <option value="TIMESTAMP_TO_DATETIME">TIMESTAMP_TO_DATETIME</option>
                    </select>
                    <button type="button" class="dbsyncer-btn dbsyncer-btn-danger remove-field-mapping"
                        style="padding: 8px; font-size: 13px;">
                        <i class="fa fa-trash"></i>
                    </button>
                </div>
            </div>
        `;

        $(`.field-mappings[data-table-index="${tableIndex}"]`).append(fieldMappingHtml);

        // 增强新添加的 select 元素
        if (typeof enhanceAllSelects === 'function') {
            enhanceAllSelects();
        }
    }

    /**
     * 添加表映射行
     */
    function addTableMappingRow() {
        // 获取选中的源表和目标表
        let selectedSourceTables = [];
        let selectedTargetTables = [];

        // 尝试从 dbSelect 组件获取值
        const sourceSelectInstance = $('#step2SourceTable').data('dbSelect');
        const targetSelectInstance = $('#step2TargetTable').data('dbSelect');

        console.log('[添加表映射] 源表实例:', sourceSelectInstance);
        console.log('[添加表映射] 目标表实例:', targetSelectInstance);

        if (sourceSelectInstance && typeof sourceSelectInstance.getValues === 'function') {
            selectedSourceTables = sourceSelectInstance.getValues() || [];
        } else {
            selectedSourceTables = $('#step2SourceTable').val() || [];
        }

        if (targetSelectInstance && typeof targetSelectInstance.getValues === 'function') {
            selectedTargetTables = targetSelectInstance.getValues() || [];
        } else {
            selectedTargetTables = $('#step2TargetTable').val() || [];
        }

        console.log('[添加表映射] 选中的源表:', selectedSourceTables);
        console.log('[添加表映射] 选中的目标表:', selectedTargetTables);

        if (selectedSourceTables.length === 0) {
            bootGrowl("请先选择数据源表！", "warning");
            return;
        }

        if (selectedTargetTables.length === 0) {
            bootGrowl("请先选择目标源表！", "warning");
            return;
        }

        // 获取主键（如果有）
        const sourcePKs = $('#step2SourceTablePK').val() || '';
        const targetPKs = $('#step2TargetTablePK').val() || '';

        let addedCount = 0;
        // 按索引一对一配对创建映射行（而不是笛卡尔积）
        const maxLength = Math.max(selectedSourceTables.length, selectedTargetTables.length);

        for (let i = 0; i < maxLength; i++) {
            // 如果某一方的表不够，则循环使用最后一个
            const sourceTable = selectedSourceTables[Math.min(i, selectedSourceTables.length - 1)];
            const targetTable = selectedTargetTables[Math.min(i, selectedTargetTables.length - 1)];

                // 确保 sourceTable 和 targetTable 是字符串
                const sourceTableName = String(sourceTable);
                const targetTableName = String(targetTable);

            console.log(`[添加表映射] 配对[${i}]:`, sourceTableName, '->', targetTableName);

                // 检查是否已存在相同的映射
                let exists = false;
                $('#tableMappingsContainer tr').each(function() {
                    const existingSource = $(this).find('td:eq(1)').text().trim();
                    const existingTarget = $(this).find('td:eq(2)').text().trim();
                    if (existingSource === sourceTableName && existingTarget === targetTableName) {
                    console.log('[检查重复] 发现重复映射，跳过:', sourceTableName, '->', targetTableName);
                        exists = true;
                        return false; // break
                    }
                });

                if (!exists) {
                    const index = tableMappingIndex++;
                    const rowHtml = `
                        <tr data-mapping-index="${index}"
                            title="点击配置字段映射"
                            class="table-mapping-row"
                            data-source-table="${sourceTableName}"
                            data-target-table="${targetTableName}"
                            data-source-isPk="${sourcePKs}"
                            data-target-isPk="${targetPKs}">
                            <td>${index + 1}</td>
                            <td>${sourceTableName}</td>
                            <td>${targetTableName}</td>
                            <td onclick="event.stopPropagation();">
                                <input type="checkbox" class="table-mapping-checkbox"
                                       onclick="event.stopPropagation();" />
                            </td>
                        </tr>
                    `;
                    console.log('[添加表映射] 生成的HTML:', rowHtml);
                    const $row = $(rowHtml);
                    $('#tableMappingsContainer').append($row);

                    // 自动加载字段映射
                    loadFieldMappingForRow($row, sourceTableName, targetTableName);

                    addedCount++;
                }
        }

        if (addedCount === 0) {
            bootGrowl("选中的表映射已存在，未添加新映射！", "warning");
            return;
        }

        // 清空选择和主键输入
        const sourceInstance = $('#step2SourceTable').data('dbSelect');
        const targetInstance = $('#step2TargetTable').data('dbSelect');

        if (sourceInstance && typeof sourceInstance.clear === 'function') {
            sourceInstance.clear();
        } else {
            $('#step2SourceTable').val(null).trigger('change');
        }

        if (targetInstance && typeof targetInstance.clear === 'function') {
            targetInstance.clear();
        } else {
            $('#step2TargetTable').val(null).trigger('change');
        }

        if (typeof $.fn.tagsinput === 'function') {
            $('#step2SourceTablePK').tagsinput('removeAll');
            $('#step2TargetTablePK').tagsinput('removeAll');
        }

        updateTableMappingIndexes();
        updateEmptyState();
        updateDeleteButtonState();

        bootGrowl(`成功添加 ${addedCount} 个表映射！`, "success");
    }

    /**
     * 为表映射行加载字段映射
     */
    function loadFieldMappingForRow($row, sourceTableName, targetTableName) {
        if (!step1Data) {
            console.warn('[字段映射] 缺少第一步数据');
            return;
        }

        const sourceConnectorId = step1Data.sourceConnectorId;
        const targetConnectorId = step1Data.targetConnectorId;

        console.log('[字段映射] 开始加载字段映射:', sourceTableName, '->', targetTableName);

        // 同时加载源表和目标表的字段
        let sourceFields = [];
        let targetFields = [];
        let loadComplete = 0;

        // 加载源表字段
        $.ajax({
            url: $basePath + "/task/getTableFields",
            type: "GET",
            dataType: "json",
            data: {
                connectorId: sourceConnectorId,
                database: step1Data.sourceDatabaseName || '',
                schema: step1Data.sourceSchema || '',
                tableName: sourceTableName
            },
            success: function (result) {
                if (result.success && result.data) {
                    sourceFields = result.data || [];
                    console.log('[字段映射] 源表字段加载成功:', sourceFields.length);
                }
                loadComplete++;
                if (loadComplete === 2) {
                    saveFieldMappingToRow($row, sourceFields, targetFields);
                }
            },
            error: function () {
                console.error('[字段映射] 源表字段加载失败');
                loadComplete++;
                if (loadComplete === 2) {
                    saveFieldMappingToRow($row, sourceFields, targetFields);
                }
            }
        });

        // 加载目标表字段
        $.ajax({
            url: $basePath + "/task/getTableFields",
            type: "GET",
            dataType: "json",
            data: {
                connectorId: targetConnectorId,
                database: step1Data.targetDatabaseName || '',
                schema: step1Data.targetSchema || '',
                tableName: targetTableName
            },
            success: function (result) {
                if (result.success && result.data) {
                    targetFields = result.data || [];
                    console.log('[字段映射] 目标表字段加载成功:', targetFields.length);
                }
                loadComplete++;
                if (loadComplete === 2) {
                    saveFieldMappingToRow($row, sourceFields, targetFields);
                }
            },
            error: function () {
                console.error('[字段映射] 目标表字段加载失败');
                loadComplete++;
                if (loadComplete === 2) {
                    saveFieldMappingToRow($row, sourceFields, targetFields);
                }
            }
        });
    }

    /**
     * 保存字段映射到表格行
     */
    function saveFieldMappingToRow($row, sourceFields, targetFields) {
        // 自动创建字段映射（同名字段自动匹配）
        const fieldMappings = [];
        sourceFields.forEach(function(sourceField) {
            const sourceFieldName = sourceField.name || sourceField.getName();
            const sourcePk = sourceField.pk || (typeof sourceField.isPk === 'function' ? sourceField.isPk() : false);

            // 查找目标表中同名的字段
            const matchingTarget = targetFields.find(function(targetField) {
                const targetFieldName = targetField.name || targetField.getName();
                return targetFieldName === sourceFieldName;
            });

            if (matchingTarget) {
                const targetPk = matchingTarget.pk || (typeof matchingTarget.isPk === 'function' ? matchingTarget.isPk() : false);

                fieldMappings.push({
                    source: sourceFieldName,
                    target: matchingTarget.name || matchingTarget.getName(),
                    sourcePk: sourcePk,
                    targetPk: targetPk
                });
            }
        });

        console.log('[字段映射] 自动匹配字段:', fieldMappings.length, '个');

        // 保存到行的 data 中
        $row.data('source-fields', sourceFields);
        $row.data('target-fields', targetFields);
        $row.data('field-mappings', fieldMappings);
    }

    /**
     * 打开字段映射配置对话框
     */
    function openFieldMappingDialog($row) {
        const sourceTableName = $row.data('source-table');
        const targetTableName = $row.data('target-table');
        const sourceFields = $row.data('source-fields') || [];
        const targetFields = $row.data('target-fields') || [];
        let fieldMappings = $row.data('field-mappings') || [];


        // 构建源字段下拉选项
        let sourceFieldOptions = '<option value="">请选择源字段</option>';
        sourceFields.forEach(function(field) {
            const fieldName = field.name || field.getName();
            const fieldType = field.typeName || field.getTypeName() || '';
            sourceFieldOptions += `<option value="${fieldName}">${fieldName} (${fieldType})</option>`;
        });

        // 构建目标字段下拉选项
        let targetFieldOptions = '<option value="">请选择目标字段</option>';
        targetFields.forEach(function(field) {
            const fieldName = field.name || field.getName();
            const fieldType = field.typeName || field.getTypeName() || '';
            targetFieldOptions += `<option value="${fieldName}">${fieldName} (${fieldType})</option>`;
        });

        // 构建字段映射行HTML
        function buildFieldMappingRows() {
        if (fieldMappings.length === 0) {
                return '<tr><td colspan="4" style="text-align: center; padding: 20px; color: #999;">暂无字段映射</td></tr>';
            }

            let rows = '';
            fieldMappings.forEach(function(mapping, index) {
                // 只有当源字段和目标字段都是主键时才显示为主键
                const isPk = mapping.sourcePk && mapping.targetPk;
                const pkClass = isPk ? 'pk-active' : '';
                const pkIcon = isPk ? '<i class="fa fa-key" style="color: #f59e0b;"></i>' : '<i class="fa fa-circle-o" style="color: #d1d5db;"></i>';

                rows += `
                    <tr data-mapping-index="${index}" class="field-mapping-row" style="cursor: pointer;">
                        <td>${mapping.source}</td>
                        <td>${mapping.target}</td>
                        <td class="pk-cell ${pkClass}" style="text-align: center;" title="点击设置/取消主键">
                            ${pkIcon}
                        </td>
                        <td style="text-align: center;">
                            <button type="button" class="btn btn-sm btn-danger remove-mapping-btn" data-index="${index}">
                                <i class="fa fa-trash"></i>
                            </button>
                        </td>
                    </tr>
                `;
            });
            return rows;
        }

        // 创建自定义对话框HTML
        const dialogId = 'fieldMappingDialog_' + Date.now();
        const dialogHtml = `
            <style>
                .pk-cell {
                    transition: all 0.2s ease;
                }
                .pk-cell:hover {
                    background-color: #fef3c7 !important;
                    cursor: pointer;
                }
                .pk-cell.pk-active {
                    background-color: #fef3c7;
                }
                .field-mapping-row:hover {
                    background-color: #f9fafb;
                }
            </style>
            <div class="confirm-overlay" id="${dialogId}" style="display: flex; align-items: center; justify-content: center; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 9999;">
                <div class="confirm-dialog size-max" style="background: white; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); max-width: 90%; max-height: 90%; overflow: hidden; display: flex; flex-direction: column;">
                    <!-- 对话框头部 -->
                    <div style="padding: 20px; border-bottom: 1px solid #e5e7eb; display: flex; align-items: center; justify-content: space-between;">
                        <h5 style="margin: 0; font-size: 18px; font-weight: 500;">
                            <i class="fa fa-columns" style="color: var(--primary-color); margin-right: 8px;"></i>
                            字段映射配置
                            <span style="font-size: 13px; color: #999; font-weight: normal; margin-left: 8px;">
                                ${sourceTableName} → ${targetTableName}
                            </span>
                            </h5>
                        <button type="button" class="btn-close-dialog" style="background: none; border: none; font-size: 24px; color: #999; cursor: pointer; padding: 0; width: 30px; height: 30px;">
                            <i class="fa fa-times"></i>
                            </button>
                        </div>

                    <!-- 对话框主体 -->
                    <div style="padding: 20px; flex: 1; overflow-y: auto;">
                        <!-- 手动添加映射区域 -->
                        <div style="background: #f9fafb; padding: 12px; border-radius: 4px; margin-bottom: 12px;">
                            <div style="display: grid; grid-template-columns: 2fr 80px 2fr 100px; gap: 8px; align-items: end;">
                                <div>
                                    <label style="font-size: 12px; color: #666; margin-bottom: 4px; display: block;">源字段</label>
                                    <select id="sourceFieldSelect" class="form-control select-control" style="width: 100%;">
                                        ${sourceFieldOptions}
                                    </select>
                                </div>
                                <div style="text-align: center; padding-top: 20px;">
                                    <i class="fa fa-arrow-right" style="color: var(--primary-color); font-size: 20px;"></i>
                                </div>
                                <div>
                                    <label style="font-size: 12px; color: #666; margin-bottom: 4px; display: block;">目标字段</label>
                                    <select id="targetFieldSelect" class="form-control select-control" style="width: 100%;">
                                        ${targetFieldOptions}
                                    </select>
                                </div>
                                <div>
                                    <button type="button" class="btn btn-sm btn-primary field-action-btn" id="addMappingBtn" style="width: 100%;">
                                        <i class="fa fa-plus"></i> 添加
                                    </button>
                                </div>
                            </div>
                        </div>

                        <!-- 快捷操作按钮 -->
                        <div style="margin-bottom: 12px;">
                            <button type="button" class="btn btn-sm btn-primary field-action-btn" id="autoMatchFieldsBtn">
                                <i class="fa fa-magic"></i> 自动匹配同名字段
                            </button>
                            <button type="button" class="btn btn-sm btn-outline field-action-btn" id="clearFieldMappingsBtn">
                                <i class="fa fa-eraser"></i> 清空映射
                            </button>
                            <span style="margin-left: 12px; font-size: 13px; color: #666;">
                                已映射 <strong id="mappingCount">${fieldMappings.length}</strong> 个字段
                            </span>
                        </div>

                        <!-- 映射列表 -->
                        <div style="max-height: 500px; overflow-y: auto; border: 1px solid #e5e7eb; border-radius: 4px;">
                            <table class="table table-bordered" style="margin: 0;">
                                <thead style="position: sticky; top: 0; background: #f9fafb; z-index: 1;">
                                    <tr>
                                        <th style="width: 40%;">源字段</th>
                                        <th style="width: 40%;">目标字段</th>
                                        <th style="width: 10%; text-align: center;">
                                            <i class="fa fa-key" style="color: var(--warning-color);" title="主键"></i>
                                        </th>
                                        <th style="width: 10%; text-align: center;">操作</th>
                                        </tr>
                                    </thead>
                                    <tbody id="fieldMappingTableBody">
                                    ${buildFieldMappingRows()}
                                    </tbody>
                                </table>
                            </div>
                            </div>

                    <!-- 对话框底部按钮 -->
                    <div style="padding: 16px 20px; border-top: 1px solid #e5e7eb; display: flex; justify-content: flex-end; gap: 12px;">
                        <button type="button" class="btn btn-outline" id="cancelMappingBtn">
                            <i class="fa fa-times"></i> 取消
                        </button>
                        <button type="button" class="btn btn-primary" id="confirmMappingBtn">
                                <i class="fa fa-check"></i> 确定
                            </button>
                    </div>
                </div>
            </div>
        `;

        // 移除旧的对话框
        $('.confirm-overlay').remove();

        // 添加对话框到页面
        $('body').append(dialogHtml);

        // 显示对话框（淡入效果）
        $('#' + dialogId).hide().fadeIn(200);

        // 等待对话框渲染完成后绑定事件（增加延迟确保DOM完全渲染）
        setTimeout(function() {
            console.log('[字段映射对话框] 延迟后开始初始化');

            const $dialog = $('#' + dialogId);
            const $dialogContent = $dialog.find('.confirm-dialog');

            console.log('[字段映射对话框] 找到对话框:', $dialog.length);

            // 阻止对话框内部的点击冒泡
            $dialogContent.off('click').on('click', function(e) {
                e.stopPropagation();
            });

            // 点击 overlay 背景关闭对话框
            $dialog.off('click').on('click', function(e) {
                if (e.target === this) {
                    console.log('[字段映射对话框] 点击背景关闭');
                    $(this).fadeOut(200, function() {
                        $(this).remove();
                    });
                }
            });

            // 绑定关闭按钮
            $('.btn-close-dialog').off('click').on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                console.log('[字段映射对话框] 点击X关闭');
                $dialog.fadeOut(200, function() {
                    $(this).remove();
                });
            });

            // 绑定确定按钮
            $('#confirmMappingBtn').off('click').on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();

                console.log('[字段映射对话框] 点击确定按钮，保存映射:', fieldMappings.length, '个');

                // 保存字段映射到行
                $row.data('field-mappings', fieldMappings);
                bootGrowl("字段映射已保存！", "success");

                // 关闭对话框
                $dialog.fadeOut(200, function() {
                    $(this).remove();
                });
            });

            // 绑定取消按钮
            $('#cancelMappingBtn').off('click').on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();

                console.log('[字段映射对话框] 点击取消按钮');

                // 关闭对话框
                $dialog.fadeOut(200, function() {
                    $(this).remove();
                });
            });

            // 更新映射列表显示
            function updateMappingDisplay() {
                console.log('[更新映射显示] 开始更新，当前映射数量:', fieldMappings.length);

                let rows = '';
                if (fieldMappings.length === 0) {
                    rows = '<tr><td colspan="4" style="text-align: center; padding: 20px; color: #999;">暂无字段映射</td></tr>';
                } else {
                    fieldMappings.forEach(function(mapping, index) {
                        console.log('[更新映射显示] 映射[' + index + ']:', mapping.source, '->', mapping.target, 'sourcePk:', mapping.sourcePk, 'targetPk:', mapping.targetPk);

                        // 只有当源字段和目标字段都是主键时才显示为主键
                        const isPk = mapping.sourcePk && mapping.targetPk;
                        const pkClass = isPk ? 'pk-active' : '';
                        const pkIcon = isPk ? '<i class="fa fa-key" style="color: #f59e0b;"></i>' : '<i class="fa fa-circle-o" style="color: #d1d5db;"></i>';

                        rows += `
                            <tr data-mapping-index="${index}" class="field-mapping-row" style="cursor: pointer;">
                                <td>${mapping.source}</td>
                                <td>${mapping.target}</td>
                                <td class="pk-cell ${pkClass}" style="text-align: center;" title="点击设置/取消主键">
                                    ${pkIcon}
                                </td>
                                <td style="text-align: center;">
                                    <button type="button" class="btn btn-sm btn-danger remove-mapping-btn" data-index="${index}">
                                        <i class="fa fa-trash"></i>
                                    </button>
                                </td>
                            </tr>
                        `;
                    });
                }

                console.log('[更新映射显示] 准备更新表格，行数:', fieldMappings.length);
                $('#fieldMappingTableBody').html(rows);
                $('#mappingCount').text(fieldMappings.length);
                console.log('[更新映射显示] 表格更新完成');

                // 重新绑定删除按钮事件
                $('.remove-mapping-btn').off('click').on('click', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    e.stopImmediatePropagation();

                    const index = $(this).data('index');
                    console.log('[删除映射] 删除索引:', index);
                    fieldMappings.splice(index, 1);
                    updateMappingDisplay();
                    bootGrowl("已删除字段映射", "info");

                    return false;
                });

                // 绑定主键单元格点击事件 - 点击一次同时设置源和目标主键
                $('.pk-cell').off('click').on('click', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    e.stopImmediatePropagation();

                    const $cell = $(this);
                    const $row = $cell.closest('tr');
                    const index = $row.data('mapping-index');

                    console.log('[切换主键] 行索引:', index);

                    // 切换主键状态：如果当前是主键，则取消；如果不是主键，则设置为主键
                    const currentIsPk = fieldMappings[index].sourcePk && fieldMappings[index].targetPk;
                    const newPkState = !currentIsPk;

                    fieldMappings[index].sourcePk = newPkState;
                    fieldMappings[index].targetPk = newPkState;

                    console.log('[切换主键] 新主键状态:', newPkState);

                    // 重新渲染
                    updateMappingDisplay();

                    return false;
                });
            }

            // 初始化日志
            console.log('[字段映射对话框] 开始绑定事件');
            console.log('[字段映射对话框] 找到添加按钮:', $('#addMappingBtn').length);
            console.log('[字段映射对话框] 找到源字段选择器:', $('#sourceFieldSelect').length);
            console.log('[字段映射对话框] 找到目标字段选择器:', $('#targetFieldSelect').length);
            console.log('[字段映射对话框] 找到表格体:', $('#fieldMappingTableBody').length);
            console.log('[字段映射对话框] 找到确定按钮:', $('#confirmMappingBtn').length);
            console.log('[字段映射对话框] 找到取消按钮:', $('#cancelMappingBtn').length);

            // 绑定手动添加映射按钮
            $('#addMappingBtn').off('click').on('click', function(e) {
                // 强制阻止所有默认行为和冒泡
                if (e) {
                    e.preventDefault();
                    e.stopPropagation();
                    e.stopImmediatePropagation();
                }

                console.log('[添加字段映射] ========== 点击添加按钮 ==========');
                console.log('[添加字段映射] 事件对象:', e);
                console.log('[添加字段映射] 按钮元素:', this);
                console.log('[添加字段映射] 按钮ID:', $(this).attr('id'));

                // 检查选择器是否存在
                const $sourceSelect = $('#sourceFieldSelect');
                const $targetSelect = $('#targetFieldSelect');
                console.log('[添加字段映射] 找到源字段选择器:', $sourceSelect.length, '元素:', $sourceSelect[0]);
                console.log('[添加字段映射] 找到目标字段选择器:', $targetSelect.length, '元素:', $targetSelect[0]);

                const sourceField = $sourceSelect.val();
                const targetField = $targetSelect.val();

                console.log('[添加字段映射] 源字段值:', sourceField, '类型:', typeof sourceField);
                console.log('[添加字段映射] 目标字段值:', targetField, '类型:', typeof targetField);
                console.log('[添加字段映射] 源字段选项数量:', $sourceSelect.find('option').length);
                console.log('[添加字段映射] 目标字段选项数量:', $targetSelect.find('option').length);

                if (!sourceField) {
                    bootGrowl("请选择源字段！", "warning");
                    return false;
                }

                if (!targetField) {
                    bootGrowl("请选择目标字段！", "warning");
                    return false;
                }

                // 检查是否已存在相同的映射
                const exists = fieldMappings.some(function(m) {
                    return m.source === sourceField && m.target === targetField;
                });

                if (exists) {
                    bootGrowl("该字段映射已存在！", "warning");
                    return false;
                }

                // 添加新映射
                fieldMappings.push({
                    source: sourceField,
                    target: targetField,
                    sourcePk: false,
                    targetPk: false
                });

                console.log('[添加字段映射] 已添加，当前映射数量:', fieldMappings.length);
                console.log('[添加字段映射] 当前所有映射:', fieldMappings);

                // 重置下拉框
                $('#sourceFieldSelect').val('');
                $('#targetFieldSelect').val('');

                // 更新显示
                updateMappingDisplay();
                console.log('[添加字段映射] 更新显示完成');

                bootGrowl("已添加字段映射！", "success");

                return false;
            });

        // 绑定自动匹配按钮
            $('#autoMatchFieldsBtn').off('click').on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();

            const newMappings = [];
            sourceFields.forEach(function(sourceField) {
                const sourceFieldName = sourceField.name || sourceField.getName();
                const sourcePk = sourceField.pk || (typeof sourceField.isPk === 'function' ? sourceField.isPk() : false);

                const matchingTarget = targetFields.find(function(targetField) {
                    const targetFieldName = targetField.name || targetField.getName();
                    return targetFieldName === sourceFieldName;
                });

                if (matchingTarget) {
                    const targetPk = matchingTarget.pk || (typeof matchingTarget.isPk === 'function' ? matchingTarget.isPk() : false);

                    newMappings.push({
                        source: sourceFieldName,
                        target: matchingTarget.name || matchingTarget.getName(),
                        sourcePk: sourcePk,
                        targetPk: targetPk
                    });
                }
            });

            fieldMappings = newMappings;
                updateMappingDisplay();
            bootGrowl(`已自动匹配 ${newMappings.length} 个字段！`, "success");

                return false;
        });

        // 绑定清空按钮
            $('#clearFieldMappingsBtn').off('click').on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();

            fieldMappings = [];
                updateMappingDisplay();
            bootGrowl("已清空字段映射！", "info");

                return false;
            });

            // 初始化删除按钮事件
            updateMappingDisplay();
        }, 300);
    }

    /**
     * 删除选中的表映射
     */
    function removeSelectedTableMappings() {
        const $checked = $('.table-mapping-checkbox:checked');
        if ($checked.length === 0) {
            bootGrowl("请先选择要删除的表映射！", "warning");
            return;
        }

        $checked.each(function() {
            $(this).closest('tr').remove();
        });

        updateTableMappingIndexes();
        updateEmptyState();
        updateDeleteButtonState();
        bootGrowl("已删除 " + $checked.length + " 个表映射", "success");
    }

    /**
     * 更新删除按钮状态
     */
    function updateDeleteButtonState() {
        const $checked = $('.table-mapping-checkbox:checked');
        if ($checked.length > 0) {
            $('#removeTableMappingBtn').prop('disabled', false).removeClass('btn-outline').addClass('btn-danger');
        } else {
            $('#removeTableMappingBtn').prop('disabled', true).removeClass('btn-danger').addClass('btn-outline');
        }
    }

    /**
     * 更新表映射索引
     */
    function updateTableMappingIndexes() {
        $('#tableMappingsContainer tr').each(function (index) {
            $(this).attr('data-mapping-index', index);
            // 更新序号列（第1列，索引为0）
            $(this).find('td:eq(0)').text(index + 1);
        });

        // 更新 tableMappingIndex
        tableMappingIndex = $('#tableMappingsContainer tr').length;
    }

    /**
     * 更新空状态提示
     */
    function updateEmptyState() {
        // 确保DOM元素存在
        if ($('#tableMappingsContainer').length === 0) {
            return;
        }

        const rowCount = $('#tableMappingsContainer tr').length;
        if (rowCount === 0) {
            $('#tableMappingTable').hide();
            $('#emptyTableMappingTip').show();
            $('#selectAllMappings').prop('checked', false);
        } else {
            $('#tableMappingTable').show();
            $('#emptyTableMappingTip').hide();
        }
    }

    /**
     * 显示Cron表达式帮助
     */
    function showCronHelp() {
        const helpText = `
            <div class="alert alert-info">
                <h4>Cron表达式格式说明：</h4>
                <p>格式：秒 分 时 日 月 周</p>
                <ul>
                    <li>秒：0-59</li>
                    <li>分：0-59</li>
                    <li>时：0-23</li>
                    <li>日：1-31</li>
                    <li>月：1-12 或 JAN-DEC</li>
                    <li>周：0-7 或 SUN-SAT (0和7都表示周日)</li>
                </ul>
                <h5>常用示例：</h5>
                <ul>
                    <li>每天凌晨1点：0 0 1 * * ?</li>
                    <li>每小时执行：0 0 * * * ?</li>
                    <li>每天上午10点：0 0 10 * * ?</li>
                    <li>每周一上午9点：0 0 9 ? * MON</li>
                </ul>
            </div>
        `;

        bootbox.alert({
            title: "Cron表达式帮助",
            message: helpText,
            size: 'large'
        });
    }

    /**
     * 加载数据库列表
     */
    function loadDatabases(connectorId, type) {
        console.log('[加载数据库] 连接器ID:', connectorId, '类型:', type);

        // 显示加载状态
        const selectId = type === 'source' ? '#sourceDatabaseName' : '#targetDatabaseName';
        $(selectId).html('<option value="">加载中...</option>');

        $.ajax({
            url: $basePath + "/task/getDatabases",
            type: "GET",
            dataType: "json",
            data: { connectorId: connectorId },
            success: function (result) {
                console.log('[加载数据库] 响应结果:', result);

                if (result.success) {
                    let options = '<option value="">请选择数据库</option>';

                    if (result.data && result.data.length > 0) {
                        result.data.forEach(function (db) {
                            options += '<option value="' + db + '">' + db + '</option>';
                        });
                        console.log('[加载数据库] 成功加载 ' + result.data.length + ' 个数据库');
                    } else {
                        console.warn('[加载数据库] 数据库列表为空');
                        options = '<option value="">暂无数据库</option>';
                    }

                    $(selectId).html(options);

                    // 增强更新后的 select 元素
                    if (typeof enhanceAllSelects === 'function') {
                        enhanceAllSelects();
                    }

                    // 加载表列表
                    loadTables(connectorId, type);
                } else {
                    console.error('[加载数据库] 失败:', result.data);
                    $(selectId).html('<option value="">加载失败</option>');
                    bootGrowl("获取数据库列表失败: " + result.data, "danger");
                }
            },
            error: function (xhr, status, error) {
                console.error('[加载数据库] 请求异常:', status, error);
                $(selectId).html('<option value="">加载失败</option>');
                bootGrowl("获取数据库列表失败: " + error, "danger");
            }
        });
    }

    /**
     * 加载表列表
     */
    function loadTables(connectorId, type) {
        $.ajax({
            url: $basePath + "/task/getTables",
            type: "GET",
            dataType: "json",
            data: {
                connectorId: connectorId,
                database: '',
                schema: ''
            },
            success: function (result) {
                if (result.success) {
                    const tableSelects = type === 'source' ? '.source-table-select' : '.target-table-select';

                    $(tableSelects).each(function () {
                        let options = '<option value="">请选择表</option>';

                        if (result.data && result.data.length > 0) {
                            result.data.forEach(function (table) {
                                options += '<option value="' + table.name + '">' + table.name + '</option>';
                            });
                        }

                        $(this).html(options);
                    });

                    // 增强更新后的 select 元素
                    if (typeof enhanceAllSelects === 'function') {
                        enhanceAllSelects();
                    }
        } else {
                    bootGrowl("获取表列表失败: " + result.data, "danger");
                }
            },
            error: function () {
                bootGrowl("获取表列表失败", "danger");
            }
        });
    }

    /**
     * 加载表字段
     */
    function loadTableFields(connectorId, tableName, tableIndex, type) {
        if (!step1Data) {
            console.warn('[加载字段] 缺少第一步数据');
            return;
        }

        // 根据类型选择对应的database和schema
        const database = type === 'source' ? step1Data.sourceDatabaseName : step1Data.targetDatabaseName;
        const schema = type === 'source' ? step1Data.sourceSchema : step1Data.targetSchema;

        $.ajax({
            url: $basePath + "/task/getTableFields",
            type: "GET",
            dataType: "json",
            data: {
                connectorId: connectorId,
                database: database || '',
                schema: schema || '',
                tableName: tableName
            },
            success: function (result) {
                if (result.success) {
                    const fieldSelects = type === 'source' ?
                        '.source-field-select[data-table-index="' + tableIndex + '"]' :
                        '.target-field-select[data-table-index="' + tableIndex + '"]';

                    $(fieldSelects).each(function () {
                        let options = '<option value="">请选择字段</option>';

                        if (result.data && result.data.length > 0) {
                            result.data.forEach(function (field) {
                                const fieldName = field.name || field.getName();
                                const fieldType = field.typeName || field.getTypeName();
                                options += '<option value="' + fieldName + '">' +
                                    fieldName + ' (' + fieldType + ')</option>';
                            });
                        }

                        $(this).html(options);
                    });

                    // 增强更新后的 select 元素
                    if (typeof enhanceAllSelects === 'function') {
                        enhanceAllSelects();
                    }
                } else {
                    bootGrowl("获取表字段失败: " + result.data, "danger");
                }
            },
            error: function () {
                bootGrowl("获取表字段失败", "danger");
            }
        });
    }

    /**
     * 清空表字段
     */
    function clearTableFields(tableIndex, type) {
        const fieldSelects = type === 'source' ?
            '.source-field-select[data-table-index="' + tableIndex + '"]' :
            '.target-field-select[data-table-index="' + tableIndex + '"]';

        $(fieldSelects).html('<option value="">请选择字段</option>');
    }

    /**
     * 检查并显示Schema输入框
     */
    function checkAndShowSchema(connectorId, type) {
        $.ajax({
            url: $basePath + "/task/getConnectorType",
            type: "GET",
            dataType: "json",
            data: { connectorId: connectorId },
            success: function (result) {
                if (result.success) {
                    const connectorType = result.data.toLowerCase();
                    let showSchema = false;
                    let defaultSchema = '';

                    if (connectorType.includes('oracle')) {
                        showSchema = true;
                        defaultSchema = 'SYSTEM';
                    } else if (connectorType.includes('postgresql')) {
                        showSchema = true;
                        defaultSchema = 'public';
                    } else if (connectorType.includes('sqlserver')) {
                        showSchema = true;
                        defaultSchema = 'dbo';
                    }

                    if (showSchema) {
                        $('#sourceSchemaGroup').show();
                        if (type === 'source') {
                            $('#sourceSchema').val(defaultSchema);
                        } else {
                            $('#targetSchema').val(defaultSchema);
                        }
                    } else {
                        $('#sourceSchemaGroup').hide();
                    }
                }
            },
            error: function () {
                console.error("获取连接器类型失败");
            }
        });
    }

    /**
     * 构建JSON配置
     */
    function buildTaskConfig() {
        const formData = $('#taskAddForm').serializeArray();
        const config = {};

        // 基础配置
        formData.forEach(function (item) {
            if (item.name.includes('tableMappings')) {
                // 表映射配置，稍后处理
                return;
            }

            if (item.name === 'trigger') {
                config[item.name] = item.value;
            } else if (item.name === 'cron' && config.trigger === 'timing') {
                config[item.name] = item.value;
            } else if (['autoMatchTable', 'verification', 'correction', 'tableStructure', 'rowData', 'index', 'triggerFlag', 'function', 'storedProcedure'].includes(item.name)) {
                config[item.name] = item.value === 'true';
            } else {
                config[item.name] = item.value;
            }
        });

        // 处理表映射
        config.tableMappings = [];
        $('.table-mapping-item').each(function () {
            const tableMapping = {
                sourceTable: {
                    name: $(this).find('select[name*="sourceTable.name"]').val(),
                    schema: $(this).find('input[name*="sourceTable.schema"]').val(),
                    columns: []
                },
                targetTable: {
                    name: $(this).find('select[name*="targetTable.name"]').val(),
                    schema: $(this).find('input[name*="targetTable.schema"]').val(),
                    columns: []
                },
                fieldMapping: []
            };

            // 处理字段映射
            $(this).find('.field-mapping-item').each(function () {
                const sourceField = $(this).find('select[name*="sourceField"]').val();
                const targetField = $(this).find('select[name*="targetField"]').val();
                const typeHandler = $(this).find('select[name*="typeHandler"]').val();

                if (sourceField && targetField) {
                    tableMapping.fieldMapping.push({
                        sourceField: sourceField,
                        targetField: targetField,
                        typeHandler: typeHandler
                    });
                }
            });

            if (tableMapping.sourceTable.name && tableMapping.targetTable.name) {
                config.tableMappings.push(tableMapping);
            }
        });

        return config;
    }

    /**
     * 返回任务列表页面
     */
    function backTaskIndexPage() {
        doLoader("/task?refresh=" + new Date().getTime());
    }
