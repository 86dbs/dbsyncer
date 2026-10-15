/**
 * 业务分组工具栏：列表筛选 / 新建·编辑·删除分组；关联资源在分组弹窗维护。
 * 约定：列表筛 groupId 空=全部；资源与分组多对多。
 * 顶栏布局见《分组设计》§7.2。
 */
(function (window, $) {
    'use strict';

    var groupsCache = null;

    function escapeHtml(text) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(text);
        }
        var div = document.createElement('div');
        div.textContent = text == null ? '' : String(text);
        return div.innerHTML;
    }

    function loadGroups(callback) {
        doPoster('/group/list', {}, function (response) {
            if (!response || !response.success) {
                bootGrowl((response && response.message) || '加载分组失败', 'danger');
                callback([]);
                return;
            }
            groupsCache = response.data || [];
            callback(groupsCache);
        });
    }

    /**
     * 列表筛选选项：全部 + 真实分组（不含「未分组」）。
     */
    function buildFilterData(groups) {
        var data = [{ label: '全部', value: '' }];
        (groups || []).forEach(function (g) {
            if (!g || !g.id) {
                return;
            }
            data.push({ label: g.name || '', value: g.id });
        });
        return data;
    }

    function isRealGroupId(groupId) {
        return !!groupId;
    }

    function resolveSelected(groups, preferId) {
        var selected = preferId != null ? preferId : '';
        var list = groups || [];
        if (isRealGroupId(selected) && !list.some(function (g) {
            return g.id === selected;
        })) {
            return '';
        }
        return selected || '';
    }

    function findGroupLabel(data, value) {
        var item = (data || []).filter(function (d) {
            return d.value === value;
        })[0];
        return item ? item.label : '';
    }

    /**
     * 初始化或刷新单选 dbSelect，并静默选中指定值（含空串「全部」）。
     */
    function applySelectData($select, api, data, selected, onSelect) {
        if (!api) {
            api = $select.dbSelect({
                type: 'single',
                data: data,
                defaultValue: [selected],
                onSelect: function (values) {
                    if (typeof onSelect === 'function') {
                        onSelect((values && values[0]) != null ? values[0] : '');
                    }
                }
            });
            if (api && selected === '') {
                api.setValues([''], true);
            }
            return api;
        }
        api.setData(data);
        api.setValues([selected], true);
        return api;
    }

    /**
     * 关联资源多选：远程分页；groupId 空=全部资源，便于勾选挂组。
     */
    function buildResourceMultiSelectOptions(searchUrl, groupIdParam, preselected) {
        var initialData = preselected || [];
        var defaultValues = initialData.map(function (item) {
            return item.value;
        });
        return {
            type: 'multiple',
            remoteSearch: true,
            pageSize: 50,
            keywordDebounceMs: 300,
            data: initialData.slice(),
            defaultValue: defaultValues,
            loadOptions: function (query, onSuccess, onError) {
                doPoster(searchUrl, {
                    pageNum: query.pageNum,
                    pageSize: query.pageSize,
                    searchKey: query.searchKey || '',
                    groupId: groupIdParam
                }, function (res) {
                    if (!res || res.success !== true) {
                        if (typeof onError === 'function') {
                            onError(res);
                        }
                        return;
                    }
                    var paging = res.data || {};
                    var rows = Array.isArray(paging.data) ? paging.data : [];
                    var data = [];
                    rows.forEach(function (row) {
                        if (row && row.id) {
                            data.push({label: row.name || row.id, value: row.id});
                        }
                    });
                    onSuccess({
                        pageNum: paging.pageNum || query.pageNum,
                        pageSize: paging.pageSize || query.pageSize,
                        total: paging.total || 0,
                        data: data
                    });
                });
            }
        };
    }

    function destroySelectApi(api) {
        if (api && typeof api.destroy === 'function') {
            api.destroy();
        }
    }

    function bindBatch(groupId, resourceType, resourceIds, done) {
        if (!resourceIds || !resourceIds.length) {
            if (typeof done === 'function') {
                done(true);
            }
            return;
        }
        doPoster('/group/bindBatch', {
            groupId: groupId || '',
            type: resourceType,
            resourceIds: resourceIds.join(',')
        }, function (response) {
            if (!response || !response.success) {
                bootGrowl((response && response.message) || '关联资源失败', 'danger');
                if (typeof done === 'function') {
                    done(false);
                }
                return;
            }
            if (typeof done === 'function') {
                done(true);
            }
        });
    }

    /** 从指定分组移出（不影响其它组） */
    function unbindBatch(groupId, resourceType, resourceIds, done) {
        if (!groupId || !resourceIds || !resourceIds.length) {
            if (typeof done === 'function') {
                done(true);
            }
            return;
        }
        doPoster('/group/unbindBatch', {
            groupId: groupId,
            type: resourceType,
            resourceIds: resourceIds.join(',')
        }, function (response) {
            if (!response || !response.success) {
                bootGrowl((response && response.message) || '移出资源失败', 'danger');
                if (typeof done === 'function') {
                    done(false);
                }
                return;
            }
            if (typeof done === 'function') {
                done(true);
            }
        });
    }

    /**
     * 加载某组全部关联资源（作编辑弹窗预勾选）。
     */
    function loadGroupResources(searchUrl, groupId, callback) {
        if (!groupId || !searchUrl) {
            callback([]);
            return;
        }
        doPoster(searchUrl, {
            pageNum: 1,
            pageSize: 500,
            searchKey: '',
            groupId: groupId
        }, function (res) {
            if (!res || res.success !== true) {
                callback([]);
                return;
            }
            var paging = res.data || {};
            var rows = Array.isArray(paging.data) ? paging.data : [];
            var data = [];
            rows.forEach(function (row) {
                if (row && row.id) {
                    data.push({label: row.name || row.id, value: row.id});
                }
            });
            callback(data);
        });
    }

    function diffIds(initialIds, finalIds) {
        var initialSet = {};
        (initialIds || []).forEach(function (id) {
            initialSet[id] = true;
        });
        var finalSet = {};
        (finalIds || []).forEach(function (id) {
            finalSet[id] = true;
        });
        var toAdd = [];
        var toRemove = [];
        (finalIds || []).forEach(function (id) {
            if (!initialSet[id]) {
                toAdd.push(id);
            }
        });
        (initialIds || []).forEach(function (id) {
            if (!finalSet[id]) {
                toRemove.push(id);
            }
        });
        return {toAdd: toAdd, toRemove: toRemove};
    }

    /**
     * 列表顶栏分组区。
     * @param {Object} options
     *   - containerSelector 筛选/编辑/删除挂载容器（搜索框右侧）
     *   - createButtonHostSelector 「分组」按钮挂载点（资源「添加」左侧）
     *   - resourceType / searchUrl / onChange
     *   - addPageUrl / addButtonSelector 可选
     */
    function initListToolbar(options) {
        options = options || {};
        var $container = $(options.containerSelector);
        if (!$container.length) {
            console.warn('[GroupToolbar] container not found:', options.containerSelector);
            return null;
        }

        var resourceType = options.resourceType || '';
        var searchUrl = options.searchUrl || '';
        var currentGroupId = '';
        var listTotal = 0;
        var hasGroups = false;
        var filterData = [];
        var selectApi = null;
        var silentChange = false;

        $container.html(
            '<div class="group-toolbar-inner flex items-center gap-2">' +
            '<div class="group-filter-wrap hidden flex-shrink-0 min-w-160">' +
            '<select class="form-control group-filter-select"></select>' +
            '</div>' +
            '<button type="button" class="btn btn-outline group-edit-btn hidden flex-shrink-0" title="编辑分组"><i class="fa fa-edit"></i></button>' +
            '<button type="button" class="btn btn-outline group-remove-btn hidden flex-shrink-0" title="删除分组"><i class="fa fa-trash"></i></button>' +
            '</div>'
        );
        var $filterWrap = $container.find('.group-filter-wrap');
        var $select = $container.find('.group-filter-select');
        var $editBtn = $container.find('.group-edit-btn');
        var $removeBtn = $container.find('.group-remove-btn');

        // 「分组」按钮挂在资源「添加」旁
        var $createHost = options.createButtonHostSelector ? $(options.createButtonHostSelector) : $();
        var $addBtn;
        if ($createHost.length) {
            $createHost.html(
                '<button type="button" class="btn btn-outline group-add-btn hidden flex-shrink-0" title="新建分组">分组</button>'
            );
            $addBtn = $createHost.find('.group-add-btn');
        } else {
            $addBtn = $();
        }

        function syncVisibility() {
            var showFilter = hasGroups;
            var showEditRemove = hasGroups && isRealGroupId(currentGroupId);
            var showAdd = listTotal > 0;
            $filterWrap.toggleClass('hidden', !showFilter);
            $editBtn.toggleClass('hidden', !showEditRemove);
            $removeBtn.toggleClass('hidden', !showEditRemove);
            $addBtn.toggleClass('hidden', !showAdd);
            // 中间筛选区：无下拉且无编辑删除时隐藏
            $container.toggleClass('hidden', !showFilter && !showEditRemove);
            if ($createHost.length) {
                $createHost.toggleClass('hidden', !showAdd);
            }
        }

        function notifyChange() {
            if (silentChange) {
                return;
            }
            if (typeof options.onChange === 'function') {
                options.onChange(currentGroupId);
            }
        }

        function onFilterSelect(value) {
            if (silentChange) {
                return;
            }
            currentGroupId = value || '';
            syncVisibility();
            notifyChange();
        }

        function refreshSelect(preferId) {
            loadGroups(function (groups) {
                hasGroups = (groups || []).length > 0;
                var selected = resolveSelected(groups, preferId != null ? preferId : currentGroupId);
                currentGroupId = selected;
                filterData = buildFilterData(groups);
                // 先显隐再初始化 dbSelect，避免父级 display:none 时宽度为 0
                syncVisibility();
                if (hasGroups) {
                    silentChange = true;
                    selectApi = applySelectData($select, selectApi, filterData, selected, onFilterSelect);
                    silentChange = false;
                    // 再同步一次：确保「全部」时编辑/删除仍隐藏
                    syncVisibility();
                } else {
                    destroySelectApi(selectApi);
                    selectApi = null;
                    $select.empty();
                    currentGroupId = '';
                    syncVisibility();
                }
            });
        }

        function openCreateDialog() {
            if (!searchUrl || !resourceType) {
                bootGrowl('未配置资源类型，无法新建分组', 'danger');
                return;
            }
            var resourceApi = null;
            var dialog = showConfirm({
                title: '新建分组',
                size: 'large',
                position: 'center',
                body: '<div class="group-dialog-body">' +
                    '<div class="form-item"><label class="form-label">名称<strong class="text-primary">*</strong></label>' +
                    '<div class="form-control-area"><input type="text" class="form-control group-name-input" maxlength="50" placeholder="请输入分组名称"/></div></div>' +
                    '<div class="form-item mt-4"><label class="form-label">关联资源</label>' +
                    '<select class="form-control group-resource-select"></select></div>' +
                    '</div>',
                confirmText: '确定',
                cancelText: '取消',
                closeOnConfirm: false,
                onConfirm: function () {
                    var name = ($('.group-name-input').val() || '').trim();
                    if (!name) {
                        bootGrowl('分组名称不能为空', 'warning');
                        return;
                    }
                    var selectedIds = resourceApi && resourceApi.getValues ? resourceApi.getValues().slice() : [];
                    doPoster('/group/add', {name: name}, function (response) {
                        if (!response.success) {
                            bootGrowl(response.message || '新建失败', 'danger');
                            return;
                        }
                        var newId = response.data || '';
                        bindBatch(newId, resourceType, selectedIds, function (ok) {
                            if (!ok) {
                                return;
                            }
                            destroySelectApi(resourceApi);
                            dialog.close();
                            bootGrowl('新建成功', 'success');
                            currentGroupId = newId;
                            refreshSelect(newId);
                            notifyChange();
                        });
                    });
                },
                onCancel: function () {
                    destroySelectApi(resourceApi);
                }
            });
            setTimeout(function () {
                $('.group-name-input').focus();
                resourceApi = $('.group-resource-select').dbSelect(
                    buildResourceMultiSelectOptions(searchUrl, '', [])
                );
            }, 0);
        }

        function openEditDialog() {
            if (!isRealGroupId(currentGroupId) || !searchUrl || !resourceType) {
                return;
            }
            var groupId = currentGroupId;
            var groupName = findGroupLabel(filterData, groupId) || groupId;
            loadGroupResources(searchUrl, groupId, function (preselected) {
                var initialIds = preselected.map(function (item) {
                    return item.value;
                });
                var resourceApi = null;
                var dialog = showConfirm({
                    title: '编辑分组',
                    size: 'large',
                    position: 'center',
                    body: '<div class="group-dialog-body">' +
                        '<div class="form-item"><label class="form-label">名称<strong class="text-primary">*</strong></label>' +
                        '<div class="form-control-area"><input type="text" class="form-control group-name-input" maxlength="50" value="' +
                        escapeHtml(groupName) + '"/></div></div>' +
                        '<div class="form-item mt-4"><label class="form-label">关联资源</label>' +
                        '<select class="form-control group-resource-select"></select></div>' +
                        '</div>',
                    confirmText: '保存',
                    cancelText: '取消',
                    closeOnConfirm: false,
                    onConfirm: function () {
                        var name = ($('.group-name-input').val() || '').trim();
                        if (!name) {
                            bootGrowl('分组名称不能为空', 'warning');
                            return;
                        }
                        var finalIds = resourceApi && resourceApi.getValues ? resourceApi.getValues().slice() : [];
                        var diff = diffIds(initialIds, finalIds);
                        doPoster('/group/edit', {id: groupId, name: name}, function (editRes) {
                            if (!editRes.success) {
                                bootGrowl(editRes.message || '保存失败', 'danger');
                                return;
                            }
                            bindBatch(groupId, resourceType, diff.toAdd, function (addOk) {
                                if (!addOk) {
                                    return;
                                }
                                unbindBatch(groupId, resourceType, diff.toRemove, function (removeOk) {
                                    if (!removeOk) {
                                        return;
                                    }
                                    destroySelectApi(resourceApi);
                                    dialog.close();
                                    bootGrowl('保存成功', 'success');
                                    refreshSelect(groupId);
                                    notifyChange();
                                });
                            });
                        });
                    },
                    onCancel: function () {
                        destroySelectApi(resourceApi);
                    }
                });
                setTimeout(function () {
                    $('.group-name-input').focus();
                    // 下拉浏览全部资源；已选标签展示组内资源，可叉掉移出
                    resourceApi = $('.group-resource-select').dbSelect(
                        buildResourceMultiSelectOptions(searchUrl, '', preselected)
                    );
                }, 0);
            });
        }

        $addBtn.on('click', openCreateDialog);
        $editBtn.on('click', openEditDialog);

        $removeBtn.on('click', function () {
            if (!isRealGroupId(currentGroupId)) {
                return;
            }
            var groupName = findGroupLabel(filterData, currentGroupId) || currentGroupId;
            doPoster('/group/memberCount', {id: currentGroupId}, function (countRes) {
                var count = 0;
                if (countRes && countRes.success) {
                    var data = countRes.data || {};
                    count = data.count != null ? data.count : 0;
                }
                showConfirm({
                    title: '确定要删除分组「' + escapeHtml(groupName) + '」？',
                    message: '该分组下共 ' + count + ' 个资源，删除后仅解除本组成属关系（资源本身及其它组归属保留）。',
                    icon: 'warning',
                    confirmType: 'danger',
                    confirmText: '删除',
                    onConfirm: function () {
                        doPoster('/group/remove', {id: currentGroupId}, function (response) {
                            if (!response.success) {
                                bootGrowl(response.message || '删除失败', 'danger');
                                return;
                            }
                            bootGrowl('删除成功', 'success');
                            currentGroupId = '';
                            refreshSelect('');
                            notifyChange();
                        });
                    }
                });
            });
        });

        if (options.addButtonSelector && options.addPageUrl) {
            $(document).off('click.groupAdd', options.addButtonSelector)
                .on('click.groupAdd', options.addButtonSelector, function (e) {
                    e.preventDefault();
                    doLoader(options.addPageUrl);
                });
        }

        syncVisibility();
        refreshSelect('');
        return {
            getGroupId: function () {
                return currentGroupId;
            },
            getSearchParams: function (extra) {
                var params = $.extend({}, extra || {});
                // 必须显式带上 groupId（含空串），否则 PaginationManager 会合并保留上次的 groupId
                params.groupId = currentGroupId || '';
                return params;
            },
            setListTotal: function (total) {
                listTotal = Number(total) || 0;
                syncVisibility();
            },
            refresh: refreshSelect
        };
    }

    window.GroupToolbar = {
        initListToolbar: initListToolbar,
        isRealGroupId: isRealGroupId
    };
})(window, jQuery);
