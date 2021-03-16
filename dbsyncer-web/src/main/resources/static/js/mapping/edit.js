//*********************************** 驱动保存 开始位置***********************************//
function submit(data) {
    doPoster("/mapping/edit", data, function (data) {
        if (data.success == true) {
            bootGrowl("修改驱动成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}
//*********************************** 驱动保存 结束位置***********************************//
// 刷新页面
function refresh(id){
    doLoader('/mapping/page/edit?id=' + id);
}

// 绑定修改驱动同步方式切换事件
function bindMappingModelChange() {
    var $mappingModelChange = $("#mappingModelChange");
    var $radio = $mappingModelChange.find('input:radio[type="radio"]');
    // 初始化icheck插件
    $radio.iCheck({
        labelHover: false,
        cursor: true,
        radioClass: 'iradio_flat-blue',
    }).on('ifChecked', function (event) {
        showMappingEditConfig($(this).val());
    });

    // 渲染选择radio配置
    var value = $mappingModelChange.find('input[type="radio"]:checked').val();
    showMappingEditConfig(value);
}

// 绑定删除表关系复选框删除事件
function bindMappingTableGroupDelClick(){
    var $mappingTableGroupList = $("#mappingTableGroupList");
    var $checkbox = $mappingTableGroupList.find('input:checkbox[type="checkbox"]');
    // 初始化icheck插件
    $checkbox.iCheck({
        checkboxClass: 'icheckbox_square-red',
        labelHover: false,
        cursor: true
    }).on('ifChecked', function (event) {
        var length = $('.ck_tags input:checked').length;
        if (length > 0) {
            // 显示删除删除按钮

        }
        console.log(length);
    }).on('ifUnchecked', function (event) {
        var length = $('.ck_tags input:checked').length;
        if (length < 1) {
            // 隐藏删除删除按钮
        }
        console.log(length);
    });
}

// 显示驱动编辑配置（全量/增量）
function showMappingEditConfig($value) {
    var $full = $("#mappingFullConfig");
    var $increment = $("#mappingIncrementConfig");

    if ('full' == $value) {
        $increment.addClass("hidden");
        $full.removeClass("hidden");
    } else {
        $full.addClass("hidden");
        $increment.removeClass("hidden");
    }
}

// 绑定表关系点击事件
function bindMappingTableGroupListClick() {
    var $tableGroupList = $("#tableGroupList");
    $tableGroupList.unbind("click");
    $tableGroupList.find("tr").bind('click', function () {
        doLoader('/tableGroup/page/editTableGroup?id=' + $(this).attr("id"));
    });

    var $del = $(".tableGroupDelete");
    $del.unbind("click");
    $del.bind('click', function () {
        // 阻止tr触发click事件
        event.cancelBubble = true;
        var $url = "/tableGroup/remove?id=" + $(this).attr("id");
        var $mappingId = $(this).attr("mappingId");
        doPoster($url, {}, function (data) {
            if (data.success == true) {
                bootGrowl("删除映射关系成功!", "success");
                refresh($mappingId);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

// 绑定新增表关系点击事件
function bindMappingTableGroupAddClick() {
    var $tableGroupAdd = $("#tableGroupAdd");
    $tableGroupAdd.unbind("click");
    $tableGroupAdd.bind('click', function () {
        var m = {};
        m.mappingId = $(this).attr("mappingId");
        m.sourceTable = $("#sourceTable option:checked").val();
        m.targetTable = $("#targetTable option:checked").val();
        doPoster("/tableGroup/add", m, function (data) {
            if (data.success == true) {
                bootGrowl("新增映射关系成功!", "success");
                refresh(m.mappingId);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

// 绑定下拉自动匹配字段
function bindAutoSelect(){
    var $sourceSelect = $("#sourceTable");
    var $targetSelect = $("#targetTable");

    // 绑定数据源下拉切换事件
    $sourceSelect.change(function () {
        var v = $(this).select2("val");
        $targetSelect.val(v).trigger("change");
    });
}

// 修改驱动名称
function mappingModifyName(){
    var $name = $("#mappingModifyName");
    var tmp = $name.text();
    $name.text("");
    $name.append("<input type='text'/>");
    var $input = $name.find("input");
    $input.focus().val(tmp);
    $input.blur(function(){
        $name.text($(this).val());
        $("#mappingModifyForm input[name='name']").val($(this).val());
        $input.unbind();
    });
}

$(function () {
    // 绑定同步方式切换事件
    bindMappingModelChange();
    // 绑定删除表映射事件
    bindMappingTableGroupDelClick();

    // 绑定表关系点击事件
    bindMappingTableGroupListClick();
    // 绑定新增表关系点击事件
    bindMappingTableGroupAddClick();

    // 绑定下拉自动匹配字段
    bindAutoSelect();

    // 初始化select2插件
    $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });

    //保存
    $("#mappingSubmitBtn").click(function () {
        var $form = $("#mappingModifyForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    //返回
    $("#mappingBackBtn").click(function () {
        backIndexPage();
    });
})