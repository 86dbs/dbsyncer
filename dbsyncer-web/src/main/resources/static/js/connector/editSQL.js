$(function () {
    // 添加SQL配置项
    let $addSqlTableBtn = $("#addSqlTableBtn");
    $addSqlTableBtn.click(function () {
        $addSqlTableBtn.prop('disabled', true);
        let $sqlTableSelectParams = $("#sqlTableSelectParams");
        let json = $sqlTableSelectParams.val();
        let jsonArray = [];
        if(!isBlank(json)){
            jsonArray = JSON.parse(json);
        }

        // 校验参数
        let $sqlName = $("#sqlName");
        if(isBlank($sqlName.val())){
            bootGrowl("SQL名称不能空.", "danger");
            $addSqlTableBtn.removeAttr('disabled');
            return;
        }
        let $table = $("#table");
        if(isBlank($table.val())){
            bootGrowl("主表不能空.", "danger");
            $addSqlTableBtn.removeAttr('disabled');
            return;
        }
        let $sql = $("#sql");
        if(isBlank($sql.val())){
            bootGrowl("SQL不能空.", "danger");
            $addSqlTableBtn.removeAttr('disabled');
            return;
        }
        // 重复校验

        // 暂存配置
        jsonArray.push({"sqlName": $sqlName.val(), "table": $table.val(), "sql": $sql.val()});
        $sqlTableSelectParams.val(JSON.stringify(jsonArray));

        $sqlName.val("");
        $table.val("");
        $sql.val("");
        $addSqlTableBtn.removeAttr('disabled');
    });


})