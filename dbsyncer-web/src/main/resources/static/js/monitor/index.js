//切换连接器
function changeMetaData($this) {
    var $id = $this.val();
    $initContainer.load('/monitor?id=' + $id);
}

$(function () {
    // 初始化select2插件
    $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });

    //连接器类型切换事件
    $("select[name='metaData']").change(function () {
        changeMetaData($(this));
    });

});