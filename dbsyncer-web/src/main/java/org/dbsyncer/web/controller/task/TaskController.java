/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.task;

import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author wuji
 * @Version 1.0.0
 * @Date 2025-10-18 19:52
 */

@Controller
@RequestMapping("/task")
public class TaskController {


    private static final Logger log = LoggerFactory.getLogger(TaskController.class);
    @Resource
    TaskService taskService;

    @PostMapping("/add")
    @ResponseBody
    public RestResult add(@RequestBody Map<String, String> params) {

        Map<String, String> param = new HashMap<>();
        param.put("name", "订单数据同步任务");
        param.put("status", "1");
        param.put("type", "123");
        param.put("json", "{\"sourceConnectorId\":\"conn_mysql_source_001\",\"sourceDatabaseName\":\"order_db\",\"targetConnectorId\":\"conn_postgres_target_001\",\"targetDatabaseName\":\"order_db_copy\",\"trigger\":\"timing\",\"cron\":\"0 0 1 * * ?\",\"autoMatchTable\":false,\"verification\":true,\"correction\":false,\"tableStructure\":true,\"rowData\":true,\"index\":false,\"triggerFlag\":false,\"function\":false,\"storedProcedure\":false,\"tableMappings\":[{\"sourceTable\":{\"name\":\"order_main\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_no\",\"type\":\"VARCHAR(50)\"},{\"name\":\"create_time\",\"type\":\"DATETIME\"}]},\"targetTable\":{\"name\":\"order_main_backup\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_no\",\"type\":\"VARCHAR(50)\"},{\"name\":\"create_time\",\"type\":\"TIMESTAMP\"}]},\"fieldMapping\":[{\"sourceField\":\"id\",\"targetField\":\"id\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"order_no\",\"targetField\":\"order_no\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"create_time\",\"targetField\":\"create_time\",\"typeHandler\":\"DATETIME_TO_TIMESTAMP\"}]},{\"sourceTable\":{\"name\":\"order_item\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_id\",\"type\":\"INT\"},{\"name\":\"product_name\",\"type\":\"VARCHAR(100)\"}]},\"targetTable\":{\"name\":\"order_item_backup\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_id\",\"type\":\"INT\"},{\"name\":\"product_name\",\"type\":\"VARCHAR(100)\"}]},\"fieldMapping\":[{\"sourceField\":\"id\",\"targetField\":\"id\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"order_id\",\"targetField\":\"order_id\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"product_name\",\"targetField\":\"product_name\",\"typeHandler\":\"DEFAULT\"}]}]}");
        param.put("createTime", DateFormatUtil.dateToString(new Date()));
        param.put("updateTime", DateFormatUtil.dateToString(new Date()));

        try {
            taskService.add(param);
        } catch (Exception e) {
            return RestResult.restFail("新增失败");
        }
        return RestResult.restSuccess("新增成功");
    }


    @PostMapping("/modify")
    @ResponseBody
    public RestResult modify(@RequestBody Map<String, String> params) {
        try {
            taskService.modify(params);
        } catch (Exception e) {
            return RestResult.restFail("修改失败");
        }
        return RestResult.restSuccess("修改成功");
    }

    @GetMapping("/delete")
    public RestResult delete(String taskId) {
        try {
            taskService.delete(taskId);
        } catch (Exception e) {
            return RestResult.restFail("删除失败");
        }
        return RestResult.restSuccess("删除成功");
    }

    @PostMapping("/list")
    @ResponseBody
    public Paging list(@RequestBody Map<String, String> params) {
        try {
            return taskService.list(params);
        } catch (Exception e) {
            log.error("查询任务列表失败", e);
            return new Paging(0, 0);
        }
    }

    @PostMapping("/result")
    @ResponseBody
    public Paging result(@RequestBody Map<String, String> params) {
        try {
            return taskService.result(params);
        } catch (Exception e) {
            log.error("查询任务结果失败", e);
            return new Paging(0, 0);
        }
    }

}
