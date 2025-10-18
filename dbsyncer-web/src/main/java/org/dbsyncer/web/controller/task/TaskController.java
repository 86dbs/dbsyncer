/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.task;

import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.sdk.spi.TaskService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Map;

/**
 * @Author wuji
 * @Version 1.0.0
 * @Date 2025-10-18 19:52
 */

@RestController
@RequestMapping("/task")
public class TaskController {


    @Resource
    TaskService taskService;

    @PostMapping("/add")
    public RestResult add(@RequestBody Map<String, String> params) {

        taskService.add(params);
        return RestResult.restSuccess("新增成功");
    }
}
