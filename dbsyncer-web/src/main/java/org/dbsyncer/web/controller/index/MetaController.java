package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/meta")
public class MetaController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MappingService mappingService;

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll() {
        try {
            return RestResult.restSuccess(mappingService.getMetaAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}