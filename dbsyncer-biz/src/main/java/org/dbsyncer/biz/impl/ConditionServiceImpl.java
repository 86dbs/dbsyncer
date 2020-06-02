package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConditionService;
import org.dbsyncer.biz.vo.ConditionVo;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.manager.Manager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/19 16:02
 */
@Component
public class ConditionServiceImpl implements ConditionService {

    @Autowired
    private Manager manager;

    @Override
    public ConditionVo getCondition() {
        List<OperationEnum> operationEnumAll = manager.getOperationEnumAll();
        List<QuartzFilterEnum> quartzFilterEnumAll = manager.getQuartzFilterEnumAll();
        List<FilterEnum> filterEnumAll = manager.getFilterEnumAll();
        return new ConditionVo(operationEnumAll, quartzFilterEnumAll, filterEnumAll);
    }
}