package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConditionService;
import org.dbsyncer.biz.vo.ConditionVo;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.dbsyncer.parser.ProfileComponent;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/19 16:02
 */
@Component
public class ConditionServiceImpl implements ConditionService {

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public ConditionVo getCondition() {
        List<OperationEnum> operationEnumAll = profileComponent.getOperationEnumAll();
        List<QuartzFilterEnum> quartzFilterEnumAll = profileComponent.getQuartzFilterEnumAll();
        List<FilterEnum> filterEnumAll = profileComponent.getFilterEnumAll();
        return new ConditionVo(operationEnumAll, quartzFilterEnumAll, filterEnumAll);
    }
}