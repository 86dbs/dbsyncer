/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConditionService;
import org.dbsyncer.biz.vo.ConditionVO;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * 支持的条件和运算符类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020-01-19 16:02
 */
@Service
public class ConditionServiceImpl implements ConditionService {

    @Override
    public ConditionVO getCondition() {
        List<OperationEnum> operationEnumAll = Arrays.asList(OperationEnum.values());;
        List<QuartzFilterEnum> quartzFilterEnumAll = Arrays.asList(QuartzFilterEnum.values());
        List<FilterEnum> filterEnumAll = Arrays.asList(FilterEnum.values());
        return new ConditionVO(operationEnumAll, quartzFilterEnumAll, filterEnumAll);
    }
}