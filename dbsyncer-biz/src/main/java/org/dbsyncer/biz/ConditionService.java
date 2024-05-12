/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ConditionVo;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/19 16:02
 */
public interface ConditionService {

    /**
     * 获取过滤条件
     *
     * @return
     */
    ConditionVo getCondition();

}