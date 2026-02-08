/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.parser.enums.ConvertEnum;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/14 0:02
 */
public interface ConvertService {

    /**
     * 获取所有转换类型
     *
     * @return
     */
    List<ConvertEnum> getConvertEnumAll();
}
