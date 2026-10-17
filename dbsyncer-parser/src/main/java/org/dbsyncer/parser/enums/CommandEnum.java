/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

/**
 * 枚举命令模式
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/24 14:19
 */
public enum CommandEnum {

    /**
     * 添加
     */
    OPR_ADD,

    /**
     * 修改
     */
    OPR_EDIT;

    /**
     * 判断是否是编辑操作
     */
    public boolean isEdit() {
        return this == OPR_EDIT;
    }
}
