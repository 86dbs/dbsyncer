/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 业务分组（{@code dbsyncer_group}）。
 * <p>勿与表映射 {@link TableGroup} 混淆。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-14 18:00
 */
public class Group extends ConfigModel {

    public Group() {
        super.setType(ConfigConstant.GROUP);
    }
}
