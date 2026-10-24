/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 角色。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
public class Role extends ConfigModel {

    public Role() {
        super.setType(ConfigConstant.ROLE);
    }
}
