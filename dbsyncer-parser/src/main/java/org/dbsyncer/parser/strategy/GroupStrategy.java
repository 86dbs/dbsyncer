/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy;

import org.dbsyncer.parser.model.ConfigModel;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:52
 */
public interface GroupStrategy<M extends ConfigModel> {

    String getGroupId(M model);

}