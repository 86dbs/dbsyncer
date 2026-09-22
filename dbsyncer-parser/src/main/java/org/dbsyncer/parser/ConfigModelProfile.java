/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.common.model.ConfigModel;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-22 21:02
 */
public interface ConfigModelProfile {

    ConfigModel getCache(String id);

    void removeCache(String id);

}