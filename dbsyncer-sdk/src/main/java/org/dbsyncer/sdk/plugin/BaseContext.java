/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 00:59
 */
public interface BaseContext {

    Map<String, String> getCommand();
}