/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.plugin;

import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.rsa.RsaManager;

import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-12-05 00:28
 */
public interface ReaderContext extends BaseContext {

    boolean isSupportedCursor();

    List<Object> getArgs();

    Object[] getCursors();

    int getPageIndex();

    int getPageSize();

    /**
     * 获取RSA加密类
     */
    RsaManager getRsaManager();

    /**
     * 获取RSA配置
     */
    RsaConfig getRsaConfig();
}
