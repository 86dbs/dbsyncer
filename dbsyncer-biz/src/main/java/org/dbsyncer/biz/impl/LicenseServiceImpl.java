/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.LicenseService;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-13 01:19
 */
public class LicenseServiceImpl implements LicenseService {

    @Override
    public String getKey() {
        return StringUtil.EMPTY;
    }

    @Override
    public int getStatus() {
        // 未授权
        return 0;
    }
}