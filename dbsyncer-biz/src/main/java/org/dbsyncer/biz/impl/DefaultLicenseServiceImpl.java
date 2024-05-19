/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.ProductInfo;
import org.dbsyncer.sdk.spi.LicenseService;

import java.io.File;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-13 01:19
 */
public final class DefaultLicenseServiceImpl implements LicenseService {

    @Override
    public String getLicensePath() {
        return new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("conf")
                .append(File.separatorChar).toString();
    }

    @Override
    public String getKey() {
        return StringUtil.EMPTY;
    }

    @Override
    public ProductInfo getProductInfo() {
        return null;
    }

    @Override
    public void updateLicense() {

    }
}