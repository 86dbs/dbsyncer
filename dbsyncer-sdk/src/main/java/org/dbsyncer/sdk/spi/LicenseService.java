/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.ProductInfo;

import java.io.File;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-13 01:17
 */
public interface LicenseService {

    /**
     * 获取License上传路径 dbsyncer/conf/
     */
    default String getLicensePath() {
        return new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("conf")
                .append(File.separatorChar).toString();
    }

    /**
     * 获取授权KEY
     *
     * @return
     */
    default String getKey() {
        return StringUtil.EMPTY;
    }

    /**
     * 获取授权信息
     *
     * @return
     */
    default ProductInfo getProductInfo() {
        return null;
    }
}