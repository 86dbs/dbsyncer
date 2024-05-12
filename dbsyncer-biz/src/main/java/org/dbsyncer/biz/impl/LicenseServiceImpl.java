/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.LicenseService;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-13 01:19
 */
@Component
public class LicenseServiceImpl implements LicenseService {

    /**
     * License路径dbsyncer/conf/
     */
    private final String LICENSE_PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("conf")
            .append(File.separatorChar).toString();

    @Override
    public String getLicensePath() {
        return LICENSE_PATH;
    }
}