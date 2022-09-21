package org.dbsyncer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/9/21 22:20
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.flush.increment.data")
public class IncrementDataConfig {

    /**
     * 是否记录同步成功数据
     */
    private boolean writeSuccess;

    /**
     * 是否记录同步失败数据
     */
    private boolean writeFail;

    /**
     * 最大记录异常信息长度
     */
    private int maxErrorLength;

    public boolean isWriteSuccess() {
        return writeSuccess;
    }

    public void setWriteSuccess(boolean writeSuccess) {
        this.writeSuccess = writeSuccess;
    }

    public boolean isWriteFail() {
        return writeFail;
    }

    public void setWriteFail(boolean writeFail) {
        this.writeFail = writeFail;
    }

    public int getMaxErrorLength() {
        return maxErrorLength;
    }

    public void setMaxErrorLength(int maxErrorLength) {
        this.maxErrorLength = maxErrorLength;
    }
}
