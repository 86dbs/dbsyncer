package org.dbsyncer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/9/21 22:20
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.parser.flush.data.increment")
public class IncrementDataConfig {

    /**
     * 是否记录同步成功数据
     */
    private boolean writerSuccess;

    /**
     * 是否记录同步失败数据
     */
    private boolean writerFail;

    /**
     * 最大记录异常信息长度
     */
    private int maxErrorLength;

    public boolean isWriterSuccess() {
        return writerSuccess;
    }

    public void setWriterSuccess(boolean writerSuccess) {
        this.writerSuccess = writerSuccess;
    }

    public boolean isWriterFail() {
        return writerFail;
    }

    public void setWriterFail(boolean writerFail) {
        this.writerFail = writerFail;
    }

    public int getMaxErrorLength() {
        return maxErrorLength;
    }

    public void setMaxErrorLength(int maxErrorLength) {
        this.maxErrorLength = maxErrorLength;
    }
}