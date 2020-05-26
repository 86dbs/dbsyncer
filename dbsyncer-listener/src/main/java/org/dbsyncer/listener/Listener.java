package org.dbsyncer.listener;

import org.dbsyncer.listener.config.ExtractorConfig;

public interface Listener {

    /**
     * 创建抽取器
     *
     * @param config 抽取器配置
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    AbstractExtractor getExtractor(ExtractorConfig config) throws IllegalAccessException, InstantiationException;
}