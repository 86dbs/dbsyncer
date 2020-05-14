package org.dbsyncer.listener;

public interface Extractor {

    /**
     * 根据日志/事件等抽取
     */
    void extract();

    /**
     * 定时抽取
     */
    void extractTiming();

    /**
     * 关闭任务
     */
    void close();

}
