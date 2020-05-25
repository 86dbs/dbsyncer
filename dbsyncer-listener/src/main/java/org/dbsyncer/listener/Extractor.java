package org.dbsyncer.listener;

public interface Extractor {

    /**
     * 启动定时/日志等方式抽取增量数据
     */
    void start();

    /**
     * 关闭任务
     */
    void close();

}