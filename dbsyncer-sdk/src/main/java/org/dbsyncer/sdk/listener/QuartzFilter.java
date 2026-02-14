package org.dbsyncer.sdk.listener;

public interface QuartzFilter<T> {

    /**
     * 获取默认参数
     */
    T getObject();

    /**
     * 反解参数
     */
    T getObject(String s);

    /**
     * 转String类型
     */
    String toString(T value);

    /**
     * 是否开始字段
     */
    boolean begin();
}
