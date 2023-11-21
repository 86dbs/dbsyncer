package org.dbsyncer.sdk.listener;

public interface QuartzFilter<T> {

    /**
     * 获取默认参数
     *
     * @return
     */
    T getObject();

    /**
     * 反解参数
     *
     * @param s
     * @return
     */
    T getObject(String s);

    /**
     * 转String类型
     *
     * @param value
     * @return
     */
    String toString(T value);

    /**
     * 是否开始字段
     *
     * @return
     */
    boolean begin();
}