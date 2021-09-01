package org.dbsyncer.listener.quartz;

public interface QuartzFilter {

    /**
     * 获取默认参数
     *
     * @return
     */
    Object getObject();

    /**
     * 反解参数
     *
     * @param s
     * @return
     */
    Object getObject(String s);

    /**
     * 转String类型
     *
     * @param value
     * @return
     */
    String toString(Object value);

    /**
     * 是否开始字段
     *
     * @return
     */
    boolean begin();
}