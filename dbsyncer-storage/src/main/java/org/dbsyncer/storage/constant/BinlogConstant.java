package org.dbsyncer.storage.constant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/13 22:14
 */
public class BinlogConstant {

    /**
     * 属性
     */
    public static final String BINLOG_ID = "id";
    public static final String BINLOG_STATUS = "s";
    public static final String BINLOG_CONTENT = "c";
    public static final String BINLOG_TIME = "t";

    /**
     * 状态类型
     */
    public static final int READY = 0;
    public static final int PROCESSING = 1;

}