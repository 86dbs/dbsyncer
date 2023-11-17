package org.dbsyncer.storage.enums;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 20:31
 */
public enum StorageEnum {

    /**
     * 配置：连接器、驱动、映射关系、同步信息、分組配置、系统配置、用戶配置
     */
    CONFIG("config"),
    /**
     * 日志：连接器、驱动、映射关系、同步信息、系统日志
     */
    LOG("log"),
    /**
     * 数据：全量或增量数据
     */
    DATA("data");

    private String type;

    StorageEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}