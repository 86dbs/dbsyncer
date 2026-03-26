package org.dbsyncer.sdk.model;

/**
 * 增量偏移量
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-23 20:00
 */
public final class ChangedOffset {

    /**
     * 驱动ID
     */
    private String metaId;

    /**
     * 增量文件名称
     */
    private String nextFileName;

    /**
     * 增量偏移量
     */
    private Object position;

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public String getNextFileName() {
        return nextFileName;
    }

    public void setNextFileName(String nextFileName) {
        this.nextFileName = nextFileName;
    }

    public Object getPosition() {
        return position;
    }

    public void setPosition(Object position) {
        this.position = position;
    }
}
