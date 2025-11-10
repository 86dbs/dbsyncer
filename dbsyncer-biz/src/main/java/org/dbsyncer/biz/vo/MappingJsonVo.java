package org.dbsyncer.biz.vo;

import org.dbsyncer.sdk.enums.ModelEnum;

/**
 * @author cdeluser
 */
public class MappingJsonVo {

    /**
     * 驱动ID
     */
    private String id;
    /**
     * 同步方式
     *
     * @see ModelEnum
     */
    private String model;

    /**
     *  元信息
     */
    private MetaVo meta;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public MetaVo getMeta() {
        return meta;
    }

    public void setMeta(MetaVo meta) {
        this.meta = meta;
    }
}
