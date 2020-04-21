package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Meta;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/03 17:20
 */
public class MetaVo extends Meta {

    // 驱动名称
    private String mappingName;
    // 同步方式
    private String model;
    // 状态
    private String status;

    public MetaVo(String mappingName, String model, String status) {
        this.mappingName = mappingName;
        this.model = model;
        this.status = status;
    }

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}