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
    private String mappingState;

    public MetaVo(String mappingName, String model, String mappingState) {
        this.mappingName = mappingName;
        this.model = model;
        this.mappingState = mappingState;
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

    public String getMappingState() {
        return mappingState;
    }

    public void setMappingState(String mappingState) {
        this.mappingState = mappingState;
    }
}