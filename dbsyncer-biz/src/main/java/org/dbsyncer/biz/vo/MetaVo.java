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

    public MetaVo(String mappingName, String model) {
        this.mappingName = mappingName;
        this.model = model;
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
}