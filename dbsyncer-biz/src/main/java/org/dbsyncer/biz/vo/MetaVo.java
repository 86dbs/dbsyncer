package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Meta;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/03 17:20
 */
public class MetaVo extends Meta {

    // 同步方式
    private String model;
    // 驱动名称
    private String mappingName;
    // 是否统计总数中
    private boolean counting;

    public MetaVo(String model, String mappingName) {
        this.model = model;
        this.mappingName = mappingName;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }

    public boolean isCounting() {
        return counting;
    }

    public void setCounting(boolean counting) {
        this.counting = counting;
    }
}