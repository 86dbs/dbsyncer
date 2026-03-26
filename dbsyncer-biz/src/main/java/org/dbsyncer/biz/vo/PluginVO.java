package org.dbsyncer.biz.vo;

import org.dbsyncer.sdk.model.Plugin;

public class PluginVO extends Plugin {

    /**
     * 正在使用插件的驱动名称，多个","拼接
     */
    private String mappingName;

    public String getMappingName() {
        return mappingName;
    }

    public void setMappingName(String mappingName) {
        this.mappingName = mappingName;
    }
}
