package org.dbsyncer.common.model;

import org.dbsyncer.common.spi.ProxyApplicationContext;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:04
 */
public class FullConvertContext extends ConvertContext {

    /**
     * 全量同步，数据源数据集合
     */
    private List<Map> sourceList;

    /**
     * 全量同步，目标源源数据集合
     */
    private List<Map> targetList;

    public FullConvertContext(ProxyApplicationContext context, List<Map> sourceList, List<Map> targetList) {
        this.context = context;
        this.sourceList = sourceList;
        this.targetList = targetList;
    }

    public List<Map> getSourceList() {
        return sourceList;
    }

    public List<Map> getTargetList() {
        return targetList;
    }
}