/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

/**
 * 同步任务列表：连接器类型（保持前端 {@code config.connectorType} 路径）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-29
 */
public class MappingListConnectorConfigVO {

    private String connectorType;

    public MappingListConnectorConfigVO(String connectorType) {
        this.connectorType = connectorType;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(String connectorType) {
        this.connectorType = connectorType;
    }
}
