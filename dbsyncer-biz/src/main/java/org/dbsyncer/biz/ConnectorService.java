/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.Connector;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:18
 */
public interface ConnectorService {

    /**
     * 新增连接器
     */
    String add(Map<String, String> params);

    /**
     * 复制连接器
     */
    String copy(String id);

    /**
     * 修改连接器
     */
    String edit(Map<String, String> params);

    /**
     * 删除连接器
     */
    String remove(String id);

    /**
     * 获取连接器
     */
    Connector getConnector(String id);

    /**
     * 获取数据库信息
     */
    List<String> getDatabase(String id);

    /**
     * 获取Schema信息
     */
    List<String> getSchema(String id, String catalog);

    /**
     * 获取所有连接器
     */
    List<Connector> getConnectorAll();

    /**
     * 搜索连接器
     */
    Paging<Connector> search(Map<String, String> params);

    /**
     * 获取所有支持的连接器类型
     */
    List<String> getConnectorTypeAll();

    /**
     * 检查连接器状态
     */
    void refreshHealth();

    /**
     * 连接器是否可用
     */
    boolean isAlive(String id);

    /**
     * 获取位点信息
     */
    Object getPosition(String params);
}
