package org.dbsyncer.biz;

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
     *
     * @param params
     */
    String add(Map<String, String> params);

    /**
     * 复制连接器
     *
     * @param id
     * @return
     */
    String copy(String id);

    /**
     * 修改连接器
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 删除连接器
     *
     * @param id
     */
    String remove(String id);

    /**
     * 获取连接器
     *
     * @param id
     * @return
     */
    Connector getConnector(String id);

    /**
     * 获取所有连接器
     *
     * @return
     */
    List<Connector> getConnectorAll();

    /**
     * 获取所有支持的连接器类型
     *
     * @return
     */
    List<String> getConnectorTypeAll();

    /**
     * 检查连接器状态
     */
    void refreshHealth();

    /**
     * 连接器是否可用
     *
     * @param id
     * @return
     */
    boolean isAlive(String id);
}