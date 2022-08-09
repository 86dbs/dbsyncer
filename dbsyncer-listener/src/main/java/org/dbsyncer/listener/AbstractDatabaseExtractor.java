package org.dbsyncer.listener;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 21:46
 */
public abstract class AbstractDatabaseExtractor extends AbstractExtractor {

    private DqlMapper dqlMapper;

    /**
     * 发送增量事件
     *
     * @param event
     */
    protected abstract void sendChangedEvent(RowChangedEvent event);

    /**
     * 发送DQL增量事件
     *
     * @param event
     */
    protected void sendDqlChangedEvent(RowChangedEvent event) {
        if (null != event && event.getSourceTableName().equals(dqlMapper.tableName)) {
            switch (event.getEvent()){
                case ConnectorConstant.OPERTION_UPDATE:
                case ConnectorConstant.OPERTION_INSERT:
                    event.setDataList(queryData(event.getDataList()));
                    break;
                default:
                    break;
            }
            changedEvent(event);
        }
    }

    /**
     * 初始化Dql连接配置
     */
    protected void postProcessDqlBeforeInitialization() {
        DatabaseConnectorMapper mapper = (DatabaseConnectorMapper) connectorFactory.connect(connectorConfig);
        DatabaseConfig cfg = mapper.getConfig();
        final String tableName = cfg.getTable();
        final String primaryKey = cfg.getPrimaryKey();
        Assert.hasText(tableName, String.format("The table name '%s' is null.", tableName));
        MetaInfo metaInfo = connectorFactory.getMetaInfo(mapper, tableName);
        final List<Field> column = metaInfo.getColumn();
        Assert.notEmpty(column, String.format("The column of table name '%s' is empty.", tableName));

        int pkIndex = 0;
        boolean findPkIndex = false;
        for (Field f : column) {
            if (f.isPk() && f.getName().equals(primaryKey)) {
                pkIndex = column.indexOf(f);
                findPkIndex = true;
                break;
            }
        }
        Assert.isTrue(findPkIndex, "The primaryKey is invalid.");
        String sql = new StringBuilder(cfg.getSql()).append(" AND ").append(primaryKey).append("=?").toString();

        dqlMapper = new DqlMapper(mapper, tableName, column, pkIndex, sql);
    }

    private List<Object> queryData(List<Object> data) {
        if (data.size() >= dqlMapper.pkIndex) {
            Map<String, Object> row = dqlMapper.mapper.execute(databaseTemplate -> databaseTemplate.queryForMap(dqlMapper.sql, data.get(dqlMapper.pkIndex)));
            if (!CollectionUtils.isEmpty(row)) {
                data.clear();
                dqlMapper.column.forEach(field -> data.add(row.get(field.getName())));
            }
        }
        return data;
    }

    final class DqlMapper {
        DatabaseConnectorMapper mapper;
        String tableName;
        List<Field> column;
        int pkIndex;
        String sql;

        public DqlMapper(DatabaseConnectorMapper mapper, String tableName, List<Field> column, int pkIndex, String sql) {
            this.mapper = mapper;
            this.tableName = tableName;
            this.column = column;
            this.pkIndex = pkIndex;
            this.sql = sql;
        }
    }

}