package org.dbsyncer.listener;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.connector.database.DatabaseConnectorMapper;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.MetaInfo;
import org.dbsyncer.connector.model.Table;
import org.dbsyncer.connector.util.PrimaryKeyUtil;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/29 21:46
 */
public abstract class AbstractDatabaseExtractor extends AbstractExtractor {

    private Map<String, DqlMapper> dqlMap = new ConcurrentHashMap<>();

    /**
     * 发送增量事件
     *
     * @param event
     */
    protected void sendChangedEvent(RowChangedEvent event) {
        changedEvent(event);
    }

    /**
     * 发送DQL增量事件
     *
     * @param event
     */
    protected void sendDqlChangedEvent(RowChangedEvent event) {
        if (null != event) {
            DqlMapper dqlMapper = dqlMap.get(event.getSourceTableName());
            if (null != dqlMapper) {
                switch (event.getEvent()) {
                    case ConnectorConstant.OPERTION_UPDATE:
                    case ConnectorConstant.OPERTION_INSERT:
                        event.setDataList(queryDqlData(dqlMapper, event.getDataList()));
                        break;
                    default:
                        break;
                }
                changedEvent(event);
            }
        }
    }

    /**
     * 初始化Dql连接配置
     */
    protected void postProcessDqlBeforeInitialization() {
        DatabaseConnectorMapper mapper = (DatabaseConnectorMapper) connectorFactory.connect(connectorConfig);
        AbstractDatabaseConnector connector = (AbstractDatabaseConnector) connectorFactory.getConnector(mapper);
        String quotation = connector.buildSqlWithQuotation();

        Map<String, String> tableMap = new HashMap<>();
        mapper.getConfig().getSqlTables().forEach(s -> tableMap.put(s.getSqlName(), s.getTable()));

        for (Table t : sourceTable) {
            String sql = t.getSql();
            String sqlName = t.getName();
            List<String> primaryKeys = PrimaryKeyUtil.findTablePrimaryKeys(t);
            String tableName = tableMap.get(sqlName);
            Assert.hasText(sql, "The sql is null.");
            Assert.hasText(tableName, "The tableName is null.");

            MetaInfo metaInfo = connectorFactory.getMetaInfo(mapper, sqlName);
            final List<Field> column = metaInfo.getColumn();
            Assert.notEmpty(column, String.format("The column of table name '%s' is empty.", sqlName));

            sql = sql.toUpperCase().replace("\t", " ");
            sql = sql.replace("\r", " ");
            sql = sql.replace("\n", " ");

            StringBuilder querySql = new StringBuilder(sql);
            boolean notContainsWhere = !StringUtil.contains(sql, " WHERE ");
            querySql.append(notContainsWhere ? " WHERE " : " AND ");
            PrimaryKeyUtil.buildSql(querySql, primaryKeys, quotation, " AND ", " = ? ", notContainsWhere);
            DqlMapper dqlMapper = new DqlMapper(mapper, querySql.toString(), column, getPrimaryKeyIndexArray(column, primaryKeys));
            dqlMap.putIfAbsent(tableName, dqlMapper);
        }
    }

    /**
     * 获取主表主键索引
     *
     * @param column
     * @param primaryKeys
     * @return
     */
    protected Integer[] getPrimaryKeyIndexArray(List<Field> column, List<String> primaryKeys) {
        List<Integer> indexList = new ArrayList<>();
        for (Field f : column) {
            if (primaryKeys.contains(f.getName())) {
                indexList.add(column.indexOf(f));
            }
        }
        Assert.isTrue(!CollectionUtils.isEmpty(indexList), "The primaryKeys is invalid.");
        Integer[] indexArray = (Integer[]) indexList.toArray();
        return indexArray;
    }

    private List<Object> queryDqlData(DqlMapper dqlMapper, List<Object> data) {
        if (!CollectionUtils.isEmpty(data)) {
            Map<String, Object> row = dqlMapper.mapper.execute(databaseTemplate -> {
                int size = dqlMapper.primaryKeyIndexArray.length;
                Object[] args = new Object[size];
                for (int i = 0; i < size; i++) {
                    args[i] = data.get(dqlMapper.primaryKeyIndexArray[i]);
                }
                return databaseTemplate.queryForMap(dqlMapper.sql, args);
            });
            if (!CollectionUtils.isEmpty(row)) {
                data.clear();
                dqlMapper.column.forEach(field -> data.add(row.get(field.getName())));
            }
        }
        return data;
    }

    final class DqlMapper {
        DatabaseConnectorMapper mapper;
        String sql;
        List<Field> column;
        Integer[] primaryKeyIndexArray;

        public DqlMapper(DatabaseConnectorMapper mapper, String sql, List<Field> column, Integer[] primaryKeyIndexArray) {
            this.mapper = mapper;
            this.column = column;
            this.primaryKeyIndexArray = primaryKeyIndexArray;
            this.sql = sql;
        }
    }

}