package org.dbsyncer.common.model;

import org.dbsyncer.common.spi.ConnectorMapper;

public class DDLConvertContext extends AbstractConvertContext {

    String sourceSql;

    String targetSql;

    String tableName;

    String originFiledName;

    String targetFileName;

    String originType;

    String targetType;

    public DDLConvertContext(ConnectorMapper sourceConnectorMapper, ConnectorMapper targetConnectorMapper, String sourceTableName, String targetTableName, String event,String sourceSql) {
        super.init(sourceConnectorMapper, targetConnectorMapper, sourceTableName, targetTableName, event, null, null);
        this.sourceSql = sourceSql;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public String getTargetSql() {
        return targetSql;
    }

    public void setTargetSql(String targetSql) {
        this.targetSql = targetSql;
    }

    /**
     * 从源sql转化为目标源sql
     */
    public void convertSql(){
        //TODO 从源sql转化为目标源sql
        //获取目标源数据库类型

        setTargetSql(sourceSql);
    }
}
