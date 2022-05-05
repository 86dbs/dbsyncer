package org.dbsyncer.connector;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.constant.ConnectorConstant;

import java.util.List;

public abstract class AbstractConnector {

    protected Field getPrimaryKeyField(List<Field> fields) {
        for (Field f : fields) {
            if (f.isPk()) {
                return f;
            }
        }
        throw new ConnectorException("主键为空");
    }

    protected boolean isUpdate(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_UPDATE, event);
    }

    protected boolean isInsert(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_INSERT, event);
    }

    protected boolean isDelete(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_DELETE, event);
    }
}