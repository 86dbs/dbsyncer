package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.UserVariable;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

/**
 * Written every time a statement uses a user variable; precedes other events for the statement.
 * Indicates the value to use for the user variable in the next statement.
 * This is written only before a QUERY_EVENT and is not used with row-based logging.
 *
 * @ClassName: UserVarEvent
 * @author: AE86
 * @date: 2018年10月17日 下午2:30:48
 */
public final class UserVarEvent extends AbstractBinlogEventV4 {
    public static final int EVENT_TYPE = MySQLConstants.USER_VAR_EVENT;

    private int varNameLength;
    private StringColumn varName;
    private int isNull;
    private int varType;
    private int varCollation;
    private int varValueLength;
    private UserVariable varValue;

    public UserVarEvent() {
    }

    public UserVarEvent(BinlogEventV4Header header) {
        this.header = header;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header)
                .append("varNameLength", varNameLength)
                .append("varName", varName)
                .append("isNull", isNull)
                .append("varType", varType)
                .append("varCollation", varCollation)
                .append("varValueLength", varValueLength)
                .append("varValue", varValue).toString();
    }

    public int getVarNameLength() {
        return varNameLength;
    }

    public void setVarNameLength(int varNameLength) {
        this.varNameLength = varNameLength;
    }

    public StringColumn getVarName() {
        return varName;
    }

    public void setVarName(StringColumn varName) {
        this.varName = varName;
    }

    public int getIsNull() {
        return isNull;
    }

    public void setIsNull(int isNull) {
        this.isNull = isNull;
    }

    public int getVarType() {
        return varType;
    }

    public void setVarType(int variableType) {
        this.varType = variableType;
    }

    public int getVarCollation() {
        return varCollation;
    }

    public void setVarCollation(int varCollation) {
        this.varCollation = varCollation;
    }

    public int getVarValueLength() {
        return varValueLength;
    }

    public void setVarValueLength(int varValueLength) {
        this.varValueLength = varValueLength;
    }

    public UserVariable getVarValue() {
        return varValue;
    }

    public void setVarValue(UserVariable varValue) {
        this.varValue = varValue;
    }
}
