package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.model.Field;

/**
 * 字段映射关系
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/16 15:20
 */
public class FieldMapping {

    private Field source;

    private Field target;

    public FieldMapping() {
    }

    public FieldMapping(Field source, Field target) {
        this.source = source;
        this.target = target;
    }

    public Field getSource() {
        return source;
    }

    public void setSource(Field source) {
        this.source = source;
    }

    public Field getTarget() {
        return target;
    }

    public void setTarget(Field target) {
        this.target = target;
    }
}
