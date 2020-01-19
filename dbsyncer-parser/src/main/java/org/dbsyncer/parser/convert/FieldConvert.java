package org.dbsyncer.parser.convert;

import java.util.List;

/**
 * 字段转换
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/7 14:20
 */
public class FieldConvert {

    // 字段名称
    private String name;

    // 转换方式
    private List<Convert> convert;

    public String getName() {
        return name;
    }

    public FieldConvert setName(String name) {
        this.name = name;
        return this;
    }

    public List<Convert> getConvert() {
        return convert;
    }

    public FieldConvert setConvert(List<Convert> convert) {
        this.convert = convert;
        return this;
    }
}
