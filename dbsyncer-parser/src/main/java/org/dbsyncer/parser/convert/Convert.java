package org.dbsyncer.parser.convert;

import org.dbsyncer.parser.enums.ConvertEnum;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 14:04
 */
public class Convert {

    /**
     * 方式
     * @see ConvertEnum
     */
    private String name;

    // 参数
    private List<String> args;

    public String getName() {
        return name;
    }

    public Convert setName(String name) {
        this.name = name;
        return this;
    }

    public List<String> getArgs() {
        return args;
    }

    public Convert setArgs(List<String> args) {
        this.args = args;
        return this;
    }
}
