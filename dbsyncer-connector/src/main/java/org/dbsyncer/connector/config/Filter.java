package org.dbsyncer.connector.config;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 15:10
 */
public class Filter {

    /**
     * and 过滤
     */
    private List<FieldFilter> and;

    /**
     * or 过滤
     */
    private List<FieldFilter> or;

    public Filter() {
    }

    public Filter(List<FieldFilter> and, List<FieldFilter> or) {
        this.and = and;
        this.or = or;
    }

    public List<FieldFilter> getAnd() {
        return and;
    }

    public List<FieldFilter> getOr() {
        return or;
    }
}