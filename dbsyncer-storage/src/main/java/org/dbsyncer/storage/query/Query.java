package org.dbsyncer.storage.query;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/16 22:56
 */
public class Query {

    private List<Param> params;

    public Query() {
        params = new ArrayList<>();
    }

    public List<Param> getParams() {
        return params;
    }

    public void setParams(List<Param> params) {
        this.params = params;
    }

    public void put(String key, String value) {
        params.add(new Param(key, value));
    }
}


