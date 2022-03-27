package org.dbsyncer.parser.flush.model;

import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public class FlushRequest extends AbstractRequest {

    private String metaId;

    private List<Map> list;

    public FlushRequest(String metaId, List<Map> list) {
        this.metaId = metaId;
        this.list = list;
    }

    public String getMetaId() {
        return metaId;
    }

    public List<Map> getList() {
        return list;
    }
}