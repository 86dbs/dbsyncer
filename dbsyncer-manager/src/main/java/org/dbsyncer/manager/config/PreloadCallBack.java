package org.dbsyncer.manager.config;

import org.dbsyncer.manager.template.Callback;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.*;

public class PreloadCallBack implements Callback {

    private Parser parser;

    private String json;

    public PreloadCallBack(Parser parser, String json) {
        this.parser = parser;
        this.json = json;
    }

    public Object parseConnector() {
        return parser.parseConnector(json);
    }

    public Object parseMapping() {
        return parser.parseObject(json, Mapping.class);
    }

    public Object parseTableGroup() {
        return parser.parseObject(json, TableGroup.class);
    }

    public Object parseMeta() {
        return parser.parseObject(json, Meta.class);
    }

    public Object parseConfig() {
        return parser.parseObject(json, Config.class);
    }

    public Object parseProjectGroup() {
        return parser.parseObject(json, ProjectGroup.class);
    }
}
