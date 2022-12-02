package org.dbsyncer.manager.command;

import org.dbsyncer.manager.Command;
import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;

public class PreloadCommand implements Command {

    private Parser parser;

    private String json;

    public PreloadCommand(Parser parser, String json) {
        this.parser = parser;
        this.json = json;
    }

    @Override
    public SystemConfig parseSystemConfig() {
        return parser.parseObject(json, SystemConfig.class);
    }

    @Override
    public UserConfig parseUserConfig() {
        return parser.parseObject(json, UserConfig.class);
    }

    @Override
    public Connector parseConnector() {
        return parser.parseConnector(json);
    }

    @Override
    public Mapping parseMapping() {
        return parser.parseObject(json, Mapping.class);
    }

    @Override
    public TableGroup parseTableGroup() {
        return parser.parseObject(json, TableGroup.class);
    }

    @Override
    public Meta parseMeta() {
        return parser.parseObject(json, Meta.class);
    }

    @Override
    public ProjectGroup parseProjectGroup() {
        return parser.parseObject(json, ProjectGroup.class);
    }

}