/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.command;

import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;

/**
 * 预加载接口
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public interface Preload {

    default SystemConfig parseSystemConfig(){
        throw new ParserException("Unsupported method parseSystemConfig");
    }

    default UserConfig parseUserConfig(){
        throw new ParserException("Unsupported method parseUserConfig");
    }

    default Connector parseConnector(){
        throw new ParserException("Unsupported method parseConnector");
    }

    default Mapping parseMapping(){
        throw new ParserException("Unsupported method parseMapping");
    }

    default TableGroup parseTableGroup(){
        throw new ParserException("Unsupported method parseTableGroup");
    }

    default Meta parseMeta(){
        throw new ParserException("Unsupported method parseMeta");
    }

    default ProjectGroup parseProjectGroup(){
        throw new ParserException("Unsupported method parseProjectGroup");
    }
}