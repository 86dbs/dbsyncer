package org.dbsyncer.manager.command;

import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.parser.model.*;

public interface Preload {

    default SystemConfig parseSystemConfig(){
        throw new ManagerException("Unsupported method parseSystemConfig");
    }

    default UserConfig parseUserConfig(){
        throw new ManagerException("Unsupported method parseUserConfig");
    }

    default Connector parseConnector(){
        throw new ManagerException("Unsupported method parseConnector");
    }

    default Mapping parseMapping(){
        throw new ManagerException("Unsupported method parseMapping");
    }

    default TableGroup parseTableGroup(){
        throw new ManagerException("Unsupported method parseTableGroup");
    }

    default Meta parseMeta(){
        throw new ManagerException("Unsupported method parseMeta");
    }

    default ProjectGroup parseProjectGroup(){
        throw new ManagerException("Unsupported method parseProjectGroup");
    }
}