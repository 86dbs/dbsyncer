package org.dbsyncer.manager.command;

import org.dbsyncer.manager.Command;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.enums.StorageEnum;

import java.util.Map;

public class PersistenceCommand implements Command {

    private StorageService storageService;

    private StorageEnum type;

    private Map params;

    public PersistenceCommand(StorageService storageService, StorageEnum type, Map params) {
        this.storageService = storageService;
        this.type = type;
        this.params = params;
    }

    public boolean add() {
        storageService.add(type, params);
        return true;
    }

    public boolean edit() {
        storageService.edit(type, params);
        return true;
    }

}