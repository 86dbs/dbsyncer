package org.dbsyncer.manager.command;

import org.dbsyncer.manager.Command;
import org.dbsyncer.storage.StorageService;

import java.util.Map;

public class PersistenceCommand implements Command {

    private StorageService storageService;

    private Map params;

    public PersistenceCommand(StorageService storageService, Map params) {
        this.storageService = storageService;
        this.params = params;
    }

    public boolean addConfig() {
        storageService.addConfig(params);
        return true;
    }

    public boolean editConfig() {
        storageService.editConfig(params);
        return true;
    }

}