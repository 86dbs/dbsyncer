package org.dbsyncer.manager.config;

import org.dbsyncer.manager.template.Callback;
import org.dbsyncer.storage.StorageService;

import java.util.Map;

public class OperationCallBack implements Callback {

    private StorageService storageService;

    private String type;

    private Map params;

    public OperationCallBack(StorageService storageService, String type, Map params) {
        this.storageService = storageService;
        this.type = type;
        this.params = params;
    }

    public void add() {
        storageService.add(type, params);
    }

    public void edit() {
        storageService.edit(type, params);
    }

}