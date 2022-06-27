package org.dbsyncer.storage.binlog;

import org.dbsyncer.storage.enums.BinlogStatusEnum;
import org.dbsyncer.storage.model.BinlogIndex;

import java.time.LocalDateTime;

public abstract class AbstractBinlogActuator implements BinlogActuator {

    protected BinlogIndex binlogIndex;
    
    @Override
    public void initBinlogIndex(BinlogIndex binlogIndex) {
        binlogIndex.setStatus(BinlogStatusEnum.RUNNING);
        binlogIndex.addLock(this);
        this.binlogIndex = binlogIndex;
    }

    @Override
    public void refreshBinlogIndexUpdateTime() {
        binlogIndex.setUpdateTime(LocalDateTime.now());
    }

    @Override
    public BinlogIndex getBinlogIndex() {
        return binlogIndex;
    }

}