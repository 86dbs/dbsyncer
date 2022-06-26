package org.dbsyncer.storage.binlog;

import org.dbsyncer.storage.enums.BinlogStatusEnum;
import org.dbsyncer.storage.model.BinlogIndex;

import java.time.Instant;
public abstract class AbstractBinlogActuator implements BinlogActuator {
    protected BinlogIndex binlogIndex;
    @Override
    public void initBinlogIndex(BinlogIndex binlogIndex) {
        binlogIndex.setStatus(BinlogStatusEnum.RUNNING);
        binlogIndex.add(this);
        this.binlogIndex = binlogIndex;
    }

    @Override
    public void refreshBinlogIndexUpdateTime(){
        binlogIndex.setUpdateTime(Instant.now().toEpochMilli());
    }

    @Override
    public BinlogIndex getBinlogIndex() {
        return binlogIndex;
    }

}