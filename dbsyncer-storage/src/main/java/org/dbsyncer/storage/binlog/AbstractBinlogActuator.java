package org.dbsyncer.storage.binlog;

import org.dbsyncer.storage.enums.BinlogStatusEnum;
import org.dbsyncer.storage.model.BinlogIndex;

import java.time.LocalDateTime;

public abstract class AbstractBinlogActuator implements BinlogActuator {

    private BinlogIndex binlogIndex;

    private BinlogStatusEnum status = BinlogStatusEnum.RUNNING;

    protected void initBinlogIndex(BinlogIndex binlogIndex) {
        binlogIndex.addLock(this);
        this.binlogIndex = binlogIndex;
    }

    protected void refreshBinlogIndexUpdateTime() {
        binlogIndex.setUpdateTime(LocalDateTime.now());
    }

    @Override
    public String getFileName() {
        return binlogIndex.getFileName();
    }

    @Override
    public boolean isRunning() {
        return status == BinlogStatusEnum.RUNNING;
    }

    @Override
    public void stop() {
        this.status = BinlogStatusEnum.STOP;
    }

}