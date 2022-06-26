package org.dbsyncer.storage.binlog;

import org.dbsyncer.storage.model.BinlogIndex;

import java.io.Closeable;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/26 23:23
 */
public interface BinlogActuator extends Closeable {

    void initBinlogIndex(BinlogIndex binlogIndex);

    void refreshBinlogIndexUpdateTime();

    BinlogIndex getBinlogIndex();
}