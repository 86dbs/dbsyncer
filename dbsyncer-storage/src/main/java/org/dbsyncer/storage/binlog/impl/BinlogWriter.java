package org.dbsyncer.storage.binlog.impl;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.storage.binlog.AbstractBinlogActuator;
import org.dbsyncer.storage.binlog.proto.BinlogMessage;
import org.dbsyncer.storage.model.BinlogIndex;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/26 23:28
 */
public class BinlogWriter extends AbstractBinlogActuator {

    private final OutputStream out;

    public BinlogWriter(String path, BinlogIndex binlogIndex) throws FileNotFoundException {
        initBinlogIndex(binlogIndex);
        this.out = new FileOutputStream(path + binlogIndex.getFileName(), true);
    }

    public void write(BinlogMessage message) throws IOException {
        if(null != message){
            message.writeDelimitedTo(out);
            refreshBinlogIndexUpdateTime();
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(out);
    }
}