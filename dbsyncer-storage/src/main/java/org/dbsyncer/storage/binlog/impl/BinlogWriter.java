package org.dbsyncer.storage.binlog.impl;

import com.google.protobuf.CodedOutputStream;
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
            // 选择固定长度int32作为tag标志位，4bytes, 最多可容纳2^31-1字节（2047MB左右, 建议上限64~128M内最佳）,
            final int serialized = message.getSerializedSize();
            final int bufferSize = CodedOutputStream.computeFixed32SizeNoTag(serialized) + serialized;
            final CodedOutputStream codedOutput = CodedOutputStream.newInstance(out, bufferSize);
            codedOutput.writeFixed32NoTag(serialized);
            message.writeTo(codedOutput);
            codedOutput.flush();
            refreshBinlogIndexUpdateTime();
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(out);
    }
}