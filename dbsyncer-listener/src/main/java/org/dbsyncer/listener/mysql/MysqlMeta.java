package org.dbsyncer.listener.mysql;

import org.dbsyncer.listener.Meta;

/**
 * binlog 读取增量数据元信息配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2018年8月2日 上午11:20:06
 */
public class MysqlMeta implements Meta {

    private static final long serialVersionUID = 7255240901494804338L;

    /**
     * binlog 文件名称
     */
    private String binlogFileName;

    /**
     * binlog 最新位置
     */
    private Long binlogPosition;

    /**
     * 监听的机器
     */
    private int master;

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public MysqlMeta setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
        return this;
    }

    public Long getBinlogPosition() {
        return binlogPosition;
    }

    public MysqlMeta setBinlogPosition(Long binlogPosition) {
        this.binlogPosition = binlogPosition;
        return this;
    }

    public int getMaster() {
        return master;
    }

    public MysqlMeta setMaster(int master) {
        this.master = master;
        return this;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Meta [binlogFileName=").append(binlogFileName).append(", binlogPosition=").append(binlogPosition).append(", master=").append(master).append("]").toString();
    }

}
