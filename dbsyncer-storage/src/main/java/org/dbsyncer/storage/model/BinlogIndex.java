package org.dbsyncer.storage.model;

import org.dbsyncer.storage.binlog.BinlogActuator;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/26 22:48
 */
public class BinlogIndex {
    private String fileName;
    private Set<BinlogActuator> lock;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;

    public BinlogIndex(String fileName, LocalDateTime createTime) {
        this.fileName = fileName;
        this.lock = new HashSet<>();
        this.createTime = createTime;
        this.updateTime = LocalDateTime.now();
    }

    public void addLock(BinlogActuator binlogActuator) {
        this.lock.add(binlogActuator);
    }

    public void removeAllLock() throws IOException {
        Iterator<BinlogActuator> iterator = lock.iterator();
        while (iterator.hasNext()){
            BinlogActuator next = iterator.next();
            next.close();
            iterator.remove();
        }
    }

    public boolean isRunning() {
        for (BinlogActuator actuator : lock){
            if(actuator.isRunning()){
                return true;
            }
        }
        return false;
    }

    public boolean isFreeLock() {
        return lock.isEmpty();
    }

    public String getFileName() {
        return fileName;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }
}