package org.dbsyncer.common.snowflake;

import org.dbsyncer.common.CommonException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@ConfigurationProperties(prefix = "dbsyncer.common.worker")
public class SnowflakeIdWorker {

    /**
     * 工作机器ID(0~31)
     */
    private long id = 1L;

    /**
     * 数据中心ID(0~31)
     */
    private long dataCenterId = 1L;

    /**
     * 毫秒内序列(0~4095)
     */
    private long sequence = 0L;

    /**
     * 上次生成ID的时间截
     */
    private long lastTimestamp = -1L;

    /**
     * 开始时间截 (2015-01-01)
     */
    private final long twepoch = 1420041600000L;

    /**
     * 机器id所占的位数
     */
    private final long workerIdBits = 5L;

    /**
     * 数据标识id所占的位数
     */
    private final long dataCenterIdBits = 5L;

    /**
     * 序列在id中占的位数
     */
    private final long sequenceBits = 12L;

    /**
     * 机器ID向左移12位
     */
    private final long workerIdShift = sequenceBits;

    /**
     * 数据标识id向左移17位(12+5)
     */
    private final long dataCenterIdShift = sequenceBits + workerIdBits;

    /**
     * 时间截向左移22位(5+5+12)
     */
    private final long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;

    /**
     * 生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)
     */
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    public SnowflakeId revert(Long id) {
        long workerId = id >> workerIdShift & ~(-1L << workerIdBits);
        long dataCenterId = id >> dataCenterIdShift & ~(-1L << dataCenterIdBits);
        long timestamp = new Long(id >> timestampLeftShift) + twepoch;
        return new SnowflakeId(workerId, dataCenterId, timestamp);
    }

    /**
     * 获得下一个ID (该方法是线程安全的)
     *
     * @return SnowflakeId
     */
    public synchronized long nextId() {
        long timestamp = timeGen();

        //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < lastTimestamp) {
            throw new CommonException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        //如果是同一时间生成的，则进行毫秒内序列
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            //毫秒内序列溢出
            if (sequence == 0) {
                //阻塞到下一个毫秒,获得新的时间戳
                timestamp = tilNextMillis(lastTimestamp);
            }
        }
        //时间戳改变，毫秒内序列重置
        else {
            sequence = 0L;
        }

        //上次生成ID的时间截
        lastTimestamp = timestamp;

        //移位并通过或运算拼到一起组成64位的ID
        return ((timestamp - twepoch) << timestampLeftShift) //
                | (dataCenterId << dataCenterIdShift) //
                | (id << workerIdShift) //
                | sequence;
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     *
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 返回以毫秒为单位的当前时间
     *
     * @return 当前时间(毫秒)
     */
    private long timeGen() {
        return Instant.now().toEpochMilli();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDataCenterId() {
        return dataCenterId;
    }

    public void setDataCenterId(long dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    private class SnowflakeId {
        private long workerId;
        private long dataCenterId;
        private long timestamp;

        public SnowflakeId(long workerId, long dataCenterId, long timestamp) {
            this.workerId = workerId;
            this.dataCenterId = dataCenterId;
            this.timestamp = timestamp;
        }

        public long getWorkerId() {
            return workerId;
        }

        public long getDataCenterId() {
            return dataCenterId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}