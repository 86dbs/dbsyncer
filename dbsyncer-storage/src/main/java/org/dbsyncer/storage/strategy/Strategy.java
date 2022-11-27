package org.dbsyncer.storage.strategy;

/**
 * 创建数据存放的集合ID
 * <pre>
 * /data
 * ----/config 连接器、驱动、运行状态
 * ----/log 连接器、驱动、系统
 * ----/data 驱动实时同步数据
 * --------/driver1
 * --------/driver2
 * --------/driver...
 * </pre>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/15 22:31
 */
public interface Strategy {

    /**
     * 创建分片
     *
     * @param separator
     * @param collectionId
     * @return
     */
    String createSharding(String separator, String collectionId);

}