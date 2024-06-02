/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.common.metric;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-06-02 22:44
 */
public class TimeMetric {
    /**
     * 槽位的数量
     */
    private int bucketSize;

    /**
     * 时间片，单位毫秒
     */
    private int slice;

    /**
     * 用于判断是否可跳过锁争抢
     */
    private int timeSliceUsed;

    /**
     * 槽位
     */
    private Bucket[] buckets;

    /**
     * 目标槽位的位置
     */
    private volatile Integer targetBucketPosition;

    /**
     * 接近目标槽位最新时间
     */
    private volatile Long latestPassedTime;

    /**
     * 进入下一个槽位时使用的锁
     */
    private ReentrantLock enterNextBucketLock;

    /**
     * 默认60个槽位，槽位的时间片为1000毫秒
     */
    public TimeMetric() {
        this(60, 1000);
    }

    /**
     * 初始化Bucket数量与每个Bucket的时间片
     *
     * @param bucketSize
     * @param slice
     */
    public TimeMetric(int bucketSize, int slice) {
        this.bucketSize = bucketSize;
        this.slice = slice;
        this.latestPassedTime = System.currentTimeMillis() - (2 * slice);
        this.timeSliceUsed = 3 * slice;
        this.targetBucketPosition = getTargetBucketPosition(System.currentTimeMillis());
        this.enterNextBucketLock = new ReentrantLock();
        this.buckets = new Bucket[bucketSize];
        for (int i = 0; i < bucketSize; i++) {
            this.buckets[i] = new Bucket();
        }
    }

    public void add(long count) {
        long passTime = System.currentTimeMillis();
        Bucket currentBucket = buckets[targetBucketPosition];
        if (passTime - latestPassedTime < slice) {
            currentBucket.add(count);
            return;
        }

        if (enterNextBucketLock.isLocked() && passTime - latestPassedTime < timeSliceUsed) {
            currentBucket.add(count);
            return;
        }

        try {
            enterNextBucketLock.lock();
            if (passTime - latestPassedTime < slice) {
                currentBucket.add(count);
                return;
            }

            int nextTargetBucketPosition = getTargetBucketPosition(passTime);
            Bucket nextBucket = buckets[nextTargetBucketPosition];
            if (nextBucket.equals(currentBucket)) {
                if (passTime - latestPassedTime >= slice) {
                    latestPassedTime = passTime;
                }
            } else {
                nextBucket.reset();
                targetBucketPosition = nextTargetBucketPosition;
                latestPassedTime = passTime;
            }
            nextBucket.add(count);
        } finally {
            enterNextBucketLock.unlock();
        }
    }

    public long getSecondsRate() {
        return System.currentTimeMillis() - latestPassedTime < slice ? buckets[targetBucketPosition].get() : 0L;
    }

    private Integer getTargetBucketPosition(long now) {
        return (int) (now / slice) % bucketSize;
    }
}