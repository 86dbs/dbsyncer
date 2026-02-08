/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据分片工具
 * 用于大数据量传输时的分片处理
 *
 * @author 穿云
 * @version 1.0.0
 */
public class DataChunkUtil {

    private static final Logger logger = LoggerFactory.getLogger(DataChunkUtil.class);

    // 默认分片大小：1MB（字节）
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;

    // 最大分片大小：10MB
    private static final int MAX_CHUNK_SIZE = 10 * 1024 * 1024;

    // 最小分片大小：100KB
    private static final int MIN_CHUNK_SIZE = 100 * 1024;

    /**
     * 将数据分片
     * 
     * @param data 原始数据（JSON字符串）
     * @param chunkSize 分片大小（字节），如果<=0则使用默认值
     * @return 分片列表
     */
    public static List<DataChunk> chunkData(String data, int chunkSize) {
        if (StringUtil.isBlank(data)) {
            return new ArrayList<>();
        }

        if (chunkSize <= 0) {
            chunkSize = DEFAULT_CHUNK_SIZE;
        }

        // 限制分片大小范围
        if (chunkSize > MAX_CHUNK_SIZE) {
            chunkSize = MAX_CHUNK_SIZE;
            logger.warn("分片大小超过最大值，使用最大值: {}MB", MAX_CHUNK_SIZE / 1024 / 1024);
        }
        if (chunkSize < MIN_CHUNK_SIZE) {
            chunkSize = MIN_CHUNK_SIZE;
            logger.warn("分片大小小于最小值，使用最小值: {}KB", MIN_CHUNK_SIZE / 1024);
        }

        byte[] dataBytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int totalSize = dataBytes.length;
        int totalChunks = (int) Math.ceil((double) totalSize / chunkSize);

        List<DataChunk> chunks = new ArrayList<>();
        String chunkId = generateChunkId();

        for (int i = 0; i < totalChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, totalSize);
            byte[] chunkBytes = new byte[end - start];
            System.arraycopy(dataBytes, start, chunkBytes, 0, end - start);

            DataChunk chunk = new DataChunk();
            chunk.setChunkId(chunkId);
            chunk.setChunkIndex(i);
            chunk.setTotalChunks(totalChunks);
            chunk.setData(new String(chunkBytes, java.nio.charset.StandardCharsets.UTF_8));
            chunk.setSize(chunkBytes.length);

            chunks.add(chunk);
        }

        logger.info("数据分片完成，总大小: {}KB, 分片数: {}, 分片大小: {}KB", totalSize / 1024, totalChunks, chunkSize / 1024);

        return chunks;
    }

    /**
     * 将数据分片（使用默认分片大小）
     * 
     * @param data 原始数据（JSON字符串）
     * @return 分片列表
     */
    public static List<DataChunk> chunkData(String data) {
        return chunkData(data, DEFAULT_CHUNK_SIZE);
    }

    /**
     * 合并分片数据
     * 
     * @param chunks 分片列表
     * @return 合并后的数据
     */
    public static String mergeChunks(List<DataChunk> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            return "";
        }

        // 按索引排序
        chunks.sort((a, b)->Integer.compare(a.getChunkIndex(), b.getChunkIndex()));

        // 验证分片完整性
        String chunkId = chunks.get(0).getChunkId();
        int totalChunks = chunks.get(0).getTotalChunks();

        if (chunks.size() != totalChunks) {
            throw new RuntimeException(String.format("分片不完整，期望: %d, 实际: %d", totalChunks, chunks.size()));
        }

        for (int i = 0; i < chunks.size(); i++) {
            DataChunk chunk = chunks.get(i);
            if (!chunkId.equals(chunk.getChunkId())) {
                throw new RuntimeException(String.format("分片ID不一致，索引: %d", i));
            }
            if (chunk.getChunkIndex() != i) {
                throw new RuntimeException(String.format("分片索引不连续，期望: %d, 实际: %d", i, chunk.getChunkIndex()));
            }
            if (chunk.getTotalChunks() != totalChunks) {
                throw new RuntimeException(String.format("分片总数不一致，索引: %d", i));
            }
        }

        // 合并数据
        StringBuilder sb = new StringBuilder();
        for (DataChunk chunk : chunks) {
            sb.append(chunk.getData());
        }

        logger.info("分片合并完成，分片数: {}, 总大小: {}KB", totalChunks, sb.length() / 1024);

        return sb.toString();
    }

    /**
     * 生成分片ID
     */
    private static String generateChunkId() {
        return UUIDUtil.getUUID();
    }

    /**
     * 数据分片
     */
    public static class DataChunk {

        private String chunkId; // 分片ID（同一批分片使用相同ID）
        private int chunkIndex; // 分片索引（从0开始）
        private int totalChunks; // 总分片数
        private String data; // 分片数据
        private int size; // 分片大小（字节）

        public String getChunkId() {
            return chunkId;
        }

        public void setChunkId(String chunkId) {
            this.chunkId = chunkId;
        }

        public int getChunkIndex() {
            return chunkIndex;
        }

        public void setChunkIndex(int chunkIndex) {
            this.chunkIndex = chunkIndex;
        }

        public int getTotalChunks() {
            return totalChunks;
        }

        public void setTotalChunks(int totalChunks) {
            this.totalChunks = totalChunks;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }
}
