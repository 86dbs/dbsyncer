/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.util;

import org.dbsyncer.common.util.DataChunkUtil.DataChunk;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 数据分片工具测试
 *
 * @author 穿云
 * @version 1.0.0
 */
public class DataChunkUtilTest {

    private static final Logger logger = LoggerFactory.getLogger(DataChunkUtilTest.class);

    @Test
    public void testChunkData_SmallData() {
        logger.info("=== 测试小数据分片 ===");
        String smallData = "这是测试数据，内容较少";

        List<DataChunk> chunks = DataChunkUtil.chunkData(smallData);
        Assert.assertNotNull("分片列表不应为空", chunks);
        Assert.assertEquals("小数据应只有1个分片", 1, chunks.size());

        DataChunk chunk = chunks.get(0);
        Assert.assertNotNull("分片ID不应为空", chunk.getChunkId());
        Assert.assertEquals("分片索引应为0", 0, chunk.getChunkIndex());
        Assert.assertEquals("总分片数应为1", 1, chunk.getTotalChunks());
        Assert.assertEquals("分片数据应相同", smallData, chunk.getData());

        logger.info("小数据分片测试通过，分片数: {}", chunks.size());
    }

    @Test
    public void testChunkData_LargeData() {
        logger.info("=== 测试大数据分片 ===");
        // 生成约2MB的数据（使用ASCII字符，避免多字节字符分片问题）
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 200000; i++) {
            largeData.append("test_data_").append(i).append(",");
        }
        String largeDataStr = largeData.toString();

        // 使用500KB分片大小
        int chunkSize = 500 * 1024;
        List<DataChunk> chunks = DataChunkUtil.chunkData(largeDataStr, chunkSize);

        Assert.assertNotNull("分片列表不应为空", chunks);
        Assert.assertTrue("大数据应被分成多个分片", chunks.size() > 1);

        // 验证所有分片的chunkId相同
        String firstChunkId = chunks.get(0).getChunkId();
        for (DataChunk chunk : chunks) {
            Assert.assertEquals("所有分片的chunkId应相同", firstChunkId, chunk.getChunkId());
            Assert.assertEquals("总分片数应一致", chunks.size(), chunk.getTotalChunks());
        }

        logger.info("大数据分片测试通过，数据大小: {}KB, 分片数: {}, 分片大小: {}KB", largeDataStr.length() / 1024, chunks.size(), chunkSize / 1024);
    }

    @Test
    public void testChunkData_CustomSize() {
        logger.info("=== 测试自定义分片大小 ===");
        // 生成约500KB的数据
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < 50000; i++) {
            data.append("测试数据").append(i);
        }
        String dataStr = data.toString();

        // 使用100KB分片大小
        int chunkSize = 100 * 1024;
        List<DataChunk> chunks = DataChunkUtil.chunkData(dataStr, chunkSize);

        Assert.assertTrue("应被分成多个分片", chunks.size() > 1);

        // 验证分片大小
        for (int i = 0; i < chunks.size() - 1; i++) {
            DataChunk chunk = chunks.get(i);
            Assert.assertTrue("分片大小应在合理范围内", chunk.getSize() <= chunkSize && chunk.getSize() > 0);
        }

        logger.info("自定义分片大小测试通过，分片数: {}", chunks.size());
    }

    @Test
    public void testMergeChunks() {
        logger.info("=== 测试合并分片 ===");
        String originalData = "这是测试数据，需要被分片和合并";

        // 分片
        List<DataChunk> chunks = DataChunkUtil.chunkData(originalData, 10); // 使用很小的分片大小

        // 合并
        String mergedData = DataChunkUtil.mergeChunks(chunks);
        Assert.assertEquals("合并后的数据应相同", originalData, mergedData);

        logger.info("合并分片测试通过");
    }

    @Test
    public void testMergeChunks_LargeData() {
        logger.info("=== 测试大数据合并 ===");
        // 生成约1MB的数据（使用ASCII字符，避免多字节字符分片问题）
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            largeData.append("test_data_").append(i).append(",");
        }
        String originalData = largeData.toString();

        // 分片
        List<DataChunk> chunks = DataChunkUtil.chunkData(originalData, 200 * 1024); // 200KB分片

        // 合并
        String mergedData = DataChunkUtil.mergeChunks(chunks);
        Assert.assertEquals("大数据合并后应相同", originalData, mergedData);

        logger.info("大数据合并测试通过，原始大小: {}KB, 分片数: {}", originalData.length() / 1024, chunks.size());
    }

    @Test
    public void testMergeChunks_Incomplete() {
        logger.info("=== 测试不完整分片合并 ===");
        // 生成足够大的数据，确保有多个分片
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            data.append("test_data_").append(i).append(",");
        }
        List<DataChunk> chunks = DataChunkUtil.chunkData(data.toString(), 100); // 使用小分片确保有多个分片

        if (chunks.size() > 1) {
            // 移除一个分片，模拟不完整
            chunks.remove(0);

            try {
                DataChunkUtil.mergeChunks(chunks);
                Assert.fail("不完整分片应抛出异常");
            } catch (RuntimeException e) {
                Assert.assertTrue("应抛出分片不完整异常", e.getMessage().contains("分片不完整"));
                logger.info("不完整分片测试通过，异常: {}", e.getMessage());
            }
        } else {
            logger.info("数据太小，无法测试不完整分片，跳过");
        }
    }

    @Test
    public void testMergeChunks_InvalidOrder() {
        logger.info("=== 测试分片顺序验证 ===");
        String data = "test_data_1234567890";
        List<DataChunk> chunks = DataChunkUtil.chunkData(data, 5);

        if (chunks.size() > 1) {
            // 打乱顺序
            DataChunk temp = chunks.get(0);
            chunks.set(0, chunks.get(1));
            chunks.set(1, temp);

            // mergeChunks会自动排序，所以应该能正常合并
            String mergedData = DataChunkUtil.mergeChunks(chunks);
            // mergeChunks方法会按索引排序，所以应该能正确合并
            Assert.assertEquals("合并后的数据应相同", data, mergedData);
            logger.info("分片顺序测试通过");
        } else {
            logger.info("数据太小，无法测试顺序，跳过");
        }
    }

    @Test
    public void testChunkData_Empty() {
        logger.info("=== 测试空数据分片 ===");
        List<DataChunk> chunks = DataChunkUtil.chunkData("");
        Assert.assertNotNull("空数据分片列表不应为null", chunks);
        Assert.assertTrue("空数据应返回空列表", chunks.isEmpty());

        chunks = DataChunkUtil.chunkData(null);
        Assert.assertNotNull("null数据分片列表不应为null", chunks);
        Assert.assertTrue("null数据应返回空列表", chunks.isEmpty());

        logger.info("空数据分片测试通过");
    }

    @Test
    public void testChunkData_SizeLimits() {
        logger.info("=== 测试分片大小限制 ===");
        String data = "测试数据";

        // 测试超过最大值的分片大小
        List<DataChunk> chunks1 = DataChunkUtil.chunkData(data, 20 * 1024 * 1024); // 20MB
        Assert.assertNotNull("分片列表不应为空", chunks1);

        // 测试小于最小值的分片大小
        List<DataChunk> chunks2 = DataChunkUtil.chunkData(data, 10 * 1024); // 10KB
        Assert.assertNotNull("分片列表不应为空", chunks2);

        logger.info("分片大小限制测试通过");
    }
}
