import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/6 21:32
 */
public class FileWatchTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String path = System.getProperty("java.io.tmpdir") + File.separator + "dbsyncer_test" + File.separator;
    private WatchService watchService;

    @After
    public void close() throws IOException {
        if (null != watchService) {
            watchService.close();
        }
    }

    @Test
    @Ignore
    public void testFileWatch() throws IOException, InterruptedException {
        // 确保测试目录存在
        Path testPath = Paths.get(path);
        if (!Files.exists(testPath)) {
            Files.createDirectories(testPath);
        }
        
        watchService = FileSystems.getDefault().newWatchService();
        Path p = Paths.get(path);
        p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

        logger.info("启动监听");
        long count = 0L;
        while (count < 5) { // 减少循环次数
            WatchKey watchKey = watchService.poll(1, TimeUnit.SECONDS); // 设置超时时间
            if (watchKey != null) {
                List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                for (WatchEvent<?> event : watchEvents) {
                    Object context = event.context();
                    logger.info("[{}{}] 文件发生了[{}]事件", path, context, event.kind());
                }
                watchKey.reset();
            }
            
            TimeUnit.SECONDS.sleep(1);
            count++;
        }
    }

    @Test
    @Ignore
    public void testReadFile() throws IOException {
        // 确保测试目录存在
        Path testPath = Paths.get(path);
        if (!Files.exists(testPath)) {
            Files.createDirectories(testPath);
        }
        
        // 创建测试文件
        Path testFile = Paths.get(path, "test.txt");
        if (!Files.exists(testFile)) {
            Files.createFile(testFile);
        }
        
        read(path + "test.txt");
    }

    private void read(String file) {
        RandomAccessFile raf = null;
        byte[] buffer = new byte[4096];
        try {
            raf = new RandomAccessFile(file, "r");
            raf.seek(raf.length());
            logger.info("offset:{}", raf.getFilePointer());

            while (true) {
                int len = raf.read(buffer);
                if (-1 != len) {
                    logger.info("offset:{}, len:{}", raf.getFilePointer(), len);
                    logger.info(new String(buffer, 1, len, "UTF-8"));
                }
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(raf);
        }
    }

}