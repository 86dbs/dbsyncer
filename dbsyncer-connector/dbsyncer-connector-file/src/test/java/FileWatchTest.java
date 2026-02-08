import org.apache.commons.io.IOUtils;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/6 21:32
 */
public class FileWatchTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String path = "d:/test/";
    private WatchService watchService;

    @After
    public void close() throws IOException {
        if (null != watchService) {
            watchService.close();
        }
    }

    @Test
    public void testFileWatch() throws IOException, InterruptedException {
        watchService = FileSystems.getDefault().newWatchService();
        Path p = Paths.get(path);
        p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

        logger.info("启动监听");
        long count = 0L;
        while (count < 30) {
            WatchKey watchKey = watchService.take();
            List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
            for (WatchEvent<?> event : watchEvents) {
                Object context = event.context();
                logger.info("[{}{}] 文件发生了[{}]事件", path, context, event.kind());
            }
            watchKey.reset();

            TimeUnit.SECONDS.sleep(1);
            count++;
        }
    }

    @Test
    public void testReadFile() {
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
