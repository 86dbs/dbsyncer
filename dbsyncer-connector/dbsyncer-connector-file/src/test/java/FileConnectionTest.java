import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/11 20:19
 */
public class FileConnectionTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private CountDownLatch latch;

    @Test
    public void testConnection() throws InterruptedException, IOException {
        File f = new File("D:\\test\\abc.txt");
        RandomAccessFile file = new RandomAccessFile(f, "rw");

        int count = 10;
        latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            Thread t = new Thread(new WritingThread(i, file.getChannel()));
            t.start();
        }
        latch.await();
        file.close();
        InputStream fileR = new FileInputStream(f);
        List<String> strings = IOUtils.readLines(fileR, Charset.defaultCharset());
        strings.forEach(line -> logger.info("{}", line));
        fileR.close();
    }

    class WritingThread implements Runnable {

        private FileChannel channel;

        private int id;

        public WritingThread(int id, FileChannel channel) {
            this.channel = channel;
            this.id = id;
        }

        @Override
        public void run() {
            logger.info("Thread {} is Writing", id);
            try {
                for (int i = 1; i <= 5; i++) {
                    String msg = String.format("%s, %s, %s\n", Thread.currentThread().getName(), id, i);
                    this.channel.write(ByteBuffer.wrap(msg.getBytes()));
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            latch.countDown();
        }
    }

}