package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.Util;
import com.carrotsearch.hppc.LongLongHashMap;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class MemoryIndex {

    Logger logger = LoggerFactory.getLogger(MemoryIndex.class);

    private Object indexPutLock = new Object();
    private RandomAccessFile randomAccessFile;
    private LongLongHashMap indexes;
    private FileChannel indexFileChannel;
    private ObjectPool<byte[]> bytesPool = new GenericObjectPool<byte[]>(new BytesPoolableFactory(16));
    private AtomicLong wrotePosition;

    public MemoryIndex(String path) {
        this.indexes = new LongLongHashMap();
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long endPosition = 0L;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.randomAccessFile = randomAccessFile;
            this.indexFileChannel = randomAccessFile.getChannel();
            endPosition = randomAccessFile.length();
            wrotePosition = new AtomicLong(randomAccessFile.length());
        } catch (IOException e) {
            logger.error("file not found", e);
        }
        long num = endPosition / 16;
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        for (long i = 0; i < num; i++) {
            byteBuffer.clear();
            try {
                indexFileChannel.read(byteBuffer);
                byteBuffer.flip();
                synchronized (indexPutLock) {
                    long key = byteBuffer.getLong();
                    long offset = byteBuffer.getLong();
                    this.indexes.put(key, offset);
                }
            } catch (IOException e) {
                logger.error("io exception", e);
            }
        }
        logger.info("load index from file to memory, num: {}", num);
    }

    public void recordPosition(byte[] key, long position) {
        // 先存文件
        position++;
        byte[] buffer = null;
        try {
            buffer = bytesPool.borrowObject();
        } catch (Exception e) {
            logger.error("borrowObject failed", e);
        }
        System.arraycopy(key, 0, buffer, 0, 8);
        System.arraycopy(Util.long2bytes(position), 0, buffer, 8, 8);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        try {
            long offset = wrotePosition.getAndAdd(16);
            while (byteBuffer.hasRemaining()) {
                indexFileChannel.write(byteBuffer, offset + (16 - byteBuffer.remaining()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 后放内存
        synchronized (indexPutLock) {
            this.indexes.put(byteArrayToLong(key), position);
        }
    }

    public Long getPosition(byte[] key) {
        long offset = this.indexes.get(byteArrayToLong(key));
        if (offset == 0L)
            return null;
        else
            return offset - 1;
    }

    public void close() {
        if (this.indexFileChannel != null) {
            try {
                this.indexFileChannel.force(true);
            } catch (IOException e) {
                logger.error("force error", e);
            }
        }
        if (randomAccessFile != null) {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                logger.error("randomAccessFile close error", e);
            }
        }
        logger.info("memoryIndex closed.");
    }

    public static long byteArrayToLong(byte[] buffer) {
        long values = 0;
        int len = 8;
        // 8 与 buffer.length 较小者
        len = len > buffer.length ? buffer.length : len;
        for (int i = 0; i < len; ++i) {
            values <<= 8;
            values |= (buffer[i] & 0xff);
        }
        return values;
    }

}
