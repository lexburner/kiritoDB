package moe.cnkirito.kiritodb;

import com.carrotsearch.hppc.LongLongHashMap;

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

    private Object indexPutLock = new Object();
    private LongLongHashMap indexes;
    private FileChannel indexFileChannel;
    static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));
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
            this.indexFileChannel = randomAccessFile.getChannel();
            endPosition = indexFileChannel.size();
            wrotePosition = new AtomicLong(indexFileChannel.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long num = endPosition / 16;
        for (long i = 0; i < num; i++) {
            ByteBuffer byteBuffer = bufferThreadLocal.get();
            byteBuffer.clear();
            try {
                indexFileChannel.read(byteBuffer);
                byteBuffer.flip();
                synchronized (indexPutLock) {
                    this.indexes.put(byteBuffer.getLong(), byteBuffer.getLong(8));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void recordPosition(byte[] key, Long position) {
        position++;
        synchronized (indexPutLock) {
            this.indexes.put(byteArrayToLong(key), position);
        }
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        buffer.put(key);
        buffer.putLong(position);
        buffer.flip();
        try {
            indexFileChannel.write(buffer, wrotePosition.getAndAdd(16));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Long getPosition(byte[] key) {
        long l;
        synchronized (indexPutLock) {
            l = this.indexes.get(byteArrayToLong(key)) - 1;
        }
        if (l == -1L)
            return null;
        else
            return l;
    }

    public void close() {
    }

    public static long byteArrayToLong(byte[] b) {
        long values = 0;
        for (int i = 0; i < 8; i++) {
            values <<= 8;
            values |= (b[i] & 0xff);
        }
        return values;
    }

}
