package moe.cnkirito.kiritodb;

import com.carrotsearch.hppc.LongLongHashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class MemoryIndex {

    private Object indexPutLock = new Object();
    private LongLongHashMap indexes;
    private MappedByteBuffer mappedByteBuffer;
    static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));

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
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024L * 1024L * 1024L * 3 / 2);
//            MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024L * 1024L );
            this.mappedByteBuffer = mappedByteBuffer;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        do {
            byte[] key = new byte[8];
            byte[] position = new byte[8];
            mappedByteBuffer.get(key);
            mappedByteBuffer.get(position);
            boolean flag = true;
            for (int i = 0; i < 8; i++) {
                if (key[i] != (byte) 0 || position[i] != (byte) 0) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                mappedByteBuffer.position(mappedByteBuffer.position() - 16);
                break;
            }
            synchronized (indexPutLock) {
                this.indexes.put(byteArrayToLong(key), byteArrayToLong(position));
            }
        } while (true);

    }

    public void recordPosition(byte[] key, Long position) {
        synchronized (indexPutLock) {
            this.indexes.put(byteArrayToLong(key), position);
        }
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        buffer.put(key);
        buffer.putLong(position);
        buffer.flip();
        synchronized (this){
            mappedByteBuffer.put(buffer);
        }
    }

    public Long getPosition(byte[] key) {
        return this.indexes.get(byteArrayToLong(key));
    }

    public void close() {
        mappedByteBuffer.force();
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
