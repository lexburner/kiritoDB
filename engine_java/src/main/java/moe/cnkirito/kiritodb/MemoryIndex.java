package moe.cnkirito.kiritodb;

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

    private Map<ComparableByteArray, Long> indexes;
    private MappedByteBuffer mappedByteBuffer;
    static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));

    public MemoryIndex(String path) {
        this.indexes = new ConcurrentHashMap<>();
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
            mappedByteBuffer.get(key);
            boolean flag = true;
            for (int i = 0; i < 8; i++) {
                if (key[i] != (byte) 0) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                mappedByteBuffer.position(mappedByteBuffer.position() - 8);
                break;
            }
            this.indexes.put(new ComparableByteArray(key), mappedByteBuffer.getLong());
        } while (true);


    }

    public void recordPosition(byte[] key, Long position) {
        this.indexes.put(new ComparableByteArray(key), position);
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        buffer.put(key);
        buffer.putLong(position);
        buffer.flip();
        mappedByteBuffer.put(buffer);
    }

    public Long getPosition(byte[] key) {
        return this.indexes.get(new ComparableByteArray(key));
    }

    public void close() {
        mappedByteBuffer.force();
    }
}
