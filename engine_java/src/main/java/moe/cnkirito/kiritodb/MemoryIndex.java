package moe.cnkirito.kiritodb;

import com.carrotsearch.hppc.LongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class MemoryIndex {

    private final static Logger logger = LoggerFactory.getLogger(MemoryIndex.class);

    // 利用了hppc的longlonghashmap
    private LongIntHashMap[] indexCacheArray = null;
    // 分片
    private final int cacheNum = 256;
    // channel
    private FileChannel[] indexFileChannels = null;
    private MappedByteBuffer[] mappedByteBuffers = null;
    // index 分片
    private final int fileNum = 256;
    // 当前索引写入的区域
    private AtomicLong[] indexPositions = null;

    public void init(String path) throws IOException {
        // 先创建文件夹
        File dirFile = new File(path);
        boolean hasSave = true;
        if (!dirFile.exists()) {
            dirFile.mkdirs();
            hasSave = false;
        }
        // 创建多个索引文件
        List<File> files = new ArrayList<>(fileNum);
        for (int i = 0; i < fileNum; ++i) {
            File file = new File(path + Constant.IndexName + i + Constant.IndexSuffix);
            if (!file.exists()) {
                file.createNewFile();
                hasSave = false;
            }
            files.add(file);
        }
        // 文件channel
        this.indexFileChannels = new FileChannel[fileNum];
        this.mappedByteBuffers = new MappedByteBuffer[fileNum];
        // 文件position
        this.indexPositions = new AtomicLong[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            File file = files.get(i);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.indexFileChannels[i] = fileChannel;
            AtomicLong atomicLong = new AtomicLong(file.length());
            this.indexPositions[i] = atomicLong;
            mappedByteBuffers[i] = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.IndexLength * 252000);
        }
        // 创建内存索引
        this.indexCacheArray = new LongIntHashMap[cacheNum];
        for (int i = 0; i < cacheNum; ++i) {
            this.indexCacheArray[i] = new LongIntHashMap(252000, 0.99);
        }
        if (hasSave) {
            this.load();
        }

    }

    public void load() {
        // 说明索引文件中已经有内容，则读取索引文件内容到内存中
        Thread[] threads = new Thread[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    // 全局buffer
                    ByteBuffer buffer = ByteBuffer.allocateDirect(5 * 1024 * 3);
                    // 源数据
                    FileChannel indexFileChannel = indexFileChannels[index];
                    boolean endFlag = true;
                    // 加载index中数据到内存中
                    while (endFlag) {
                        buffer.clear();
                        int size = 0;
                        try {
                            size = indexFileChannel.read(buffer);
                        } catch (IOException e) {
                            logger.error("读取文件error，index={}", index, e);
                        }
                        if (size == -1) {
                            break;
                        }
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            long key = buffer.getLong();
                            int offset = buffer.getInt();
                            // 脏数据，不进行处理
                            if (offset == 0 && key == 0) {
                                endFlag = false;
                                break;
                            }
                            // 插入内存
                            insertIndexCache(key, offset);
                        }
                    }
                    ((DirectBuffer) buffer).cleaner().clean();
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error("Join Interrupted", e);
            }
        }
    }

    public void destroy() throws IOException {
        if (this.mappedByteBuffers != null) {
            for (MappedByteBuffer mappedByteBuffer : this.mappedByteBuffers) {
                Util.clean(mappedByteBuffer);
            }
        }
        if (this.indexFileChannels != null) {
            for (FileChannel fileChannel : this.indexFileChannels) {
                fileChannel.close();
            }
        }
    }

    public Long read(byte[] key) {
        int index = getPartition(key);
        LongIntHashMap map = indexCacheArray[index];
        int ans = map.getOrDefault(Util.bytes2Long(key), -1);
        if (ans == -1) {
            return null;
        }
        return ((long) ans) * Constant.ValueLength;
    }

    public void write(byte[] key, long offset) {
        int offsetInt = (int) (offset / Constant.ValueLength);
        try {
            writeIndexFile(key, offsetInt);
        } catch (Exception e) {
            logger.error("写入文件错误, error", e);
        }
    }

    private void insertIndexCache(long key, Integer value) {
        // cache分片
        int index = getPartition(Util.long2bytes(key));
        LongIntHashMap map = indexCacheArray[index];
        synchronized (map) {
            map.put(key, value);
        }
    }

    private void writeIndexFile(byte[] key, Integer value) {
        // 文件分片
        int index = getPartition(key);
        long position = this.indexPositions[index].getAndAdd(Constant.IndexLength);
        // buffer
        ByteBuffer buffer = this.mappedByteBuffers[index].slice();
        buffer.position((int) position);
        buffer.put(key);
        buffer.putInt(value);
    }

    private int getPartition(byte[] key) {
        return key[0] & 0xff;
    }
}
