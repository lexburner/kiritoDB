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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class MemoryIndex {

    private final static Logger logger = LoggerFactory.getLogger(MemoryIndex.class);
    // 分片
    private final int cacheNum = 256;
    // index 分片
    private final int fileNum = 256;
    // 利用了hppc的longlonghashmap
    private LongIntHashMap[] indexCacheArray = null;
    private final MappedByteBuffer[] mappedByteBuffers = new MappedByteBuffer[fileNum];
    // 当前索引写入的区域
    private final AtomicLong[] indexPositions = new AtomicLong[fileNum];
    private CommitLog commitLog;

    public void init(String path, CommitLog commitLog) throws IOException {
        this.commitLog = commitLog;
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
        // 文件position
        for (int i = 0; i < fileNum; ++i) {
            File file = files.get(i);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.indexPositions[i] = new AtomicLong(0);
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
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch countDownLatch = new CountDownLatch(fileNum);
        for (int i = 0; i < fileNum; ++i) {
            final int index = i;
            executorService.execute(() -> {
                MappedByteBuffer mappedByteBuffer = mappedByteBuffers[index];
                int indexSize = commitLog.getFileLength(index);
                indexPositions[index].set(indexSize * 12);
                for (int curIndex = 0; curIndex < indexSize; curIndex++) {
                    mappedByteBuffer.position(curIndex * 12);
                    long key = mappedByteBuffer.getLong();
                    int offset = mappedByteBuffer.getInt();
                    // 插入内存
                    insertIndexCache(key, offset);
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("thread interrupted exception", e);
        }
        executorService.shutdown();
    }

    public void destroy() {
        for (MappedByteBuffer mappedByteBuffer : this.mappedByteBuffers) {
            Util.clean(mappedByteBuffer);
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

    public void write(byte[] key, int offsetInt) {
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
