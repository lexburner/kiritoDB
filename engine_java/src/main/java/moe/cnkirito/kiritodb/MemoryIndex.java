package moe.cnkirito.kiritodb;

import com.carrotsearch.hppc.LongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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

    // 利用了hppc的longlonghashmap
    private LongIntHashMap[] indexCacheArray = null;
    // 分片
    private final int cacheNum = 1024;
    // channel
    private FileChannel[] indexFileChannels = null;
    // index 分片
    private final int fileNum = 1024;
    // 当前索引写入的区域
    private AtomicLong[] indexPositions = null;
    // buffer
    private ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.IndexLength));

    public void init(String path) throws IOException {
        // 先创建文件夹
        File dirFile = new File(path);
        boolean hasSave = true;
        if (!dirFile.exists()) {
            if (dirFile.mkdirs()) {
                logger.info("创建文件夹成功,dir=" + path);
            } else {
                logger.error("创建文件夹失败,dir=" + path);
            }
            hasSave = false;
        }
        // 创建多个索引文件
        List<File> files = new ArrayList<>(fileNum);
        for (int i = 0; i < fileNum; ++i) {
            File file = new File(path + Constant.IndexName + i + Constant.IndexSuffix);
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    logger.error("创建文件失败,file=" + file.getPath());
                }
                hasSave = false;
            }
            files.add(file);
        }
        // 文件channel
        this.indexFileChannels = new FileChannel[fileNum];
        // 文件position
        this.indexPositions = new AtomicLong[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            File file = files.get(i);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.indexFileChannels[i] = fileChannel;
            AtomicLong atomicLong = new AtomicLong(file.length());
            this.indexPositions[i] = atomicLong;
        }
        // 创建内存索引
        this.indexCacheArray = new LongIntHashMap[cacheNum];
        for (int i = 0; i < cacheNum; ++i) {
            this.indexCacheArray[i] = new LongIntHashMap();
        }
        if (!hasSave) {
            logger.info("第一次进入索引文件，里面没内容，所以不用初始化到内存中");
            return;
        }
        CountDownLatch countDownLatch = new CountDownLatch(fileNum);
        long tmp = System.currentTimeMillis();
        // 说明索引文件中已经有内容，则读取索引文件内容到内存中
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        for (int i = 0; i < fileNum; ++i) {
            final int index = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    // 源数据
                    FileChannel indexFileChannel = indexFileChannels[index];
                    long len = indexPositions[index].get();
                    if (len > 0) {
                        // 全局buffer
                        ByteBuffer buffer = ByteBuffer.allocate((int) len);
                        // 加载index中数据到内存中
                        try {
                            indexFileChannel.read(buffer);
                        } catch (IOException e) {
                            logger.error("读取文件error，index=" + index, e);
                        }
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            long key = buffer.getLong();
                            int offset = buffer.getInt();
                            // 脏数据，不进行处理
                            if (offset <= 0) {
                                logger.error("offset < 0");
                                continue;
                            }
                            // 插入内存
                            insertIndexCache(key, offset);
                        }
                    }
                    // 计数器减1
                    countDownLatch.countDown();
                }
            });
        }
        executorService.shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("多线程读取index文件失败", e);
        }
        logger.info("end load index" + (System.currentTimeMillis() - tmp) + "ms");

    }

    public void destroy() throws IOException {
        if (this.indexFileChannels != null) {
            for (FileChannel fileChannel : this.indexFileChannels) {
                fileChannel.force(true);
            }
        }
        this.indexFileChannels = null;
        this.indexPositions = null;
        this.indexCacheArray = null;
    }

    public Long read(long key) {
        // 分片的位置
        int index = (int) (Math.abs(key) % cacheNum);
        LongIntHashMap map = indexCacheArray[index];
        int ans = map.get(key);
        // 不存在offset
        if (ans == 0) {
            return null;
        }
        // offset-1。因为之前加过1
        return ((long) ans - 1) * Constant.ValueLength;
    }

    public void write(long key, long offset) throws IOException {
        // 为了让offset大于0
        int offsetInt = (int) (offset / Constant.ValueLength) + 1;
        try {
            // 写入到索引文件
            writeIndexFile(key, offsetInt);
        } catch (Exception e) {
            logger.error("写入文件错误, error", e);
        }
        // 写入到内存
        insertIndexCache(key, offsetInt);
    }

    private void insertIndexCache(Long key, Integer value) {
        // cache分片
        int index = (int) (Math.abs(key) % cacheNum);
        // 写入内存。因为LongLongHashMap不支持并发，所以加锁
        LongIntHashMap map = indexCacheArray[index];
        synchronized (map) {
            map.put(key, value);
        }
    }

    private void writeIndexFile(long key, Integer value) throws Exception {
        // 文件分片
        int index = (int) (Math.abs(key) % fileNum);
        // buffer
        ByteBuffer buffer = this.bufferThreadLocal.get();
        buffer.clear();
        buffer.putLong(key);
        buffer.putInt(value);
        buffer.flip();
        // 加载元数据
        long idx = this.indexPositions[index].getAndAdd(Constant.IndexLength);
        FileChannel fileChannel = this.indexFileChannels[index];
        // 写入数据
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer, idx + (Constant.IndexLength - buffer.remaining()));
        }
    }

}
