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
    private final int cacheNum = 1000;
    // channel
    private FileChannel[] indexFileChannels = null;
    private MappedByteBuffer[] mappedByteBuffers = null;
    // index 分片
    private final int fileNum = 38;
    // 当前索引写入的区域
    private AtomicLong[] indexPositions = null;
    private volatile boolean loadFlag = false;

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
        this.mappedByteBuffers = new MappedByteBuffer[fileNum];
        // 文件position
        this.indexPositions = new AtomicLong[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            File file = files.get(i);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.indexFileChannels[i] = fileChannel;
            AtomicLong atomicLong = new AtomicLong(file.length());
            this.indexPositions[i] = atomicLong;
            mappedByteBuffers[i] = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 800 * 1024 * 1024 / fileNum);
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
        this.load();
    }

    public void load(){
        CountDownLatch countDownLatch = new CountDownLatch(fileNum);
        long tmp = System.currentTimeMillis();
        // 说明索引文件中已经有内容，则读取索引文件内容到内存中
        for (int i = 0; i < fileNum; ++i) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    // 全局buffer
                    ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024 * 3);
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
                            logger.error("读取文件error，index=" + index, e);
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
                    // 计数器减1
                    countDownLatch.countDown();
                }
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("多线程读取index文件失败", e);
        }
        logger.info("end load index" + (System.currentTimeMillis() - tmp) + "ms");
        this.loadFlag = true;
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
//        insertIndexCache(key, offsetInt);
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
        long position = this.indexPositions[index].getAndAdd(Constant.IndexLength);
        // buffer
        ByteBuffer buffer = this.mappedByteBuffers[index].slice();
        buffer.position((int) position);
        buffer.putLong(key);
        buffer.putInt(value);
        // 加载元数据
    }

    public boolean isLoadFlag() {
        return loadFlag;
    }

    public void setLoadFlag(boolean loadFlag) {
        this.loadFlag = loadFlag;
    }
}
