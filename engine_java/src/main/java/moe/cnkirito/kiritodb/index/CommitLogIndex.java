package moe.cnkirito.kiritodb.index;

import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.data.CommitLog;
import moe.cnkirito.kiritodb.data.CommitLogAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Contended;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static moe.cnkirito.kiritodb.common.UnsafeUtil.UNSAFE;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
@Contended
public class CommitLogIndex implements CommitLogAware {

    private final static Logger logger = LoggerFactory.getLogger(CommitLogIndex.class);
    private MemoryIndex memoryIndex;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private long address;
    // 当前索引写入的区域
    private CommitLog commitLog;
    private volatile boolean loadFlag = false;

    //    public static final int expectedNumPerPartition = 64000;
//    public static final int expectedNumPerPartition = 253000;
    public static final int expectedNumPerPartition = Constant.expectedNumPerPartition;

    public void init(String path, int no) throws IOException {
        // 先创建文件夹
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
            loadFlag = true;
        }
        // 创建多个索引文件
        File file = new File(path + Constant.INDEX_PREFIX + no + Constant.INDEX_SUFFIX);
        if (!file.exists()) {
            file.createNewFile();
            loadFlag = true;
        }
        // 文件position
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        //todo
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * expectedNumPerPartition);
        this.address = ((DirectBuffer) mappedByteBuffer).address();
    }

    public void load() {
        // 说明索引文件中已经有内容，则读取索引文件内容到内存中
        int indexSize = commitLog.getFileLength();
        // todo 扩容校验
        if (indexSize > expectedNumPerPartition) {
            logger.info("current partition's data size more than expect, true size {}", indexSize);
            try {
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * indexSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.memoryIndex = new ArrayMemoryIndex(indexSize);
        for (int curIndex = 0; curIndex < indexSize; curIndex++) {
            this.mappedByteBuffer.position(curIndex * Constant.INDEX_LENGTH);
            long key = this.mappedByteBuffer.getLong();
            this.memoryIndex.insertIndexCache(key, curIndex);
        }
        memoryIndex.init();
        this.loadFlag = true;
    }


    public void releaseFile() throws IOException {
        if (this.mappedByteBuffer != null) {
            fileChannel.close();
            Util.clean(this.mappedByteBuffer);
            this.fileChannel = null;
            this.mappedByteBuffer = null;
        }
    }

    public void destroy() throws IOException {
        commitLog = null;
        loadFlag = false;
        releaseFile();
    }

    public Long read(byte[] key) {
        int offsetInt = this.memoryIndex.get(Util.bytes2Long(key));
        if (offsetInt < 0) {
            return null;
        }
        return ((long) offsetInt) * Constant.VALUE_LENGTH;
    }

    public void write(byte[] key) {
        int position = this.mappedByteBuffer.position();
        UNSAFE.copyMemory(key, 16, null, address + position, Constant.INDEX_LENGTH);
        try {
            if (position + Constant.INDEX_LENGTH <= mappedByteBuffer.limit()) {
                this.mappedByteBuffer.position(position + Constant.INDEX_LENGTH);
            } else {
                logger.info("trigger increase size");
                // TODO 扩容校验
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * expectedNumPerPartition * 4);
                this.mappedByteBuffer.position(position + Constant.INDEX_LENGTH);
            }
        } catch (Exception e) {
            logger.error("failed to write index with mmap", e);
        }
    }

    public boolean isLoadFlag() {
        return loadFlag;
    }

    @Override
    public CommitLog getCommitLog() {
        return this.commitLog;
    }

    @Override
    public void setCommitLog(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    public MemoryIndex getMemoryIndex() {
        return memoryIndex;
    }
}
