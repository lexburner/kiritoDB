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
import java.nio.ByteBuffer;
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
    // memory index dataStructure
    private MemoryIndex memoryIndex;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    // mmap byteBuffer start address
    private long address;
    // 当前索引写入的区域
    private CommitLog commitLog;
    // determine current index block is loaded into memory
    private volatile boolean loadFlag = false;
    private long wrotePosition;
    // use mmap to write index
    private boolean mmapFlag = false;

    public void init(String path, int no) throws IOException {
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
            loadFlag = true;
        }
        File file = new File(path + Constant.INDEX_PREFIX + no + Constant.INDEX_SUFFIX);
        if (!file.exists()) {
            file.createNewFile();
            loadFlag = true;
        }
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        if (mmapFlag) {
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * Constant.expectedNumPerPartition);
            this.address = ((DirectBuffer) mappedByteBuffer).address();
        }
        this.wrotePosition = 0;
    }

    public void load() {
        if (!mmapFlag) {
            int indexSize = commitLog.getFileLength();
            this.memoryIndex = new ArrayMemoryIndex(indexSize);
            ByteBuffer buffer = ByteBuffer.allocateDirect(indexSize * Constant.INDEX_LENGTH);
            try {
                fileChannel.read(buffer);
            } catch (IOException e) {
                logger.error("load index failed", e);
            }
            buffer.flip();
            for (int curIndex = 0; curIndex < indexSize; curIndex++) {
                buffer.position(curIndex * Constant.INDEX_LENGTH);
                long key = buffer.getLong();
                this.memoryIndex.insertIndexCache(key, curIndex);
            }
            ((DirectBuffer) buffer).cleaner().clean();
            memoryIndex.init();
            this.loadFlag = true;
        } else {
            int indexSize = commitLog.getFileLength();
            this.memoryIndex = new ArrayMemoryIndex(indexSize);
            for (int curIndex = 0; curIndex < indexSize; curIndex++) {
                this.mappedByteBuffer.position(curIndex * Constant.INDEX_LENGTH);
                long key = this.mappedByteBuffer.getLong();
                this.memoryIndex.insertIndexCache(key, curIndex);
            }
            memoryIndex.init();
            this.loadFlag = true;
        }
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
        if (!mmapFlag) {
            try {
                fileChannel.write(ByteBuffer.wrap(key));
            } catch (IOException e) {
                logger.error("failed to direct write index", e);
            }
        } else {
            UNSAFE.copyMemory(key, 16, null, address + wrotePosition, Constant.INDEX_LENGTH);
            wrotePosition += Constant.INDEX_LENGTH;
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
