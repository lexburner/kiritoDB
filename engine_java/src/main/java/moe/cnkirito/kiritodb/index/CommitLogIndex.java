package moe.cnkirito.kiritodb.index;

import moe.cnkirito.directio.DirectIOLib;
import moe.cnkirito.directio.DirectIOUtils;
import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.data.CommitLog;
import moe.cnkirito.kiritodb.data.CommitLogAware;
import net.smacke.jaydio.DirectRandomAccessFile;
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

import static moe.cnkirito.kiritodb.common.Constant._4kb;
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
    private DirectRandomAccessFile directRandomAccessFile;
    // mmap byteBuffer start address
    private long address;
    // 当前索引写入的区域
    private CommitLog commitLog;
    // determine current index block is loaded into memory
    private volatile boolean loadFlag = false;
    private boolean mmapFlag;
    private boolean dioSupport;

    // for direct write
    private moe.cnkirito.directio.DirectRandomAccessFile directFileForWrite;
    private ByteBuffer writeBuffer;
    private long wrotePosition;
    private int bufferPosition;
    private int bufferFullSize;

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
        mmapFlag = false;
        if(DirectIOLib.binit){
            directRandomAccessFile = new DirectRandomAccessFile(file, "r");
            directFileForWrite = new moe.cnkirito.directio.DirectRandomAccessFile(file, "rw");
        }
        dioSupport = false;
        bufferFullSize = 2;
        if (dioSupport) {
            writeBuffer = DirectIOUtils.allocateForDirectIO(Constant.directIOLib, Constant.INDEX_LENGTH * bufferFullSize);
            wrotePosition = 0;
            bufferPosition = 0;
            address = ((DirectBuffer) writeBuffer).address();
        }else {
            writeBuffer = ByteBuffer.allocateDirect(Constant.INDEX_LENGTH * bufferFullSize);
            wrotePosition = 0;
            bufferPosition = 0;
            address = ((DirectBuffer) writeBuffer).address();
        }
    }

    public void load() {
        int indexSize = commitLog.getFileLength();
        this.memoryIndex = new ArrayMemoryIndex(indexSize);
        if (indexSize == 0) {
            return;
        }
        if (DirectIOLib.binit) {
            // todo
            ByteBuffer buffer = ByteBuffer.allocate((indexSize * Constant.INDEX_LENGTH / _4kb + 1) * _4kb);
            try {
                directRandomAccessFile.read(buffer.array());
            } catch (IOException e) {
                logger.error("load index failed", e);
            }
            buffer.position(0);
            buffer.limit(indexSize * Constant.INDEX_LENGTH);
            for (int curIndex = 0; curIndex < indexSize; curIndex++) {
                buffer.position(curIndex * Constant.INDEX_LENGTH);
                long key = buffer.getLong();
                this.memoryIndex.insertIndexCache(key, curIndex);
            }
        } else {
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
        if (bufferPosition > 0) {
            if(dioSupport){
                this.writeBuffer.position(0);
                this.writeBuffer.limit(bufferFullSize * Constant.INDEX_LENGTH);
                this.directFileForWrite.write(writeBuffer, this.wrotePosition);
            }else {
                this.writeBuffer.position(0);
                this.writeBuffer.limit(bufferFullSize * Constant.INDEX_LENGTH);
                this.fileChannel.write(writeBuffer, this.wrotePosition);
            }

        }
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
            if(dioSupport){
                try {
                    UNSAFE.copyMemory(key, 16, null, address + bufferPosition * Constant.INDEX_LENGTH, Constant.INDEX_LENGTH);
                    bufferPosition++;
                    if (bufferPosition >= bufferFullSize) {
                        this.writeBuffer.position(0);
                        this.writeBuffer.limit(bufferPosition * Constant.INDEX_LENGTH);
                        this.directFileForWrite.write(writeBuffer, this.wrotePosition);
                        this.wrotePosition += Constant.INDEX_LENGTH * bufferPosition;
                        bufferPosition = 0;
                    }
                } catch (IOException e) {
                    logger.error("failed to direct write index", e);
                }
            }else {
                try {
                    UNSAFE.copyMemory(key, 16, null, address + bufferPosition * Constant.INDEX_LENGTH, Constant.INDEX_LENGTH);
                    bufferPosition++;
                    if (bufferPosition >= bufferFullSize) {
                        this.writeBuffer.position(0);
                        this.writeBuffer.limit(bufferPosition * Constant.INDEX_LENGTH);
                        this.fileChannel.write(writeBuffer, this.wrotePosition);
                        this.wrotePosition += Constant.INDEX_LENGTH * bufferPosition;
                        bufferPosition = 0;
                    }
                } catch (IOException e) {
                    logger.error("failed to direct write index", e);
                }
            }
        } else {
            if (this.mappedByteBuffer == null) {
                try {
                    this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * Constant.expectedNumPerPartition);
                } catch (IOException e) {
                    logger.error("mmap failed", e);
                }
                this.address = ((DirectBuffer) mappedByteBuffer).address();
                this.wrotePosition = 0;
            }
            if (this.wrotePosition >= this.mappedByteBuffer.limit() - Constant.INDEX_LENGTH) {
                try {
                    this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * 203000);
                } catch (IOException e) {
                    logger.error("mmap failed", e);
                }
                this.address = ((DirectBuffer) mappedByteBuffer).address();
            }
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
