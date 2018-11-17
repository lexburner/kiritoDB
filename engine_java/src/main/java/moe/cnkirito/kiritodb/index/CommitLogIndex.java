package moe.cnkirito.kiritodb.index;

import com.carrotsearch.hppc.LongIntHashMap;
import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.data.CommitLog;
import moe.cnkirito.kiritodb.data.CommitLogAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Contended;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
@Contended
public class CommitLogIndex implements CommitLogAware {

    private final static Logger logger = LoggerFactory.getLogger(CommitLogIndex.class);
    // key 和文件逻辑偏移的映射
    private LongIntHashMap key2OffsetMap;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    // 当前索引写入的区域
    private CommitLog commitLog;
    private volatile boolean loadFlag = false;

    public void init(String path, int no) throws IOException {
        // 先创建文件夹
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
            loadFlag = true;
        }
        // 创建多个索引文件
        File file = new File(path + Constant.indexPrefix + no + Constant.indexSuffix);
        if (!file.exists()) {
            file.createNewFile();
            loadFlag = true;
        }
        // 文件position
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.indexLength * 252000 * 4);
        this.key2OffsetMap = new LongIntHashMap(252000*4, 0.99);
    }

    public void load() {
        // 说明索引文件中已经有内容，则读取索引文件内容到内存中
        MappedByteBuffer mappedByteBuffer = this.mappedByteBuffer;
        int indexSize = commitLog.getFileLength();
        for (int curIndex = 0; curIndex < indexSize; curIndex++) {
            mappedByteBuffer.position(curIndex * Constant.indexLength);
            long key = mappedByteBuffer.getLong();
            // 插入内存
            insertIndexCache(key, curIndex);
        }
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
        key2OffsetMap = null;
        commitLog = null;
        loadFlag = false;
        releaseFile();
    }

    public Long read(byte[] key) {
        int offsetInt = this.key2OffsetMap.getOrDefault(Util.bytes2Long(key), -1);
        if (offsetInt == -1) {
            return null;
        }
        return ((long) offsetInt) * Constant.valueLength;
    }

    public void write(byte[] key)  {
        ByteBuffer buffer = this.mappedByteBuffer;
        buffer.put(key);
    }

    private void insertIndexCache(long key, Integer value) {
        this.key2OffsetMap.put(key, value);
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

}
