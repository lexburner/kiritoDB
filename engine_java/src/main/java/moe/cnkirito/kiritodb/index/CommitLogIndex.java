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
import java.util.Arrays;
import java.util.Comparator;

import static moe.cnkirito.kiritodb.common.UnsafeUtil.UNSAFE;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
@Contended
public class CommitLogIndex implements CommitLogAware {

    private final static Logger logger = LoggerFactory.getLogger(CommitLogIndex.class);
    // key 和文件逻辑偏移的映射
    private IndexEntry[] indexEntries;
    private static ThreadLocal<IndexEntry> entryForSearch = ThreadLocal.withInitial(()->new IndexEntry(-1,-1));
    private int indexSize;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private long address;
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
        File file = new File(path + Constant.INDEX_PREFIX + no + Constant.INDEX_SUFFIX);
        if (!file.exists()) {
            file.createNewFile();
            loadFlag = true;
        }
        // 文件position
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.INDEX_LENGTH * 252000 * 4);
        this.address = ((DirectBuffer) mappedByteBuffer).address();
        this.indexEntries = new IndexEntry[252000 * 4];
        this.indexSize = 0;
    }

    public void load() {
        // 说明索引文件中已经有内容，则读取索引文件内容到内存中
        MappedByteBuffer mappedByteBuffer = this.mappedByteBuffer;
        this.indexSize = commitLog.getFileLength();
        for (int curIndex = 0; curIndex < indexSize; curIndex++) {
            mappedByteBuffer.position(curIndex * Constant.INDEX_LENGTH);
            long key = mappedByteBuffer.getLong();
            // 插入内存
            insertIndexCache(key, curIndex);
        }
        sortAndCompact();
        this.loadFlag = true;
    }

    private void sortAndCompact() {
        Arrays.sort(indexEntries, 0, this.indexSize, (a, b) -> {
            if (a.getKey() == b.getKey()) {
                return Integer.compare(a.getOffsetInt(), b.getOffsetInt());
            } else {
                return Long.compare(a.getKey(), b.getKey());
            }
        });
        IndexEntry[] newIndexEntries = new IndexEntry[252000 * 4];
        newIndexEntries[0] = indexEntries[0];
        int newIndexSize = 1;
        for (int i = 1; i < this.indexSize; i++) {
            if (indexEntries[i].getKey() != indexEntries[i - 1].getKey()) {
                newIndexSize++;
            }
            newIndexEntries[newIndexSize - 1] = indexEntries[i];
        }
        this.indexEntries = newIndexEntries;
        this.indexSize = newIndexSize;
    }

    private int binarySearchPosition(long key) {
        IndexEntry indexEntry = entryForSearch.get();
        indexEntry.setKey(key);
        int index = Arrays.binarySearch(indexEntries, 0, indexSize, indexEntry, Comparator.comparingLong(IndexEntry::getKey));
        if (index >= 0) {
            return this.indexEntries[index].getOffsetInt();
        } else {
            return -1;
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
        indexEntries = null;
        commitLog = null;
        loadFlag = false;
        releaseFile();
    }

    public Long read(byte[] key) {
        int offsetInt = this.binarySearchPosition(Util.bytes2Long(key));
        if (offsetInt < 0) {
            return null;
        }
        return ((long) offsetInt) * Constant.VALUE_LENGTH;
    }

    public void write(byte[] key) {
        int position = this.mappedByteBuffer.position();
        UNSAFE.copyMemory(key, 16, null, address + position, Constant.INDEX_LENGTH);
        this.mappedByteBuffer.position(position + Constant.INDEX_LENGTH);
    }

    private void insertIndexCache(long key, int value) {
        this.indexEntries[value] = new IndexEntry(key, value);
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
