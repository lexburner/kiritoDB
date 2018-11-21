package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.data.CommitLog;
import moe.cnkirito.kiritodb.data.CommitLogAware;
import moe.cnkirito.kiritodb.index.CommitLogIndex;
import moe.cnkirito.kiritodb.partition.FirstBytePartitoner;
import moe.cnkirito.kiritodb.partition.Partitionable;
import moe.cnkirito.kiritodb.range.FetchDataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    private static final Logger logger = LoggerFactory.getLogger(KiritoDB.class);
    private final int partitionNum = 1 << 8; //64
    // 用于获取 key 的分区
    private volatile Partitionable partitionable;
    private volatile CommitLog[] commitLogs;
    private volatile CommitLogIndex[] commitLogIndices;
    private Lock[] partitionLocks;
    private Condition[] readDiskConditions;
    private Condition[] readCacheConditions;
    // 判断是否需要加载索引进入内存
    private volatile boolean loadFlag = false;

    public KiritoDB() {
        partitionable = new FirstBytePartitoner();
    }

    public void open(String path) throws EngineException {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        commitLogs = new CommitLog[partitionNum];
        commitLogIndices = new CommitLogIndex[partitionNum];
        partitionLocks = new Lock[partitionNum];
        readDiskConditions = new Condition[partitionNum];
        readCacheConditions = new Condition[partitionNum];
        try {
            for (int i = 0; i < partitionNum; i++) {
                commitLogs[i] = new CommitLog();
                commitLogs[i].init(path, i);
            }
            for (int i = 0; i < partitionNum; i++) {
                commitLogIndices[i] = new CommitLogIndex();
                commitLogIndices[i].init(path, i);
                if (commitLogIndices[i] instanceof CommitLogAware) {
                    ((CommitLogAware) commitLogIndices[i]).setCommitLog(commitLogs[i]);
                }
                this.loadFlag = commitLogIndices[i].isLoadFlag();
            }
            if (!loadFlag) {
                loadAllIndex();
            }
            // for range
            for (int i = 0; i < partitionNum; i++) {
                partitionLocks[i] = new ReentrantLock();
                readDiskConditions[i] = partitionLocks[i].newCondition();
                readCacheConditions[i] = partitionLocks[i].newCondition();
            }
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "open exception");
        }
    }

    public void write(byte[] key, byte[] value) throws EngineException {
        int partition = partitionable.getPartition(key);
        CommitLog hitCommitLog = commitLogs[partition];
        CommitLogIndex hitIndex = commitLogIndices[partition];
        synchronized (hitCommitLog) {
            hitCommitLog.write(value);
            hitIndex.write(key);
        }
    }

    private AtomicBoolean readFirst = new AtomicBoolean(false);

    public byte[] read(byte[] key) throws EngineException {
        if (readFirst.compareAndSet(false, true)) {
            logger.info("[read info] loadFlag={}", loadFlag);
            for (int i = 0; i < partitionNum; i++) {
                logger.info("[read info] partition[{}],commitLogLength[{}],indexSize[{}]", i, commitLogs[i].getFileLength(), commitLogIndices[i].getMemoryIndex().getSize());
            }
        }
        int partition = partitionable.getPartition(key);
        CommitLog hitCommitLog = commitLogs[partition];
        CommitLogIndex hitIndex = commitLogIndices[partition];
        Long offset = hitIndex.read(key);
        if (offset == null) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, Util.bytes2Long(key) + " not found");
        }
        try {
            return hitCommitLog.read(offset);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "commit log read exception");
        }
    }

    // 开启 fetch 线程的标记
    private final AtomicBoolean producerFlag = new AtomicBoolean(false);
    // 读完当前分区的线程计数器
    private volatile AtomicInteger readingCacheCnt = new AtomicInteger(0);
    private volatile AtomicBoolean fetchMode = new AtomicBoolean(true);
    private volatile ByteBuffer buffer;
    private static ThreadLocal<byte[]> visitorCallbackValue = ThreadLocal.withInitial(() -> new byte[Constant.VALUE_LENGTH]);
    private AtomicInteger threadNum;

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        threadNum.incrementAndGet();
        // 第一次 range 的时候开启 fetch 线程
        if (producerFlag.compareAndSet(false, true)) {
            initPreFetchThreads();
        }
        for (int i = 0; i < partitionNum; i++) {
            try {
                partitionLocks[i].lock();
                try{
                    readDiskConditions[i].await();
                }finally {
                    partitionLocks[i].unlock();
                }
                System.out.println("read partition" + i + " from cache");
                CommitLogIndex commitLogIndex = this.commitLogIndices[i];
                int size = commitLogIndex.getMemoryIndex().getSize();
                int[] offsetInts = commitLogIndex.getMemoryIndex().getOffsetInts();
                long[] keys = commitLogIndex.getMemoryIndex().getKeys();
                for (int j = 0; j < size; j++) {
                    byte[] bytes = visitorCallbackValue.get();
                    try {
                        ByteBuffer slice = buffer.slice();
                        slice.position(offsetInts[j] * Constant.VALUE_LENGTH);
                        slice.get(bytes);
                    } catch (IndexOutOfBoundsException | BufferUnderflowException | IllegalArgumentException e) {
                        logger.error("[partition {} range error] size={},offset={},buffer limit={}", i, size, offsetInts[j] * Constant.VALUE_LENGTH, buffer.limit(), e);
                    }
                    visitor.visit(Util.long2bytes(keys[j]), bytes);
                }
                if(readingCacheCnt.get()==64){
                    fetchMode.set(true);
                    writeCondition.signal();
                }
            } catch (InterruptedException e) {
                logger.error("readCondition.await() interrupted", e);
            } finally {
                partitionLock.unlock();
            }
        }
    }

    /**
     * 初始化preFetch线程
     */
    private void initPreFetchThreads() {
        try {
            logger.info("[range info]loadFlag={}", loadFlag);
            for (int i = 0; i < partitionNum; i++) {
                logger.info("[range info] partition[{}],commitLogLength[{}],indexSize[{}]", i, commitLogs[i].getFileLength(), commitLogIndices[i].getMemoryIndex().getSize());
            }
        } catch (Exception e) {
            logger.error("print error", e);
        }
        new Thread(() -> {
            FetchDataProducer fetchDataProducer = new FetchDataProducer();
            for (int i = 0; i < partitionNum; i++) {
                partitionLocks[i].lock();
                try {
                    while (!fetchMode.get()) {
                        writeCondition.await();
                    }
                    try {
                        logger.info("[range info] read partition {}, current partition has {} value.", i, commitLogs[i].getFileLength());
                    } catch (Exception e) {
                        logger.error("获取失败", e);
                    }
                    fetchDataProducer.resetPartition(commitLogs[i]);
                    buffer = fetchDataProducer.produce();
                    logger.info("[range info] read partition {} success. buffer limit = {}", i, buffer.limit());
                    System.out.println("read partition" + i + " from disk");
                    readingCacheCnt.set(0);
                    for (int k = 0; k < 64; k++) {
                        readCondition.signal();
                    }
                } catch (InterruptedException e) {
                    logger.error("writeCondition.await() interrupted", e);
                } finally {
                    partitionLock.unlock();
                }
            }
        }).start();
    }

    private void loadAllIndex() {
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch countDownLatch = new CountDownLatch(partitionNum);
        for (int i = 0; i < partitionNum; i++) {
            final int index = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    commitLogIndices[index].load();
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("load index interrupted", e);
        }
        executorService.shutdown();
        this.loadFlag = true;
    }

    public void close() {
        if (commitLogs != null) {
            for (CommitLog commitLog : commitLogs) {
                try {
                    commitLog.destroy();
                } catch (IOException e) {
                    logger.error("data destory error", e);
                }
            }
        }
        if (commitLogIndices != null) {
            for (CommitLogIndex commitLogIndex : commitLogIndices) {
                try {
                    commitLogIndex.destroy();
                } catch (IOException e) {
                    logger.error("data destory error", e);
                }
            }
        }
        this.partitionable = null;
        this.commitLogs = null;
        this.commitLogIndices = null;
    }
}
