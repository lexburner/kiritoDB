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
import java.nio.BufferOverflowException;
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
    private final int partitionNum = 1 << 10; //64
    // 用于获取 key 的分区
    private volatile Partitionable partitionable;
    private volatile CommitLog[] commitLogs;
    private volatile CommitLogIndex[] commitLogIndices;
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

    public byte[] read(byte[] key) throws EngineException {
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
    private volatile AtomicInteger consumerReadCompleteCnt = new AtomicInteger(0);
    private volatile AtomicBoolean canRead = new AtomicBoolean(false);
    private final Lock partitionLock = new ReentrantLock();
    private final Condition writeCondition = partitionLock.newCondition();
    private final Condition readCondition = partitionLock.newCondition();
    private volatile ByteBuffer buffer;
    private static ThreadLocal<byte[]> visitorCallbackValue = ThreadLocal.withInitial(() -> new byte[Constant.VALUE_LENGTH]);

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        if (producerFlag.compareAndSet(false, true)) {
            new Thread(() -> {
                FetchDataProducer fetchDataProducer = new FetchDataProducer();
                for (int i = 0; i < partitionNum; i++) {
                    partitionLock.lock();
                    try {
                        canRead.set(false);
                        while (consumerReadCompleteCnt.get() >0) {
                            writeCondition.await();
                        }
                        fetchDataProducer.resetPartition(commitLogs[i]);
                        buffer = fetchDataProducer.produce();
                        canRead.set(true);
                        readCondition.signalAll();
                    } catch (InterruptedException e) {
                        logger.error("writeCondition.await() interrupted", e);
                    } finally {
                        partitionLock.unlock();
                    }
                }
            }).start();
        }
        for (int i = 0; i < partitionNum; i++) {
            partitionLock.lock();
            try {
                while (!canRead.get()) {
                    readCondition.await();
                }
                consumerReadCompleteCnt.incrementAndGet();
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
                        logger.error("size={},offset={}", size, offsetInts[j] * Constant.VALUE_LENGTH, e);
                    }

                    visitor.visit(Util.long2bytes(keys[j]), bytes);
                }
                consumerReadCompleteCnt.decrementAndGet();
                writeCondition.signalAll();
            } catch (InterruptedException e) {
                logger.error("readCondition.await() interrupted", e);
            } finally {
                partitionLock.unlock();
            }
        }
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
