package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.common.Util;
import moe.cnkirito.kiritodb.data.CommitLog;
import moe.cnkirito.kiritodb.index.CommitLogIndex;
import moe.cnkirito.kiritodb.partition.HighTenPartitioner;
import moe.cnkirito.kiritodb.partition.Partitionable;
import moe.cnkirito.kiritodb.range.CacheItem;
import moe.cnkirito.kiritodb.range.FetchDataProducer;
import moe.cnkirito.kiritodb.range.RangeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    private static final Logger logger = LoggerFactory.getLogger(KiritoDB.class);
    // partition num
    private final int partitionNum = Constant.partitionNum;
    // key -> partition
    private volatile Partitionable partitionable;
    // data
    public volatile CommitLog[] commitLogs;
    // index
    private volatile CommitLogIndex[] commitLogIndices;
    // true means need to load index into memory, false means no need
    private volatile boolean loadFlag = false;

    public KiritoDB() {
        partitionable = new HighTenPartitioner();
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
                commitLogIndices[i].setCommitLog(commitLogs[i]);
                this.loadFlag = commitLogIndices[i].isLoadFlag();
            }
            if (!loadFlag) {
                loadAllIndex();
            }
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "open exception");
        }
    }

//    private AtomicBoolean writeFirst = new AtomicBoolean(false);

    public void write(byte[] key, byte[] value) throws EngineException {
//        if (writeFirst.compareAndSet(false, true)) {
//            logger.info("[jvm info] write first, now {} ", Util.getFreeMemory());
//        }
        int partition = partitionable.getPartition(key);
        CommitLog hitCommitLog = commitLogs[partition];
        CommitLogIndex hitIndex = commitLogIndices[partition];
        synchronized (hitCommitLog) {
            hitCommitLog.write(value);
            hitIndex.write(key);
        }
    }

//    private AtomicBoolean readFirst = new AtomicBoolean(false);

    public byte[] read(byte[] key) throws EngineException {
//        if (readFirst.compareAndSet(false, true)) {
//            logger.info("[jvm info] read first now {} ", Util.getFreeMemory());
//        }
        int partition = partitionable.getPartition(key);
        CommitLog hitCommitLog = commitLogs[partition];
        CommitLogIndex hitIndex = commitLogIndices[partition];
        Long offset = hitIndex.read(key);
        if (offset == null) {
//            throw new EngineException(RetCodeEnum.NOT_FOUND, Util.bytes2Long(key) + " not found");
            throw new EngineException(RetCodeEnum.NOT_FOUND, "");
        }
        try {
            return hitCommitLog.read(offset);
        } catch (IOException e) {
//            throw new EngineException(RetCodeEnum.IO_ERROR, "commit log read exception");
            throw new EngineException(RetCodeEnum.IO_ERROR, "");
        }
    }

    // fetch thread flag
    private final AtomicBoolean rangFirst = new AtomicBoolean(false);
    private static ThreadLocal<byte[]> visitorCallbackValue = ThreadLocal.withInitial(() -> new byte[Constant.VALUE_LENGTH]);
    private static ThreadLocal<byte[]> visitorCallbackKey = ThreadLocal.withInitial(() -> new byte[Constant.INDEX_LENGTH]);
    private final static int THREAD_NUM = 64;
    private LinkedBlockingQueue<RangeTask> rangeTaskLinkedBlockingQueue = new LinkedBlockingQueue<>();

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        // 第一次 range 的时候开启 fetch 线程
        if (rangFirst.compareAndSet(false, true)) {
//            logger.info("[jvm info] range first now {} ", Util.getFreeMemory());
            initPreFetchThreads();
        }
        RangeTask rangeTask = new RangeTask(visitor, new CountDownLatch(1));
        rangeTaskLinkedBlockingQueue.offer(rangeTask);
        try {
            rangeTask.getCountDownLatch().await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private volatile FetchDataProducer fetchDataProducer;

    private void initPreFetchThreads() {
        Thread fetchThread = new Thread(() -> {
            fetchDataProducer = new FetchDataProducer(this);
            long gapTime = System.currentTimeMillis();
            for (int f = 0; f < 2; f++) {
                final int turn = f;
                RangeTask[] rangeTasks = new RangeTask[THREAD_NUM];
                for (int i = 0; i < THREAD_NUM; i++) {
                    try {
                        rangeTasks[i] = rangeTaskLinkedBlockingQueue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                logger.info("gap time = {}", System.currentTimeMillis() - gapTime);
                fetchDataProducer.init();
                fetchDataProducer.startFetch();
                for (int i = 0; i < THREAD_NUM; i++) {
                    final int rangeIndex = i;
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            RangeTask myTask = rangeTasks[rangeIndex];
                            for (int dbIndex = 0; dbIndex < partitionNum; dbIndex++) {
                                CacheItem cacheItem;
                                while (true) {
                                    cacheItem = fetchDataProducer.getCacheItem(dbIndex);
                                    if (cacheItem != null) {
                                        break;
                                    }
                                    sleep1us();
                                }
                                while (true) {
                                    if (cacheItem.ready) {
                                        break;
                                    }
                                    sleep1us();
                                }
                                byte[] value = visitorCallbackValue.get();
                                byte[] key = visitorCallbackKey.get();
                                ByteBuffer slice = cacheItem.buffer.slice();
                                int keySize = commitLogIndices[dbIndex].getMemoryIndex().getSize();
                                int[] coffsetInts = commitLogIndices[dbIndex].getMemoryIndex().getOffsetInts();
                                long[] keys = commitLogIndices[dbIndex].getMemoryIndex().getKeys();
                                for (int j = 0; j < keySize; j++) {
                                    slice.position(coffsetInts[j] * Constant.VALUE_LENGTH);
                                    slice.get(value);
                                    Util.long2bytes(key, keys[j]);
                                    rangeTasks[rangeIndex].getAbstractVisitor().visit(key, value);
                                }
                                while (true) {
                                    if (cacheItem.allReach) {
                                        break;
                                    }
                                    sleep1us();
                                }
                                fetchDataProducer.release(dbIndex);
                            }
                            myTask.getCountDownLatch().countDown();
                        }
                    });
                    thread.setDaemon(true);
                    thread.start();
                }
            }
        });
        fetchThread.setDaemon(true);
        fetchThread.start();
    }

    private void sleep1us() {
        try {
            Thread.sleep(0, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void loadAllIndex() {
        int loadThreadNum = THREAD_NUM;
        CountDownLatch countDownLatch = new CountDownLatch(loadThreadNum);
        for (int i = 0; i < loadThreadNum; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int partition = 0; partition < partitionNum; partition++) {
                        if (partition % loadThreadNum == index) {
                            commitLogIndices[partition].load();
                        }
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("load index interrupted", e);
        }
        this.loadFlag = true;
    }

    public void close() {
        if (commitLogs != null) {
            for (CommitLog commitLog : commitLogs) {
                try {
                    commitLog.destroy();
                } catch (IOException e) {
                    logger.error("data destroy error", e);
                }
            }
        }
        if (commitLogIndices != null) {
            for (CommitLogIndex commitLogIndex : commitLogIndices) {
                try {
                    commitLogIndex.destroy();
                } catch (IOException e) {
                    logger.error("data destroy error", e);
                }
            }
        }
        if (this.fetchDataProducer != null) {
            fetchDataProducer.destroy();
        }
        this.partitionable = null;
        this.commitLogs = null;
        this.commitLogIndices = null;
    }
}
