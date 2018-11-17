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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    private static final Logger logger = LoggerFactory.getLogger(KiritoDB.class);
    private final int commitLogNum = 1 << 6; //64
    private final int indexNum = 1 << 6; //64
    // 用于获取 key 的分区
    private Partitionable partitionable;
    private CommitLog[] commitLogs;
    private CommitLogIndex[] commitLogIndices;
    // 判断是否需要加载索引进入内存
    private boolean loadFlag = false;

    public KiritoDB() {
        partitionable = new FirstBytePartitoner();
    }

    public void open(String path) throws EngineException {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        commitLogs = new CommitLog[commitLogNum];
        commitLogIndices = new CommitLogIndex[indexNum];
        try {
            for (int i = 0; i < commitLogNum; i++) {
                commitLogs[i] = new CommitLog();
                commitLogs[i].init(path, i);
            }
            for (int i = 0; i < indexNum; i++) {
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
            throw Constant.ioException;
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

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    private void loadAllIndex() {
        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch countDownLatch = new CountDownLatch(indexNum);
        for (int i = 0; i < indexNum; i++) {
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
