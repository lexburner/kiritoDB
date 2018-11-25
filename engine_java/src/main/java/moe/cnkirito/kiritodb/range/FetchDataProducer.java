package moe.cnkirito.kiritodb.range;

import moe.cnkirito.kiritodb.KiritoDB;
import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.data.CommitLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FetchDataProducer {

    public final static Logger logger = LoggerFactory.getLogger(FetchDataProducer.class);

    private volatile CommitLog curCommitLog;
    private volatile ByteBuffer buffer;

    public FetchDataProducer(KiritoDB kiritoDB) {
        int expectedNumPerPartition = kiritoDB.commitLogs[0].getFileLength() * Constant.VALUE_LENGTH;
        for (int i = 1; i < Constant.partitionNum; i++) {
            expectedNumPerPartition = Math.max(kiritoDB.commitLogs[i].getFileLength() * Constant.VALUE_LENGTH, expectedNumPerPartition);
        }
        this.buffer = ByteBuffer.allocateDirect(expectedNumPerPartition);
        logger.info("expectedNumPerPartition={}",expectedNumPerPartition);
    }

    public void resetPartition(CommitLog commitLog) {
        this.curCommitLog = commitLog;
    }

    public ByteBuffer produce() {
        try {
            curCommitLog.loadAll(this.buffer);
            return this.buffer;
        } catch (IOException e) {
            logger.error("loadWithMmap buffer error", e);
            return null;
        }
    }

}
