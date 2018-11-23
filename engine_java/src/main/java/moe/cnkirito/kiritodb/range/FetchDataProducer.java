package moe.cnkirito.kiritodb.range;

import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.data.CommitLog;
import moe.cnkirito.kiritodb.index.CommitLogIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FetchDataProducer {

    public final static Logger logger = LoggerFactory.getLogger(FetchDataProducer.class);

    private volatile CommitLog curCommitLog;
    private volatile ByteBuffer buffer;

    public FetchDataProducer() {
        int expectedNumPerPartition = CommitLogIndex.expectedNumPerPartition * Constant.VALUE_LENGTH;
        this.buffer = ByteBuffer.allocateDirect(expectedNumPerPartition);
    }

    public void resetPartition(CommitLog commitLog) {
        this.curCommitLog = commitLog;
    }

    public ByteBuffer produce() {
        try {
            curCommitLog.loadAll(this.buffer);
            return this.buffer;
        } catch (IOException e) {
            logger.error("load buffer error",e);
            return null;
        }
    }

}
