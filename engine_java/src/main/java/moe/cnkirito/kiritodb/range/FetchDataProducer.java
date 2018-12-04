package moe.cnkirito.kiritodb.range;

import moe.cnkirito.kiritodb.KiritoDB;
import moe.cnkirito.kiritodb.common.Constant;
import moe.cnkirito.kiritodb.data.CommitLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

public class FetchDataProducer {

    public final static Logger logger = LoggerFactory.getLogger(FetchDataProducer.class);

    private int windowsNum;
    private ByteBuffer[] buffers;
    private ByteBuffer[] sliceBuffers;
    private Semaphore[] readSemaphores;
    private Semaphore[] writeSemaphores;
    private CommitLog[] commitLogs;

    public FetchDataProducer(KiritoDB kiritoDB) {
        int expectedNumPerPartition = kiritoDB.commitLogs[0].getFileLength();
        for (int i = 1; i < Constant.partitionNum; i++) {
            expectedNumPerPartition = Math.max(kiritoDB.commitLogs[i].getFileLength(), expectedNumPerPartition);
        }
        while (expectedNumPerPartition % 4 != 0) {
            expectedNumPerPartition++;
        }
        if (expectedNumPerPartition < 64000) {
            // 性能评测
            windowsNum = 4;
            buffers = new ByteBuffer[windowsNum];
            for (int i = 0; i < windowsNum; i++) {
                buffers[i] = ByteBuffer.allocate(expectedNumPerPartition * Constant.VALUE_LENGTH);
            }
        } else {
            // 正确性
            windowsNum = 1;
            sliceBuffers = new ByteBuffer[4];
            for (int i = 0; i < 4; i++) {
                sliceBuffers[i] = ByteBuffer.allocate(expectedNumPerPartition * Constant.VALUE_LENGTH / 4);
            }
        }

        readSemaphores = new Semaphore[windowsNum];
        writeSemaphores = new Semaphore[windowsNum];
        for (int i = 0; i < windowsNum; i++) {
            writeSemaphores[i] = new Semaphore(1);
            readSemaphores[i] = new Semaphore(0);
        }
        this.commitLogs = kiritoDB.commitLogs;
        logger.info("expectedNumPerPartition={}", expectedNumPerPartition);
    }

    public void startFetch() {
        for (int threadNo = 0; threadNo < windowsNum; threadNo++) {
            final int threadPartition = threadNo;
            new Thread(() -> {
                try {
                    for (int i = 0; i < Constant.partitionNum / windowsNum; i++) {
                        writeSemaphores[threadPartition].acquire();
                        if (commitLogs[i * windowsNum + threadPartition].getFileLength() > 0) {
                            if (windowsNum == 1) {
                                commitLogs[i * windowsNum + threadPartition].loadAll(sliceBuffers);
                            } else {
                                commitLogs[i * windowsNum + threadPartition].loadAll(buffers[threadPartition]);
                            }
                        }
                        readSemaphores[threadPartition].release();
                    }
                } catch (InterruptedException | IOException e) {
                    logger.error("threadNo{} load failed", threadPartition, e);
                }
            }).start();
        }
    }

    public ByteBuffer[] getBuffer(int partition) {
        try {
            readSemaphores[partition % windowsNum].acquire();
        } catch (InterruptedException e) {
            logger.error("threadNo{} getBuffer failed", partition, e);
        }
        if (windowsNum == 1) {
            return sliceBuffers;
        } else {
            return new ByteBuffer[]{buffers[partition % windowsNum]};
        }
    }

    public void release(int partition) {
        writeSemaphores[partition % windowsNum].release();
    }

    public void destroy() {
        if (buffers != null) {
            for (ByteBuffer buffer : buffers) {
                if (buffer instanceof DirectBuffer) {
                    ((DirectBuffer) buffer).cleaner().clean();
                }
            }
        }
    }

}
