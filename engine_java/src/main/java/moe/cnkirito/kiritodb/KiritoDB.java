package moe.cnkirito.kiritodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    Logger logger = LoggerFactory.getLogger(KiritoDB.class);

    private CommitLog commitLog;
    private MemoryIndex memoryIndex;

    public void open(String path) {
        logger.info("open KiritoDB start...");
        commitLog = new CommitLog(path);
        memoryIndex = new MemoryIndex(path + "_index");
        logger.info("open KiritoDB end...");
    }

    public void write(byte[] key, byte[] value) {
        long offset = commitLog.write(value);
        memoryIndex.recordPosition(key, offset);
    }

    public byte[] read(byte[] key) {
        Long position = memoryIndex.getPosition(key);
        if (position == null)
            return null;
        return commitLog.read(position);
    }

    public void close() {
        if (commitLog != null) {
            commitLog.close();
        }
        if (memoryIndex != null) {
            memoryIndex.close();
        }
    }
}
