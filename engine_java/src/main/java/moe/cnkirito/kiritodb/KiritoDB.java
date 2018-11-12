package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    private static final Logger logger = LoggerFactory.getLogger(KiritoDB.class);

    private CommitLog commitLog;
    private MemoryIndex memoryIndex;

    public void open(String path) throws EngineException {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        commitLog = new CommitLog();
        memoryIndex = new MemoryIndex();
        try {
            commitLog.init(path);
            memoryIndex.init(path);
        } catch (IOException e) {
            throw Constant.ioException;
        }
    }

    public void write(byte[] key, byte[] value) throws EngineException {
        try {
            Long offset = commitLog.write(key, value);
            memoryIndex.write(key, offset);
        } catch (IOException e) {
            throw Constant.ioException;
        }
    }

    public byte[] read(byte[] key) throws EngineException {
        Long offset = memoryIndex.read(key);
        if (offset == null) {
            throw Constant.keyNotFoundException;
        }
        try {
            return commitLog.read(key, offset, Constant.ValueLength);
        } catch (IOException e) {
            throw Constant.ioException;
        }
    }

    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    public void close() {
        if (memoryIndex != null) {
            try {
                memoryIndex.destroy();
            } catch (IOException e) {
                logger.error("index destory error", e);
            }
        }
        if (commitLog != null) {
            try {
                commitLog.destroy();
            } catch (IOException e) {
                logger.error("data destory error", e);
            }
        }
    }
}
