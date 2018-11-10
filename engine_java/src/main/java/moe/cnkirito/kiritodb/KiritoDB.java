package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    Logger logger = LoggerFactory.getLogger(KiritoDB.class);

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
            logger.error("初始化文件错误", e);
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
    }

    public void write(byte[] key, byte[] value) throws EngineException {
        long kI = Util.bytes2Long(key);
//        Long offset = memoryIndex.read(kI);
//        if (offset != null) {
//            // 已经写过的key，直接写offset的value就好
//            try {
//                commitLog.write(kI, offset, value);
//            } catch (IOException e) {
//                logger.error("io error", e);
//                throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
//            }
//        } else {
        try {
            // append value
            Long offset = commitLog.write(kI, value);
            memoryIndex.write(kI, offset);
        } catch (IOException e) {
            logger.error("io2 error", e);
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
//        }
    }

    public byte[] read(byte[] key) throws EngineException {
//        if(!memoryIndex.isLoadFlag()){
//            synchronized (this){
//                if(!memoryIndex.isLoadFlag()){
//                    memoryIndex.load();
//                }
//            }
//        }
        long kI = Util.bytes2Long(key);
        Long offset = memoryIndex.read(kI);
        if (offset == null) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, Util.bytes2Long(key) + "不存在");
        }
        try {
            return commitLog.read(kI, offset, Constant.ValueLength);
        } catch (IOException e) {
            logger.error("io3 error", e);
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
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
        memoryIndex = null;
        commitLog = null;
    }
}
