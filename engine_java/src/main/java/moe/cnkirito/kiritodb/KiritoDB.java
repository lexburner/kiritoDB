package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.File;
import java.io.IOException;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class KiritoDB {

    CommitLog commitLog;

    public void open(String path){
        commitLog = new CommitLog(path);
    }

    public void write(byte[] key, byte[] value){
        commitLog.write(key, value);
    }

    public byte[] read(byte[] key){
        return commitLog.read(key);
    }

    public void close() {
        commitLog.close();
    }
}
