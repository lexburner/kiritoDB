package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class CommitLog {

    private static Logger logger = LoggerFactory.getLogger(CommitLog.class);

    private FileChannel[] fileChannels = null;
    // 自增索引
    private AtomicLong[] atomicLongs = null;
    private final int bitOffset = 7;
    private final int fileNum = 2 << bitOffset;

    // buffer
    private ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.ValueLength));

    public void init(String path) throws IOException {
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            if (dirFile.mkdirs()) {
                logger.info("创建文件夹成功,dir=" + path);
            } else {
                logger.error("创建文件夹失败,dir=" + path);
            }
        }
        File[] files = new File[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            File file = new File(path + Constant.DataName + i + Constant.DataSuffix);
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    logger.error("创建文件失败,file=" + file.getPath());
                }
            }
            files[i] = file;
        }
        this.fileChannels = new FileChannel[fileNum];
        this.atomicLongs = new AtomicLong[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            FileChannel fileChannel = new RandomAccessFile(files[i], "rw").getChannel();
            this.fileChannels[i] = fileChannel;
            this.atomicLongs[i] = new AtomicLong(files[i].length());
        }
    }

    public void destroy() throws IOException {
        if (this.fileChannels != null) {
            for (FileChannel channel : fileChannels) {
                if (channel != null)
                    channel.force(true);
            }
        }
    }

    public byte[] read(long key, long offset, int size) throws IOException, EngineException {
        int index = getPartition(key);
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        FileChannel fileChannel = this.fileChannels[index];
        int read = fileChannel.read(buffer, offset);
        if (read != size) {
            logger.error(String.format("read=%d,size=%d", read, size));
            throw new EngineException(RetCodeEnum.IO_ERROR, "read != size");
        }
        buffer.flip();
        return buffer.array();
    }

    public long write(long key, byte[] data) throws IOException {
        int index = getPartition(key);
        AtomicLong atomicLong = atomicLongs[index];
        // 先获取自增的offset
        long curOffset = atomicLong.addAndGet(Constant.ValueLength);
        // 在offset位置写data
        write(key, curOffset, data);
        return curOffset;
    }

    public void write(long key, long offset, byte[] data) throws IOException {
        int index = getPartition(key);
        FileChannel fileChannel = this.fileChannels[index];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int size = data.length;
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer, offset + (size - buffer.remaining()));
        }
    }

    private int getPartition(long key){
        return (int) (Math.abs(key) % (63 - bitOffset));
    }
}
