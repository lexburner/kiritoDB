package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class CommitLog {

    private static Logger logger = LoggerFactory.getLogger(CommitLog.class);

    private FileChannel[] fileChannels = null;
    // 自增索引
    private AtomicLong[] fileLength = null;
    private final int bitOffset = 7;
    private final int fileNum = 2 << bitOffset;//256

    // buffer
    private ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.ValueLength));

    public void init(String path) throws IOException {
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File[] files = new File[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            File file = new File(path + Constant.DataName + i + Constant.DataSuffix);
            if (!file.exists()) {
                file.createNewFile();
            }
            files[i] = file;
        }
        this.fileChannels = new FileChannel[fileNum];
        this.fileLength = new AtomicLong[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            FileChannel fileChannel = new RandomAccessFile(files[i], "rw").getChannel();
            this.fileChannels[i] = fileChannel;
            this.fileLength[i] = new AtomicLong(files[i].length());
        }
    }

    public void destroy() throws IOException {
        if (this.fileChannels != null) {
            for (FileChannel channel : fileChannels) {
                channel.close();
            }
        }
    }

    public byte[] read(byte[] key, long offset, int size) throws IOException, EngineException {
        int index = getPartition(key);
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        FileChannel fileChannel = this.fileChannels[index];
        int read = fileChannel.read(buffer, offset);
        if (read != size) {
            throw Constant.ioException;
        }
        return buffer.array();
    }

    public long write(byte[] key, byte[] data) throws IOException {
        int index = getPartition(key);
        AtomicLong atomicLong = fileLength[index];
        // 先获取自增的offset
        long curOffset = atomicLong.addAndGet(Constant.ValueLength);
        // 在offset位置写data
        write(key, curOffset, data);
        return curOffset;
    }

    public void write(byte[] key, long offset, byte[] data) throws IOException {
        int index = getPartition(key);
        FileChannel fileChannel = this.fileChannels[index];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        fileChannel.write(buffer, offset);
    }

    public long getFileLength(int no){
        return this.fileLength[no].get();
    }

    private int getPartition(byte[] key) {
        return key[0] & 0xff;
    }
}
