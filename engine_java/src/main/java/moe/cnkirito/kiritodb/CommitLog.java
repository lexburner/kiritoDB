package moe.cnkirito.kiritodb;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import static moe.cnkirito.kiritodb.UnsafeUtil.UNSAFE;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class CommitLog {

    private static Logger logger = LoggerFactory.getLogger(CommitLog.class);

    private FileChannel[] fileChannels = null;
    // 逻辑长度 要乘以 4096
    private int[] fileLength = null;
    private final int bitOffset = 7;
    private final int fileNum = 2 << bitOffset;//256

    // buffer
    private ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.ValueLength));
    private ByteBuffer[] writeBuffer;
    private long[] addresses;

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
        this.fileLength = new int[fileNum];
        this.writeBuffer = new ByteBuffer[fileNum];
        this.addresses = new long[fileNum];
        for (int i = 0; i < fileNum; ++i) {
            FileChannel fileChannel = new RandomAccessFile(files[i], "rw").getChannel();
            this.fileChannels[i] = fileChannel;
            this.fileLength[i] = (int) (files[i].length() / Constant.ValueLength);
            this.writeBuffer[i] = ByteBuffer.allocateDirect(Constant.ValueLength);
            this.addresses[i] = ((DirectBuffer) this.writeBuffer[i]).address();
        }
    }

    public void destroy() throws IOException {
        if (this.fileChannels != null) {
            for (FileChannel channel : fileChannels) {
                channel.close();
            }
        }
    }

    public byte[] read(byte[] key, long offset) throws IOException, EngineException {
        int index = getPartition(key);
        ByteBuffer buffer = bufferThreadLocal.get();
        buffer.clear();
        FileChannel fileChannel = this.fileChannels[index];
        fileChannel.read(buffer, offset);
        return buffer.array();
    }

    public int write(byte[] key, byte[] data) throws IOException {
        int index = getPartition(key);
        FileChannel fileChannel = this.fileChannels[index];
        int offsetInt;
        synchronized (fileChannel) {
            offsetInt = fileLength[index] ++;
            UNSAFE.copyMemory(data, 16, null, addresses[index], 4096);
            this.writeBuffer[index].position(0);
            try {
                fileChannel.write(this.writeBuffer[index]);
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        return offsetInt;
    }

    public int getFileLength(int no) {
        return this.fileLength[no];
    }

    private int getPartition(byte[] key) {
        return key[0] & 0xff;
    }
}
