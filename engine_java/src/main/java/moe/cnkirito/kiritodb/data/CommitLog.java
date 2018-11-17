package moe.cnkirito.kiritodb.data;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import moe.cnkirito.kiritodb.common.Constant;
import net.smacke.jaydio.DirectRandomAccessFile;
import sun.misc.Contended;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static moe.cnkirito.kiritodb.common.UnsafeUtil.UNSAFE;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
@Contended
public class CommitLog {

    // buffer
    public static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.ValueLength));
    public static ThreadLocal<byte[]> byteArrayThreadLocal = ThreadLocal.withInitial(() -> new byte[Constant.ValueLength]);

    private FileChannel fileChannel;
    private DirectRandomAccessFile directRandomAccessFile;
    // 逻辑长度 要乘以 4096
    private int fileLength;
    private ByteBuffer writeBuffer;
    private int bufferSize = 0;
    private long addresses;
    private boolean dioSupport;

    public void init(String path, int no) throws IOException {
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File file = new File(path + Constant.DataName + no + Constant.DataSuffix);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        try {
            this.directRandomAccessFile = new DirectRandomAccessFile(file, "r");
            this.dioSupport = true;
        } catch (Exception e) {
            this.dioSupport = false;
        }
        this.fileLength = (int) (this.fileChannel.size() / Constant.ValueLength);
        this.writeBuffer = ByteBuffer.allocateDirect(Constant.ValueLength * 4);
        this.addresses = ((DirectBuffer) this.writeBuffer).address();
    }

    public void destroy() throws IOException {
        if (bufferSize != 0) {
            this.writeBuffer.position(0);
            this.writeBuffer.limit(bufferSize * Constant.ValueLength);
            try {
                this.fileChannel.write(this.writeBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        this.writeBuffer = null;
        if (this.fileChannel != null) {
            this.fileChannel.close();
        }
        if (this.directRandomAccessFile != null) {
            this.directRandomAccessFile.close();
        }
    }

    public synchronized byte[] read(long offset) throws IOException {
//        if (this.dioSupport) {
        byte[] bytes = byteArrayThreadLocal.get();
        directRandomAccessFile.seek(offset);
        directRandomAccessFile.read(bytes);
        return bytes;
//        } else {
//            ByteBuffer buffer = bufferThreadLocal.get();
//            buffer.clear();
//            this.fileChannel.read(buffer, offset);
//            return buffer.array();
//        }
    }

    public synchronized void write(byte[] data) throws EngineException {
        UNSAFE.copyMemory(data, 16, null, addresses + Constant.ValueLength * bufferSize, 4096);
        bufferSize++;
        if (bufferSize >= 4) {
            this.writeBuffer.position(0);
            try {
                this.fileChannel.write(this.writeBuffer);
            } catch (IOException e) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "write data io error");
            }
            bufferSize = 0;
        }
    }

    public int getFileLength() {
        return this.fileLength;
    }

}
