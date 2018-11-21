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
    public static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.VALUE_LENGTH));
    public static ThreadLocal<byte[]> byteArrayThreadLocal = ThreadLocal.withInitial(() -> new byte[Constant.VALUE_LENGTH]);
    private FileChannel fileChannel;
    private DirectRandomAccessFile directRandomAccessFile;
    private ByteBuffer writeBuffer;
    private int curBufferSize = 0;
    private boolean dioSupport;
    private long addresses;

    public void init(String path, int no) throws IOException {
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        File file = new File(path + Constant.DATA_PREFIX + no + Constant.DATA_SUFFIX);
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
        this.writeBuffer = ByteBuffer.allocateDirect(Constant.VALUE_LENGTH);
        this.addresses = ((DirectBuffer) this.writeBuffer).address();
    }

    public void destroy() throws IOException {
        this.writeBuffer = null;
        if (this.fileChannel != null) {
            this.fileChannel.close();
        }
        if (this.directRandomAccessFile != null) {
            this.directRandomAccessFile.close();
        }
    }

    public synchronized byte[] read(long offset) throws IOException {
        if (this.dioSupport) {
            byte[] bytes = byteArrayThreadLocal.get();
            directRandomAccessFile.seek(offset);
            directRandomAccessFile.read(bytes);
            return bytes;
        } else {
            ByteBuffer buffer = bufferThreadLocal.get();
            buffer.clear();
            this.fileChannel.read(buffer, offset);
            return buffer.array();
        }
    }

    public synchronized void write(byte[] data) throws EngineException {
        UNSAFE.copyMemory(data, 16, null, addresses, Constant.VALUE_LENGTH);
        this.writeBuffer.position(0);
        try {
            this.fileChannel.write(this.writeBuffer);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "write data io error");
        }
    }

    /**
     * 加载整个data文件进入内存
     */
    public void loadAll(ByteBuffer buffer) throws IOException {
        buffer.clear();
        this.fileChannel.read(buffer,0);
        buffer.flip();
    }

    public int getFileLength() {
        try {
            return (int) (this.fileChannel.size() / Constant.VALUE_LENGTH);
        } catch (IOException e) {
            return 0;
        }
    }

}
