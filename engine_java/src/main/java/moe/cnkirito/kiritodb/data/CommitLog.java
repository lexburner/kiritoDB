package moe.cnkirito.kiritodb.data;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import moe.cnkirito.directio.DirectIOLib;
import moe.cnkirito.directio.DirectIOUtils;
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
    private moe.cnkirito.directio.DirectRandomAccessFile directFileForRange;
    private ByteBuffer writeBuffer;
    private boolean dioSupport;
    private long addresses;
    private long wrotePosition;
    private int bufferPosition;

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
        if (DirectIOLib.binit) {
            directFileForRange = new moe.cnkirito.directio.DirectRandomAccessFile(file, "rw");
            this.writeBuffer = DirectIOUtils.allocateForDirectIO(Constant.directIOLib, Constant.VALUE_LENGTH * 4);
        } else {
            this.writeBuffer = ByteBuffer.allocateDirect(Constant.VALUE_LENGTH * 4);
        }

        this.addresses = ((DirectBuffer) this.writeBuffer).address();
        this.wrotePosition = 0;
        this.bufferPosition = 0;

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
        UNSAFE.copyMemory(data, 16, null, addresses + bufferPosition * Constant.VALUE_LENGTH, Constant.VALUE_LENGTH);
        bufferPosition++;
        if (bufferPosition >= 4) {
            this.writeBuffer.position(0);
            this.writeBuffer.limit(bufferPosition * Constant.VALUE_LENGTH);
            if (DirectIOLib.binit) {
                try {
                    this.directFileForRange.write(writeBuffer, this.wrotePosition);
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.IO_ERROR, "direct write data io error");
                }
            } else {
                try {
                    this.fileChannel.write(this.writeBuffer);
                } catch (IOException e) {
                    throw new EngineException(RetCodeEnum.IO_ERROR, "fileChannel write data io error");
                }
            }
            this.wrotePosition += Constant.VALUE_LENGTH * bufferPosition;
            bufferPosition = 0;
        }
    }

    /**
     * 加载整个data文件进入内存
     */
    public void loadAll(ByteBuffer buffer) throws IOException {
        buffer.clear();
        if (DirectIOLib.binit) {
            if (directRandomAccessFile.length() > 0) {
                directFileForRange.read(buffer, 0);
            }
            // no need flip
        } else {
            this.fileChannel.read(buffer, 0);
            buffer.flip();
        }
    }

    public int getFileLength() {
        try {
            return (int) (this.fileChannel.size() / Constant.VALUE_LENGTH);
        } catch (IOException e) {
            return 0;
        }
    }

    public void destroy() throws IOException {
        if (bufferPosition > 0) {
            this.writeBuffer.position(0);
            this.writeBuffer.limit(bufferPosition * Constant.VALUE_LENGTH);
            if (DirectIOLib.binit) {
                this.directFileForRange.write(writeBuffer, this.wrotePosition);
            } else {
                this.fileChannel.write(this.writeBuffer);
            }
            this.wrotePosition += Constant.VALUE_LENGTH * bufferPosition;
            bufferPosition = 0;
        }
        this.writeBuffer = null;
        if (this.fileChannel != null) {
            this.fileChannel.close();
        }
        if (this.directRandomAccessFile != null) {
            this.directRandomAccessFile.close();
        }
    }

}
