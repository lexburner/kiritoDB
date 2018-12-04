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
//    public static ThreadLocal<ByteBuffer> myByteArrayThreadLocal = ThreadLocal.withInitial(() -> DirectIOUtils.allocateForDirectIO(DirectIOLib.getLibForPath("test_directory"), Constant.VALUE_LENGTH));

    private File file;
    private FileChannel fileChannel;
    private DirectRandomAccessFile directRandomAccessFile;
    //    private moe.cnkirito.directio.DirectRandomAccessFile myDirectRandomAccessFile;
    private ByteBuffer writeBuffer;
    private boolean dioSupport;
    private long addresses;

    public void init(String path, int no) throws IOException {
        File dirFile = new File(path);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
        this.file = new File(path + Constant.DATA_PREFIX + no + Constant.DATA_SUFFIX);
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
//        if(DirectIOLib.binit){
//            myDirectRandomAccessFile = new moe.cnkirito.directio.DirectRandomAccessFile(file,"rw");
//        }
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
//            ByteBuffer byteBuffer = myByteArrayThreadLocal.get();
//            myDirectRandomAccessFile.read(byteBuffer, offset);
//            byte[] bytes = byteArrayThreadLocal.get();
//            byteBuffer.get(bytes);
//            return bytes;
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
        if (dioSupport) {
            buffer.clear();
            directRandomAccessFile.seek(0);
            directRandomAccessFile.read(buffer.array());
            buffer.position(0);
            buffer.limit(buffer.capacity());
        } else {
            buffer.clear();
            this.fileChannel.read(buffer, 0);
            buffer.flip();
        }
    }

    public void loadAll(ByteBuffer[] buffer) throws IOException {
        if (dioSupport) {
            long position = 0;
            for (int i = 0; i < buffer.length; i++) {
                buffer[i].clear();
                directRandomAccessFile.seek(position);
                directRandomAccessFile.read(buffer[i].array());
                buffer[i].position(0);
                buffer[i].limit(buffer[i].capacity());
                position += buffer[i].capacity();
                if(position>directRandomAccessFile.length()){
                    return;
                }
            }
        } else {
            this.fileChannel.position(0);
            for (int i = 0; i < buffer.length; i++) {
                buffer[i].clear();
                this.fileChannel.read(buffer[i]);
                buffer[i].flip();
            }
        }
    }

    public int getFileLength() {
        try {
            return (int) (this.fileChannel.size() / Constant.VALUE_LENGTH);
        } catch (IOException e) {
            return 0;
        }
    }

}
