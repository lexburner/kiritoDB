package moe.cnkirito.kiritodb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

    final String path;
    private FileChannel fileChannel;
    private AtomicLong wrotePosition;

    static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(()-> ByteBuffer.allocate(4*1024));

    private MemoryIndex memoryIndex;


    public CommitLog(String path) {
        this.path = path;
        File file = new File(path);
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.wrotePosition = new AtomicLong(fileChannel.position());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.memoryIndex = new MemoryIndex(path+"_index");
    }

    public Long write(byte[] key,byte[] value){
        long position = wrotePosition.getAndAdd(value.length);
        try {
            this.fileChannel.write(ByteBuffer.wrap(value), position);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        this.memoryIndex.recordPosition(key, position);
        return position;
    }

    public byte[] read(byte[] key){
        Long position = this.memoryIndex.getPosition(key);
        if(position==null) return null;
        ByteBuffer readBuffer = bufferThreadLocal.get();
        readBuffer.clear();
        try {
            fileChannel.read(readBuffer, position);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return readBuffer.array();
    }


    public void close() {
        memoryIndex.close();
    }
}
