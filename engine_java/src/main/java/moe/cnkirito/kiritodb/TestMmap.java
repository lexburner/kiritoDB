package moe.cnkirito.kiritodb;


import com.carrotsearch.hppc.LongLongHashMap;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class TestMmap {

    @Test
    public void test1() throws Exception {
        File file = new File("/tmp/mmap");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");
        MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024L * 1024L);
//        mappedByteBuffer.put("12345678".getBytes());
//        mappedByteBuffer.putLong(1L);
//        mappedByteBuffer.position(0);
        byte[] content = new byte[8];
        mappedByteBuffer.get(content);
        System.out.println(new String(content));
    }


    public void test2() throws Exception {
        File file = new File("/tmp/kiritoDB_index");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");
        MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024L * 1024L);
        byte[] content = new byte[8];
        mappedByteBuffer.get(content);
        long aLong = mappedByteBuffer.getLong();
        System.out.println(new String(content));
        System.out.println(aLong);
        mappedByteBuffer.get(content);
        aLong = mappedByteBuffer.getLong();
        System.out.println(new String(content));
        System.out.println(aLong);

    }

    @Test
    public void testLongLongHashMap(){
        LongLongHashMap longHashMap = new LongLongHashMap();
        longHashMap.put(1L,1L);
        longHashMap.put(2L,2L);
        System.out.println(longHashMap.get(1L));
        System.out.println(longHashMap.get(2L));
        System.out.println(longHashMap.get(3L));
    }

}
