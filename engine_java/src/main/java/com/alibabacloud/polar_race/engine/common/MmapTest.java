package com.alibabacloud.polar_race.engine.common;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-11-10
 */
public class MmapTest {

    @Test
    public void test1() throws IOException {
        File file = new File("/tmp/mmaptest");
        if(file.exists()){
            file.delete();
        }
        file.createNewFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");
        MappedByteBuffer mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 800 * 1024 * 1024);
        mappedByteBuffer.putInt(1);
        mappedByteBuffer.putInt(2);
        mappedByteBuffer.putInt(3);
        mappedByteBuffer.putInt(4);
    }

    @Test
    public void test2() throws IOException {
        File file = new File("/tmp/mmaptest");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 5);
        int size = channel.read(buffer,800 * 1024 * 1024 - 4);
        System.out.println(size);
        buffer.flip();
        System.out.println(buffer.getInt());
//        System.out.println(buffer.getInt());
//        System.out.println(buffer.getInt());
//        System.out.println(buffer.getInt());
//        System.out.println(buffer.getInt());
    }
}
