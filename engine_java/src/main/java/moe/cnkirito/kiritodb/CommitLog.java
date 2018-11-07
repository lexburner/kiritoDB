package moe.cnkirito.kiritodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    Logger logger = LoggerFactory.getLogger(CommitLog.class);

    final String path;
    private FileChannel fileChannel;
    private RandomAccessFile randomAccessFile;
    private AtomicLong wrotePosition;

    public CommitLog(String path) {
        this.path = path;
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            this.randomAccessFile = randomAccessFile;
            this.fileChannel = randomAccessFile.getChannel();
            this.wrotePosition = new AtomicLong(randomAccessFile.length());
            logger.info("current data file size: {}", randomAccessFile.length());
        } catch (IOException e) {
            logger.error("io exception", e);
        }
    }

    public void close() {
        if (this.fileChannel != null) {
            try {
                this.fileChannel.force(true);
            } catch (IOException e) {
                logger.error("force error", e);
            }
        }
        if (randomAccessFile != null) {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                logger.error("randomAccessFile close error", e);
            }
        }
        logger.info("commitLog closed.");
    }

    public long write(byte[] value) {
        long position = wrotePosition.getAndAdd(Constant.DATA_SIZE);
        try {
            ByteBuffer buffer = ByteBuffer.wrap(value);
            while (buffer.hasRemaining()) {
                this.fileChannel.write(buffer, position + (value.length - buffer.remaining()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return position;
    }

    public byte[] read(long position) {
        ByteBuffer readBuffer = ByteBuffer.allocate(Constant.DATA_SIZE);
        readBuffer.clear();
        try {
            fileChannel.read(readBuffer, position);
        } catch (IOException e) {
            logger.error("read error", e);
            return null;
        }
        readBuffer.flip();
        byte[] bytes = new byte[Constant.DATA_SIZE];
        readBuffer.get(bytes);
        return bytes;
    }

}
