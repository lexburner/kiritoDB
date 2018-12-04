//package moe.cnkirito.directio;
//
//import java.io.Closeable;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.ByteBuffer;
//
///**
// * Class to emulate the behavior of {@link RandomAccessFile}, but using direct I/O.
// *
// */
//public class DirectRandomAccessFile implements Closeable {
//
//    private DirectChannel channel;
//
//
//    /**
//     * @param file The file to open
//     *
//     * @param mode Either "rw" or "r", depending on whether this file is read only
//     *
//     * @throws IOException
//     */
//    public DirectRandomAccessFile(File file, String mode)
//        throws IOException {
//
//        boolean readOnly = false;
//        if (mode.equals("r")) {
//            readOnly = true;
//        } else if (!mode.equals("rw")) {
//            throw new IllegalArgumentException("only r and rw modes supported");
//        }
//
//        if (readOnly && !file.isFile()) {
//            throw new FileNotFoundException("couldn't find file " + file);
//        }
//
//        this.channel = DirectChannelImpl.getChannel(file, readOnly);
//    }
//
//    @Override
//    public void close() throws IOException {
//        channel.close();
//    }
//
//
//    public int write(ByteBuffer src, long position) throws IOException {
//        return channel.write(src, position);
//    }
//
//    public int read(ByteBuffer dst, long position) throws IOException {
//        return channel.read(dst, position);
//    }
//
//    /**
//     * @return The current position in the file
//     */
//    public long getFilePointer() {
//        return channel.getFD();
//    }
//
//    /**
//     * @return The current length of the file
//     */
//    public long length() {
//        return channel.size();
//    }
//
//}