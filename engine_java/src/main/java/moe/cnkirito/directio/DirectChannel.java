package moe.cnkirito.directio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;

public interface DirectChannel extends Channel {
    /**
     * Writes from the <tt>src</tt> buffer into this channel at <tt>position</tt>. </p>
     *
     * @param src
     *        The {@link ByteBuffer} to write from
     *
     * @param position
     *        The position within the file at which to start writing
     *
     * @return How many bytes were written from <tt>src</tt> into the file
     * @throws IOException
     */
    int write(ByteBuffer src, long position) throws IOException;

    /**
     * Reads from this channel into the <tt>dst</tt> buffer from <tt>position</tt>. </p>
     *
     * @param dst
     *        The {@link ByteBuffer} to read into
     *
     * @param position
     *        The position within the file at which to start reading
     *
     * @return How many bytes were placed into <tt>dst</tt>
     * @throws IOException
     */
    int read(ByteBuffer dst, long position) throws IOException;

    /**
     * @return The file size for this channel
     */
    long size();

    /**
     * @return <tt>true</tt> if this channel is read only, <tt>false</tt> otherwise
     */
    boolean isReadOnly();

    /**
     * Truncates this file's length to <tt>fileLength</tt>. </p>
     *
     * @param fileLength The length to which to truncate
     *
     * @return This UnsafeByteAlignedChannel
     *
     * @throws IOException
     */
    DirectChannel truncate(long fileLength) throws IOException;

    /**
     * @return The file descriptor for this channel
     */
    int getFD();
}