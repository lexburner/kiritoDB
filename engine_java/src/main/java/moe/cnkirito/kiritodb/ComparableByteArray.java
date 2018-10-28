package moe.cnkirito.kiritodb;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class ComparableByteArray implements Comparator<byte[]> {
    private byte[] content;

    public ComparableByteArray(byte[] content) {
        this.content = content;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
        int offset1 = 0;
        int offset2 = 0;
        int length1 = o1.length;
        int length2 = o2.length;
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (o1[i] & 0xff);
            int b = (o2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ComparableByteArray))
        {
            return false;
        }
        return Arrays.equals(content, ((ComparableByteArray)other).content);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(content);
    }

}
