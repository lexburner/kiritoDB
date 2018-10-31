package moe.cnkirito.kiritodb;

import java.util.Arrays;

/**
 * @author kirito.moe@foxmail.com
 * Date 2018-10-28
 */
public class ComparableByteArray implements Comparable<ComparableByteArray> {
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
    public boolean equals(Object other) {
        if (!(other instanceof ComparableByteArray)) {
            return false;
        }
        return Arrays.equals(content, ((ComparableByteArray) other).content);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(content);
    }

    @Override
    public int compareTo(ComparableByteArray o) {
        int result = 0;
        if (content.length != o.content.length) {
            result = content.length - o.content.length;
        } else {
            for (int i = 0; i < content.length; i++) {
                if (content[i] != o.content[i]) {
                    result = (int) (content[i] - o.content[i]);
                    break;
                }
            }
        }
        return result;
    }
}
