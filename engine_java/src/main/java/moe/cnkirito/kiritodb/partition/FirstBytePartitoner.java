package moe.cnkirito.kiritodb.partition;

import moe.cnkirito.kiritodb.common.Util;

/**
 * 根据 key 的第一个字节做分区，因为评测的 key 是从 0 ~ Long.MAX_VALUE 均匀分布的
 */
public class FirstBytePartitoner implements Partitionable {
    @Override
    public int getPartition(byte[] key) {
        return (int) ((Util.bytes2Long(key) >> (64 - 10)) + (1024/2));
//        return ((key[0] & 0xff) << 2) | ((key[1] & 0xff) >> 6);
//        return key[0] & 0xff;
//        int modulus = (int) ((keyL >> (64-log2(分组数)) + (分组数/2));
    }
}
