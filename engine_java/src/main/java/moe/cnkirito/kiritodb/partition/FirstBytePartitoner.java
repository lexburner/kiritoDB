package moe.cnkirito.kiritodb.partition;

/**
 * 根据 key 的第一个字节做分区，因为评测的 key 是从 0 ~ Long.MAX_VALUE 均匀分布的
 */
public class FirstBytePartitoner implements Partitionable {
    @Override
    public int getPartition(byte[] key) {
        return key[0] & 0xff;
    }
}
