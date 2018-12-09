package moe.cnkirito.kiritodb.partition;

/**
 * using high ten bit of the given key to determine which file it hits.
 */
public class HighTenPartitioner implements Partitionable {
    @Override
    public int getPartition(byte[] key) {
        return ((key[0] & 0xff) << 2) | ((key[1] & 0xff) >> 6);
    }
}
