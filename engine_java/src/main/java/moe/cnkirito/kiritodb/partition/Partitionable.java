package moe.cnkirito.kiritodb.partition;

/**
 * determine how to hash the key
 */
public interface Partitionable {
    int getPartition(byte[] key);
}
